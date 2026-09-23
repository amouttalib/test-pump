"""
analytics_pipeline.py

Pipeline d'analyse temps réel.

Flux principal Pump.fun :

logsSubscribe
    ↓
parse_trade_from_logs()
    ↓
Trade
    ↓
OrderFlowAnalyzer.record_trade()
    ↓
LaunchTracker.record_trade()
    ↓
Flow Gate / SnipingStrategy

IMPORTANT :
Le chemin critique du sniper ne doit contenir aucun appel réseau
supplémentaire après réception d'un trade Pump.fun.

En particulier :
    - pas de fetch_sol_usd()
    - pas de DexScreener
    - pas de Helius
    - pas d'appel RPC supplémentaire

Les enrichissements USD / holders / lifecycle / sentiment sont
effectués séparément et ne doivent jamais bloquer l'arrivée
d'un BUY/SELL dans OrderFlow.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import aiohttp

from analysis.order_flow import (
    OrderFlowAnalyzer,
    Trade,
)
from analysis.lifecycle import LifecycleAnalyzer
from analysis.sentiment import SentimentAnalyzer
from analysis.social_graph import SocialGraphAnalyzer

from utils.bonding_curve import BondingCurveAnalyzer

from utils.config_loader import Config
from utils.data_providers import get_providers
from utils.logger import setup_logger

from utils.pumpfun_parser import PUMP_FUN_PROGRAM
from utils.pumpfun_trade_parser import parse_trade_from_logs

from utils.launch_tracker import LaunchTracker


log = setup_logger("analytics_pipeline")


class AnalyticsPipeline:

    def __init__(
        self,
        order_flow: OrderFlowAnalyzer,
        lifecycle: LifecycleAnalyzer,
        social_graph: SocialGraphAnalyzer,
        sentiment: SentimentAnalyzer,
        bonding: Optional[BondingCurveAnalyzer] = None,
        risk_positions_ref=None,
        launch_tracker: Optional[LaunchTracker] = None,
    ) -> None:

        self.order_flow = order_flow

        self.lifecycle = lifecycle

        self.social_graph = social_graph

        self.sentiment = sentiment

        # IMPORTANT :
        # Le même LaunchTracker doit être partagé avec
        # SnipingStrategy.
        self.launch_tracker = launch_tracker

        self.bonding = (
            bonding
            or BondingCurveAnalyzer()
        )

        self._get_positions = (
            risk_positions_ref
            or (lambda: {})
        )

        self.cfg = Config.get()

        self._tasks: list[asyncio.Task] = []

        self._stop_event = asyncio.Event()

        self._tracked_mints: dict[str, float] = {}

        self._MAX_TRACKED = 500

        # ------------------------------------------------------------
        # LaunchTracker partagé avec le sniper
        # ------------------------------------------------------------
        #
        # Si aucun tracker n'est fourni par Orchestrator, on crée
        # un fallback local.
        #
        # Dans le fonctionnement normal, Orchestrator fournit le
        # tracker partagé.
        #
        self.launch_tracker = (
            launch_tracker
            or LaunchTracker(
                min_trades=5,
                timeout_sec=3.0,
            )
        )

    # ================================================================
    # LIFECYCLE
    # ================================================================

    async def start(self) -> None:

        self._tasks.append(
            asyncio.create_task(
                self._pumpfun_trade_stream()
            )
        )

        self._tasks.append(
            asyncio.create_task(
                self._lifecycle_observer_loop()
            )
        )

        self._tasks.append(
            asyncio.create_task(
                self._sentiment_poller_loop()
            )
        )

        log.info(
            "analytics_pipeline.started",
            tracked_capacity=self._MAX_TRACKED,
            trade_stream="pumpfun_logsSubscribe",
            launch_tracker=True,
        )

    async def stop(self) -> None:

        self._stop_event.set()

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:

            try:
                await task

            except asyncio.CancelledError:
                pass

        self._tasks.clear()

    # ================================================================
    # TRACKING MINTS
    # ================================================================

    def _track_mint(
        self,
        mint: str,
    ) -> None:

        if not mint:
            return

        if mint not in self._tracked_mints:

            self._tracked_mints[mint] = time.time()

            if (
                len(self._tracked_mints)
                > self._MAX_TRACKED
            ):

                oldest = min(
                    self._tracked_mints,
                    key=self._tracked_mints.get,
                )

                self._tracked_mints.pop(
                    oldest,
                    None,
                )

    # ================================================================
    # PUMP.FUN REAL-TIME TRADE STREAM
    # ================================================================

    async def _pumpfun_trade_stream(
        self,
    ) -> None:

        import websockets

        ws_endpoint = (
            self.cfg["chains"]["solana"]["ws_endpoint"]
        )

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                {
                    "mentions": [
                        PUMP_FUN_PROGRAM
                    ]
                },
                {
                    "commitment": "confirmed"
                },
            ],
        }

        backoff = 1.0

        while not self._stop_event.is_set():

            try:

                async with websockets.connect(
                    ws_endpoint,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:

                    await ws.send(
                        json.dumps(payload)
                    )

                    log.info(
                        "analytics_pipeline.trade_ws_connected"
                    )

                    backoff = 1.0

                    async for raw in ws:

                        if self._stop_event.is_set():
                            break

                        try:

                            msg = json.loads(raw)

                            if (
                                msg.get("method")
                                != "logsNotification"
                            ):
                                continue

                            await self._process_log_event(
                                msg
                            )

                        except Exception as e:

                            log.warning(
                                "analytics_pipeline.log_process_error",
                                error=str(e),
                            )

            except asyncio.CancelledError:

                raise

            except Exception as e:

                log.warning(
                    "analytics_pipeline.trade_ws_disconnected",
                    error=str(e),
                    backoff=backoff,
                )

                try:

                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=backoff,
                    )

                except asyncio.TimeoutError:
                    pass

                backoff = min(
                    backoff * 2,
                    30.0,
                )

    # ================================================================
    # PROCESS TRADE
    # ================================================================

    async def _process_log_event(
        self,
        msg: dict,
    ) -> None:

        event_received_at = time.monotonic()

        # ------------------------------------------------------------
        # 1. EXTRACTION LOGS
        # ------------------------------------------------------------

        result = (
            msg.get("params", {})
            .get("result", {})
        )

        value = (
            result.get("value", {})
        )

        logs = value.get("logs") or []

        if not logs:
            return

        # Transaction échouée -> on ignore.
        if value.get("err") is not None:
            return

        # ------------------------------------------------------------
        # SIGNATURE SOLANA
        # ------------------------------------------------------------
        #
        # La signature est extrêmement importante pour le diagnostic
        # CREATE <-> TRADE.
        #
        # Elle sera présente directement dans logsNotification.result.value
        # sur Solana.
        # ------------------------------------------------------------

        signature = (
            value.get("signature")
            or result.get("signature")
            or ""
        )

        # ------------------------------------------------------------
        # 2. PARSING PUMP.FUN TRADE
        # ------------------------------------------------------------

        parsed = parse_trade_from_logs(
            logs
        )

        if parsed is None:

            # --------------------------------------------------------
            # Aucun TradeEvent reconnu.
            #
            # On ne loggue pas chaque transaction non-trade en INFO,
            # car logsSubscribe reçoit énormément d'événements.
            # --------------------------------------------------------

            return

        mint = parsed.mint

        if not mint:
            return

        self._track_mint(
            mint
        )

        # ------------------------------------------------------------
        # 3. CALCUL LOCAL DU TRADE
        # ------------------------------------------------------------

        sol_amount = (
            parsed.sol_amount_lamports
            / 1_000_000_000
        )

        token_amount = (
            parsed.token_amount_raw
            / 1_000_000
        )

        price = (
            sol_amount / token_amount
            if token_amount > 0
            else 0.0
        )

        side = (
            "buy"
            if parsed.is_buy
            else "sell"
        )

        trade_timestamp = (
            parsed.timestamp
            if parsed.timestamp
            else time.time()
        )

        # ------------------------------------------------------------
        # DIAGNOSTIC CREATE <-> TRADE
        # ------------------------------------------------------------

        log.info(
            "analytics_pipeline.trade_mint_observed",
            mint=mint,
            side=side,
            trader=parsed.user,
            sol=round(
                sol_amount,
                6,
            ),
            signature=signature,
        )

        # ------------------------------------------------------------
        # 4. CONSTRUCTION TRADE
        # ------------------------------------------------------------
        #
        # Aucun appel réseau.
        #
        # size_usd reste volontairement à 0.0.
        # ------------------------------------------------------------

        trade = Trade(
            ts=trade_timestamp,
            side=side,
            size_sol=sol_amount,
            size_usd=0.0,
            size_token=token_amount,
            trader=parsed.user,
            price=price,
            is_whale=(
                sol_amount
                >= self.order_flow.WHALE_THRESHOLD_SOL
            ),
        )

        # ------------------------------------------------------------
        # 5. ORDER FLOW — IMMÉDIAT
        # ------------------------------------------------------------

        order_flow_started = time.monotonic()

        try:

            self.order_flow.record_trade(
                mint,
                trade,
            )

        except Exception as e:

            log.warning(
                "analytics_pipeline.order_flow_record_failed",
                mint=mint,
                signature=signature,
                error=str(e),
            )

            return

        order_flow_ms = (
            time.monotonic()
            - order_flow_started
        ) * 1000.0

        # ------------------------------------------------------------
        # 6. LAUNCH TRACKER — IMMÉDIAT
        # ------------------------------------------------------------

        tracker_started = time.monotonic()

        tracker_count = None

        if self.launch_tracker is not None:

            try:

                tracker_count = (
                    self.launch_tracker.record_trade(
                        mint
                    )
                )

            except Exception as e:

                log.warning(
                    "analytics_pipeline.launch_tracker_record_failed",
                    mint=mint,
                    signature=signature,
                    error=str(e),
                )

        tracker_ms = (
            time.monotonic()
            - tracker_started
        ) * 1000.0

        # ------------------------------------------------------------
        # 7. LATENCE
        # ------------------------------------------------------------

        processing_ms = (
            time.monotonic()
            - event_received_at
        ) * 1000.0

        # ------------------------------------------------------------
        # 8. LOG FINAL
        # ------------------------------------------------------------

        log.info(
            "analytics_pipeline.trade_detected",
            mint=mint,
            side=trade.side,
            trader=parsed.user,
            sol=round(
                sol_amount,
                6,
            ),
            tokens=round(
                token_amount,
                2,
            ),
            signature=signature,
            order_flow_ms=round(
                order_flow_ms,
                3,
            ),
            launch_tracker_ms=round(
                tracker_ms,
                3,
            ),
            tracker_count=tracker_count,
            processing_ms=round(
                processing_ms,
                3,
            ),
        )

        # ------------------------------------------------------------
        # IMPORTANT :
        #
        # Aucun await réseau ici.
        #
        # Le Flow Gate peut maintenant immédiatement voir le trade.
        #
        # Les enrichissements USD / holders / DexScreener /
        # lifecycle / sentiment restent dans les boucles secondaires.
        # ------------------------------------------------------------

    # ================================================================
    # LIFECYCLE OBSERVER
    # ================================================================

    async def _lifecycle_observer_loop(
        self,
    ) -> None:

        interval = self.cfg.get_nested(
            "analysis",
            "lifecycle_observation_interval_sec",
            default=30,
        )

        while not self._stop_event.is_set():

            try:

                mints = set(
                    self._tracked_mints.keys()
                )

                try:

                    positions = (
                        self._get_positions()
                    )

                    for pos in positions.values():

                        if (
                            getattr(
                                pos,
                                "chain",
                                None,
                            )
                            == "solana"
                        ):

                            mints.add(
                                pos.token
                            )

                except Exception:
                    pass

                for mint in mints:

                    await self._observe_one(
                        mint
                    )

            except Exception as e:

                log.error(
                    "analytics_pipeline.lifecycle_loop_error",
                    error=str(e),
                )

            try:

                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=interval,
                )

            except asyncio.TimeoutError:
                pass

    # ================================================================
    # OBSERVE ONE TOKEN
    # ================================================================

    async def _observe_one(
        self,
        mint: str,
    ) -> None:

        try:

            providers = get_providers()

            helius = providers["helius"]

            dex = providers["dexscreener"]

            # --------------------------------------------------------
            # HOLDERS
            # --------------------------------------------------------

            holders = await helius.get_token_holders(
                mint
            )

            holder_count = (
                len(holders)
                if isinstance(
                    holders,
                    list,
                )
                else 0
            )

            # --------------------------------------------------------
            # PRICE
            # --------------------------------------------------------

            price = await dex.get_price_usd(
                mint
            )

            # --------------------------------------------------------
            # ORDER FLOW
            # --------------------------------------------------------

            snap = self.order_flow.snapshot(
                mint,
                window_sec=300,
            )

            trade_count = (
                snap.total_trades
                if snap
                else 0
            )

            # --------------------------------------------------------
            # LIFECYCLE
            # --------------------------------------------------------

            self.lifecycle.record_observation(
                mint,
                holder_count,
                price,
                trade_count,
            )

        except Exception as e:

            log.debug(
                "analytics_pipeline.observe_one_failed",
                mint=mint,
                error=str(e),
            )

    # ================================================================
    # SENTIMENT
    # ================================================================

    async def _sentiment_poller_loop(
        self,
    ) -> None:

        interval = self.cfg.get_nested(
            "analysis",
            "sentiment_poll_interval_sec",
            default=120,
        )

        telegram_chat_ids = (
            self.cfg.get_nested(
                "analysis",
                "sentiment",
                default={},
            ).get(
                "telegram_chat_ids",
                [],
            )
        )

        while not self._stop_event.is_set():

            try:

                mints = set(
                    self._tracked_mints.keys()
                )

                try:

                    positions = (
                        self._get_positions()
                    )

                    for pos in positions.values():

                        if (
                            getattr(
                                pos,
                                "chain",
                                None,
                            )
                            == "solana"
                        ):

                            mints.add(
                                pos.token
                            )

                except Exception:
                    pass

                for mint in mints:

                    symbol = await self._symbol_for(
                        mint
                    )

                    if not symbol:
                        continue

                    # ------------------------------------------------
                    # TWITTER
                    # ------------------------------------------------

                    try:

                        await self.sentiment.fetch_twitter_mentions(
                            mint,
                            symbol,
                        )

                    except Exception as e:

                        log.debug(
                            "analytics_pipeline.twitter_poll_failed",
                            mint=mint,
                            error=str(e),
                        )

                    # ------------------------------------------------
                    # TELEGRAM
                    # ------------------------------------------------

                    for chat_id in telegram_chat_ids:

                        try:

                            await self.sentiment.fetch_telegram_mentions(
                                mint,
                                symbol,
                                chat_id,
                            )

                        except Exception as e:

                            log.debug(
                                "analytics_pipeline.telegram_poll_failed",
                                mint=mint,
                                chat=chat_id,
                                error=str(e),
                            )

            except Exception as e:

                log.error(
                    "analytics_pipeline.sentiment_loop_error",
                    error=str(e),
                )

            try:

                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=interval,
                )

            except asyncio.TimeoutError:
                pass

    # ================================================================
    # SYMBOL
    # ================================================================

    async def _symbol_for(
        self,
        mint: str,
    ) -> Optional[str]:

        try:

            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=5
                )
            ) as session:

                async with session.get(
                    "https://api.dexscreener.com/latest/dex/tokens/"
                    + mint
                ) as response:

                    if response.status != 200:
                        return None

                    data = await response.json()

            pairs = data.get(
                "pairs"
            ) or []

            if pairs:

                return (
                    pairs[0]
                    .get(
                        "baseToken",
                        {},
                    )
                    .get(
                        "symbol"
                    )
                )

        except Exception:

            return None

        return None