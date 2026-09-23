"""
sniping.py

Sniper Pump.fun avec confirmation par Order Flow.

Flux :

CREATE
    ↓
Fast Risk
    ↓
LaunchTracker.register()
    ↓
OrderFlow snapshot
    ↓
Observation BUY/SELL
    ↓
Flow Gate
    ↓
Delayed Risk
    ↓
BUY
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

from chains.base_chain import BaseChainAdapter

from strategies.base_strategy import (
    BaseStrategy,
    Signal,
    SignalType,
)

from utils.config_loader import Config
from utils.latency_tracer import get_tracer
from utils.logger import setup_logger

from utils.pumpfun_parser import (
    PUMP_FUN_PROGRAM,
    fetch_create_event_from_signature,
)

from analysis.order_flow import OrderFlowAnalyzer


log = setup_logger(
    "sniping"
)


class SnipingStrategy(BaseStrategy):

    name = "sniping"

    def __init__(
        self,
        adapter: BaseChainAdapter,
        signal_queue: asyncio.Queue,
        anti_rug=None,
        order_flow=None,
        launch_tracker=None,
    ) -> None:

        super().__init__(
            adapter
        )

        self.queue = signal_queue

        self.anti_rug = anti_rug

        self.launch_tracker = (
            launch_tracker
        )

        self.cfg = (
            Config.get()
            .get_nested(
                "strategies",
                "sniping",
                default={},
            )
        )

        self.scan_interval = (
            self.cfg.get(
                "new_pool_scan_interval_sec",
                2,
            )
        )

        self.max_delay_ms = (
            self.cfg.get(
                "max_buy_delay_ms",
                1500,
            )
        )

        # ========================================================
        # FLOW CONFIRMATION
        # ========================================================

        self.confirmation_enabled = (
            self.cfg.get(
                "flow_confirmation_enabled",
                True,
            )
        )

        self.observation_ms = (
            self.cfg.get(
                "flow_observation_ms",
                3000,
            )
        )

        self.min_trades = (
            self.cfg.get(
                "flow_min_trades",
                5,
            )
        )

        self.min_buy_trades = (
            self.cfg.get(
                "flow_min_buy_trades",
                3,
            )
        )

        self.min_unique_buyers = (
            self.cfg.get(
                "flow_min_unique_buyers",
                3,
            )
        )

        self.min_volume_sol = (
            self.cfg.get(
                "flow_min_volume_sol",
                0.5,
            )
        )

        self.min_pressure_score = (
            self.cfg.get(
                "flow_min_pressure_score",
                20.0,
            )
        )

        self.min_net_flow_sol = (
            self.cfg.get(
                "flow_min_net_flow_sol",
                0.0,
            )
        )

        self.order_flow = (
            order_flow
            if order_flow is not None
            else OrderFlowAnalyzer()
        )

        # ========================================================
        # LAUNCH TIMING
        # ========================================================

        self._tracked_launches: dict[
            str,
            float,
        ] = {}

        # ========================================================
        # CONCURRENCY / DUPLICATE PROTECTION
        # ========================================================

        self._processing_mints: set[
            str
        ] = set()

        self._launch_tasks: set[
            asyncio.Task
        ] = set()

        self._decision_made_mints: set[
            str
        ] = set()

    # ============================================================
    # RUN
    # ============================================================

    async def run(
        self,
    ) -> None:

        log.info(
            "sniping.started",
            chain=self.adapter.chain_name,
            program=PUMP_FUN_PROGRAM,
            flow_confirmation=(
                self.confirmation_enabled
            ),
            observation_ms=(
                self.observation_ms
            ),
            min_trades=self.min_trades,
        )

        tracer = get_tracer()

        try:

            async for event in (
                self.adapter.watch_new_tokens()
            ):

                mint_hint = event.get(
                    "mint",
                    "",
                )

                detected_at = time.perf_counter()

                trace_id = tracer.start(
                    "snipe",
                    token=mint_hint,
                )

                tracer.mark_once(
                    trace_id,
                    "detect",
                    at=detected_at,
                )

                log.info(
                    "sniping.create_received",
                    mint=mint_hint,
                    signature=event.get(
                        "signature"
                    ),
                    trace_id=trace_id,
                )

                task = asyncio.create_task(
                    self._evaluate_launch_task(
                        event,
                        trace_id,
                        detected_at,
                    )
                )

                self._launch_tasks.add(
                    task
                )

                task.add_done_callback(
                    self._launch_tasks.discard
                )

        except asyncio.CancelledError:

            log.info(
                "sniping.cancelled"
            )

            tasks = list(
                self._launch_tasks
            )

            for task in tasks:
                task.cancel()

            if tasks:

                await asyncio.gather(
                    *tasks,
                    return_exceptions=True,
                )

            raise

    # ============================================================
    # CONCURRENT LAUNCH TASK
    # ============================================================

    async def _evaluate_launch_task(
        self,
        event: dict,
        trace_id: int | None,
        detected_at: float,
    ) -> None:

        mint_hint = event.get(
            "mint",
            "",
        )

        claimed_mints: set[
            str
        ] = set()

        if mint_hint:

            if (
                mint_hint
                in self._decision_made_mints
            ):

                log.debug(
                    "sniping.duplicate_launch_ignored",
                    mint=mint_hint,
                    reason="decision_already_made",
                )

                if trace_id is not None:
                    get_tracer().finish(
                        trace_id
                    )

                return

            if (
                mint_hint
                in self._processing_mints
            ):

                log.debug(
                    "sniping.duplicate_launch_ignored",
                    mint=mint_hint,
                    reason="already_processing",
                )

                if trace_id is not None:
                    get_tracer().finish(
                        trace_id
                    )

                return

            # ----------------------------------------------------
            # Claim AVANT le premier await.
            # ----------------------------------------------------

            self._processing_mints.add(
                mint_hint
            )

            claimed_mints.add(
                mint_hint
            )

        try:

            signal = await self.evaluate(
                event,
                trace_id=trace_id,
                claimed_mints=claimed_mints,
                detected_at=detected_at,
            )

            if (
                signal
                and signal.signal_type
                == SignalType.BUY
            ):

                await self.queue.put(
                    signal
                )

                if trace_id is not None:

                    get_tracer().mark_once(
                        trace_id,
                        "signal_emitted",
                    )

                log.info(
                    "sniping.signal_emitted",
                    mint=(
                        signal.token_address
                    ),
                    trace_id=trace_id,
                )

            if trace_id is not None:

                get_tracer().finish(
                    trace_id
                )

        except asyncio.CancelledError:

            raise

        except Exception as e:

            log.exception(
                "sniping.launch_task_failed",
                mint=mint_hint,
                error=str(e),
            )

            if trace_id is not None:

                get_tracer().finish(
                    trace_id
                )

        finally:

            for mint in claimed_mints:

                self._processing_mints.discard(
                    mint
                )

    # ============================================================
    # EVALUATE
    # ============================================================

    async def evaluate(
        self,
        event: dict,
        trace_id: int | None = None,
        claimed_mints: Optional[
            set[str]
        ] = None,
        detected_at: float | None = None,
    ) -> Signal | None:

        if claimed_mints is None:

            claimed_mints = set()

        chain = event.get(
            "chain",
            self.adapter.chain_name,
        )

        signature = event.get(
            "signature"
        )

        if not signature:
            return None

        mint = event.get(
            "mint"
        )

        bonding_curve = event.get(
            "bonding_curve"
        )

        dev_wallet = event.get(
            "user"
        )

        symbol = ""

        name = ""

        # ========================================================
        # 1. PARSE CREATE
        # ========================================================

        if not mint:

            try:

                create_event = (
                    await fetch_create_event_from_signature(
                        self.adapter,
                        signature,
                    )
                )

            except Exception as e:

                log.warning(
                    "sniping.fetch_create_failed",
                    sig=signature,
                    error=str(e),
                )

                return None

            if not create_event:
                return None

            if isinstance(
                create_event,
                (list, tuple),
            ):

                if not create_event:
                    return None

                create_event = (
                    create_event[0]
                )

            mint = getattr(
                create_event,
                "mint",
                None,
            )

            bonding_curve = (
                bonding_curve
                or getattr(
                    create_event,
                    "bonding_curve",
                    None,
                )
            )

            dev_wallet = (
                dev_wallet
                or getattr(
                    create_event,
                    "user",
                    None,
                )
            )

            symbol = (
                getattr(
                    create_event,
                    "symbol",
                    None,
                )
                or ""
            )

            name = (
                getattr(
                    create_event,
                    "name",
                    None,
                )
                or ""
            )

        if not mint:
            return None

        # ========================================================
        # CLAIM MINT APRÈS PARSING
        # ========================================================

        if (
            mint
            in self._decision_made_mints
        ):

            log.debug(
                "sniping.duplicate_launch_ignored",
                mint=mint,
                reason="decision_already_made",
            )

            return None

        if (
            mint
            not in self._processing_mints
        ):

            self._processing_mints.add(
                mint
            )

            claimed_mints.add(
                mint
            )

        elif (
            mint
            not in claimed_mints
        ):

            log.debug(
                "sniping.duplicate_launch_ignored",
                mint=mint,
                reason="already_processing_after_parse",
            )

            return None

        # ========================================================
        # TRACE PARSED
        # ========================================================

        if trace_id is not None:

            get_tracer().mark_once(
                trace_id,
                "parsed",
            )

        parse_elapsed_ms = 0.0

        if detected_at is not None:

            parse_elapsed_ms = (
                time.perf_counter()
                - detected_at
            ) * 1000.0

        log.info(
            "sniping.create_parsed",
            mint=mint,
            signature=signature,
            parse_elapsed_ms=round(
                parse_elapsed_ms,
                3,
            ),
        )

        # ========================================================
        # CREATE BASELINE
        # ========================================================

        launch_started_at = (
            time.monotonic()
        )

        self._tracked_launches[
            mint
        ] = launch_started_at

        log.info(
            "sniping.new_launch_detected",
            mint=mint,
            symbol=symbol or "?",
            name=(name or "?")[:40],
            signature=signature,
            source=event.get(
                "source",
                "fetch",
            ),
            trace_id=trace_id,
        )

        # ========================================================
        # 2. FAST RISK
        # ========================================================

        if self.anti_rug is not None:

            try:

                fast_verdict = (
                    await self.anti_rug.check_fast(
                        mint
                    )
                )

                if not fast_verdict.is_safe:

                    log.info(
                        "sniping.fast_risk_blocked",
                        mint=mint,
                        reasons=(
                            fast_verdict.reasons
                        ),
                    )

                    self._cleanup_launch(
                        mint
                    )

                    return None

            except Exception as e:

                log.warning(
                    "sniping.fast_risk_failed",
                    mint=mint,
                    error=str(e),
                )

                self._cleanup_launch(
                    mint
                )

                return None

        # ========================================================
        # 3. REGISTER LAUNCH
        # ========================================================

        if (
            self.confirmation_enabled
            and self.launch_tracker is not None
        ):

            existing_flow = (
                self.order_flow.snapshot(
                    mint,
                    window_sec=60,
                )
            )

            existing_trade_count = (
                existing_flow.total_trades
                if existing_flow is not None
                else 0
            )

            state = (
                self.launch_tracker.register(
                    mint=mint,
                    event=event,
                    existing_trades=(
                        existing_trade_count
                    ),
                    created_at=(
                        launch_started_at
                    ),
                )
            )

            if trace_id is not None:

                get_tracer().mark_once(
                    trace_id,
                    "registered",
                )

            log.info(
                "sniping.launch_registered",
                mint=mint,
                signature=signature,
                existing_trades=(
                    existing_trade_count
                ),
                tracker_trades=(
                    state.trade_count
                ),
                pending_ready=(
                    state.ready.is_set()
                ),
                elapsed_ms=round(
                    (
                        time.monotonic()
                        - launch_started_at
                    )
                    * 1000.0,
                    3,
                ),
                trace_id=trace_id,
            )

        # ========================================================
        # 4. ORDER FLOW
        # ========================================================

        if self.confirmation_enabled:

            if trace_id is not None:

                get_tracer().mark_once(
                    trace_id,
                    "flow_watch_started",
                )

            log.info(
                "sniping.flow_watch_started",
                mint=mint,
                signature=signature,
                observation_ms=(
                    self.observation_ms
                ),
                min_trades=(
                    self.min_trades
                ),
                min_buy_trades=(
                    self.min_buy_trades
                ),
                min_unique_buyers=(
                    self.min_unique_buyers
                ),
                min_volume_sol=(
                    self.min_volume_sol
                ),
                min_net_flow_sol=(
                    self.min_net_flow_sol
                ),
                min_pressure_score=(
                    self.min_pressure_score
                ),
                elapsed_ms=round(
                    (
                        time.monotonic()
                        - launch_started_at
                    )
                    * 1000.0,
                    3,
                ),
                trace_id=trace_id,
            )

            flow = await self._wait_for_flow(
                mint,
                launch_started_at,
                trace_id=trace_id,
            )

            # ----------------------------------------------------
            # IMPORTANT :
            # None signifie qu'aucun snapshot exploitable
            # n'est disponible.
            # ----------------------------------------------------

            if flow is None:

                elapsed_ms = (
                    time.monotonic()
                    - launch_started_at
                ) * 1000.0

                log.info(
                    "sniping.flow_timeout",
                    mint=mint,
                    signature=signature,
                    elapsed_ms=round(
                        elapsed_ms,
                        2,
                    ),
                    reason="no_order_flow_snapshot",
                    trace_id=trace_id,
                )

                self._cleanup_launch(
                    mint
                )

                return None

            # ----------------------------------------------------
            # FLOW SNAPSHOT
            # ----------------------------------------------------

            elapsed_ms = (
                time.monotonic()
                - launch_started_at
            ) * 1000.0

            log.info(
                "sniping.flow_snapshot",
                mint=mint,
                signature=signature,
                trades=flow.total_trades,
                buys=flow.buy_count,
                sells=flow.sell_count,
                unique_buyers=(
                    flow.unique_buyers
                ),
                unique_sellers=(
                    flow.unique_sellers
                ),
                volume_sol=round(
                    flow.volume_sol,
                    4,
                ),
                buy_volume_sol=round(
                    flow.buy_volume_sol,
                    4,
                ),
                sell_volume_sol=round(
                    flow.sell_volume_sol,
                    4,
                ),
                net_flow_sol=round(
                    flow.net_sol_flow,
                    4,
                ),
                pressure=round(
                    flow.pressure_score,
                    2,
                ),
                elapsed_ms=round(
                    elapsed_ms,
                    2,
                ),
                trace_id=trace_id,
            )

            # ----------------------------------------------------
            # FLOW GATE
            # ----------------------------------------------------

            passed = self._log_flow_gate(
                mint,
                flow,
                launch_started_at,
            )

            if trace_id is not None:

                get_tracer().mark_once(
                    trace_id,
                    "flow_gate",
                )

            if not passed:

                log.info(
                    "sniping.flow_rejected",
                    mint=mint,
                    signature=signature,
                    trades=flow.total_trades,
                    buys=flow.buy_count,
                    sells=flow.sell_count,
                    unique_buyers=(
                        flow.unique_buyers
                    ),
                    volume_sol=(
                        flow.volume_sol
                    ),
                    net_flow=(
                        flow.net_sol_flow
                    ),
                    pressure=(
                        flow.pressure_score
                    ),
                    trace_id=trace_id,
                )

                self._cleanup_launch(
                    mint
                )

                return None

            log.info(
                "sniping.flow_gate_passed",
                mint=mint,
                signature=signature,
                pressure=round(
                    flow.pressure_score,
                    2,
                ),
                elapsed_ms=round(
                    elapsed_ms,
                    2,
                ),
                trace_id=trace_id,
            )

        # ========================================================
        # 5. DELAYED RISK
        # ========================================================

        if self.anti_rug is not None:

            try:

                verdict = await self.anti_rug.check(
                    mint,
                    skip_honeypot=True,
                )

                if not verdict.is_safe:

                    log.info(
                        "sniping.delayed_risk_blocked",
                        mint=mint,
                        reasons=(
                            verdict.reasons
                        ),
                    )

                    self._cleanup_launch(
                        mint
                    )

                    return None

            except Exception as e:

                log.warning(
                    "sniping.delayed_risk_failed",
                    mint=mint,
                    error=str(e),
                )

                self._cleanup_launch(
                    mint
                )

                return None

        if trace_id is not None:

            get_tracer().mark_once(
                trace_id,
                "scored",
            )

        log.info(
            "sniping.delayed_risk_passed",
            mint=mint,
            trace_id=trace_id,
        )

        # ========================================================
        # 6. BUY
        # ========================================================

        if (
            mint
            in self._decision_made_mints
        ):

            log.warning(
                "sniping.duplicate_buy_blocked",
                mint=mint,
            )

            self._cleanup_launch(
                mint
            )

            return None

        if self.launch_tracker is not None:

            if not self.launch_tracker.mark_decision(
                mint
            ):

                log.warning(
                    "sniping.duplicate_buy_blocked",
                    mint=mint,
                    reason=(
                        "launch_tracker_decision_already_made"
                    ),
                )

                self._cleanup_launch(
                    mint
                )

                return None

        self._decision_made_mints.add(
            mint
        )

        total_elapsed_ms = (
            time.monotonic()
            - launch_started_at
        ) * 1000.0

        log.info(
            "sniping.buy_signal",
            mint=mint,
            signature=signature,
            reason=(
                "initial order flow confirmed"
            ),
            elapsed_ms=round(
                total_elapsed_ms,
                2,
            ),
            trace_id=trace_id,
        )

        if trace_id is not None:

            get_tracer().mark_once(
                trace_id,
                "buy_signal",
            )

        self._cleanup_launch(
            mint,
            remove_tracker=True,
        )

        return Signal(
            strategy=self.name,
            chain=chain,
            token_address=mint,
            signal_type=SignalType.BUY,
            suggested_size_pct=0.02,
            confidence=0.65,
            reason=(
                "Pump.fun launch confirmed by "
                "initial order flow: "
                f"{symbol or '?'}"
            ),
            stop_loss_pct=15.0,
            take_profit_pct=80.0,
            metadata={
                "dev_wallet": dev_wallet,
                "bonding_curve": bonding_curve,
                "token_symbol": symbol,
                "token_name": name,
                "trace_id": trace_id,
                "create_signature": signature,
            },
        )

    # ============================================================
    # FLOW GATE DIAGNOSTIC
    # ============================================================

    def _log_flow_gate(
        self,
        mint: str,
        flow,
        launch_started_at: float,
    ) -> bool:

        if flow is None:

            log.info(
                "sniping.flow_gate_evaluated",
                mint=mint,
                passed=False,
                reason="no_snapshot",
            )

            return False

        checks = {
            "min_trades": (
                flow.total_trades
                >= self.min_trades
            ),
            "min_buy_trades": (
                flow.buy_count
                >= self.min_buy_trades
            ),
            "min_unique_buyers": (
                flow.unique_buyers
                >= self.min_unique_buyers
            ),
            "min_volume_sol": (
                flow.volume_sol
                >= self.min_volume_sol
            ),
            "min_net_flow_sol": (
                flow.net_sol_flow
                >= self.min_net_flow_sol
            ),
            "min_pressure_score": (
                flow.pressure_score
                >= self.min_pressure_score
            ),
        }

        passed = all(
            checks.values()
        )

        elapsed_ms = (
            time.monotonic()
            - launch_started_at
        ) * 1000.0

        log.info(
            "sniping.flow_gate_evaluated",
            mint=mint,
            passed=passed,
            trades=flow.total_trades,
            buys=flow.buy_count,
            sells=flow.sell_count,
            unique_buyers=(
                flow.unique_buyers
            ),
            unique_sellers=(
                flow.unique_sellers
            ),
            volume_sol=round(
                flow.volume_sol,
                6,
            ),
            buy_volume_sol=round(
                flow.buy_volume_sol,
                6,
            ),
            sell_volume_sol=round(
                flow.sell_volume_sol,
                6,
            ),
            net_flow_sol=round(
                flow.net_sol_flow,
                6,
            ),
            pressure=round(
                flow.pressure_score,
                2,
            ),
            checks=checks,
            elapsed_ms=round(
                elapsed_ms,
                3,
            ),
        )

        return passed

    # ============================================================
    # FLOW GATE
    # ============================================================

    def _flow_is_good(
        self,
        flow,
    ) -> bool:

        if flow is None:
            return False

        if (
            flow.total_trades
            < self.min_trades
        ):
            return False

        if (
            flow.buy_count
            < self.min_buy_trades
        ):
            return False

        if (
            flow.unique_buyers
            < self.min_unique_buyers
        ):
            return False

        if (
            flow.volume_sol
            < self.min_volume_sol
        ):
            return False

        if (
            flow.net_sol_flow
            < self.min_net_flow_sol
        ):
            return False

        if (
            flow.pressure_score
            < self.min_pressure_score
        ):
            return False

        return True

    # ============================================================
    # WAIT FOR FLOW
    # ============================================================

    async def _wait_for_flow(
        self,
        mint: str,
        launch_started_at: float,
        trace_id: int | None = None,
    ):

        timeout_sec = (
            self.observation_ms
            / 1000.0
        )

        deadline = (
            launch_started_at
            + timeout_sec
        )

        # ========================================================
        # SNAPSHOT INITIAL
        # ========================================================

        snapshot = (
            self.order_flow.snapshot(
                mint,
                window_sec=60,
            )
        )

        if snapshot is not None:

            if (
                snapshot.total_trades
                > 0
            ):

                if trace_id is not None:

                    get_tracer().mark_once(
                        trace_id,
                        "flow_first_trade",
                    )

                if (
                    snapshot.total_trades
                    >= self.min_trades
                ):

                    if trace_id is not None:

                        get_tracer().mark_once(
                            trace_id,
                            "flow_5_trades",
                        )

            log.info(
                "sniping.flow_initial_snapshot",
                mint=mint,
                trades=snapshot.total_trades,
                buys=snapshot.buy_count,
                sells=snapshot.sell_count,
                unique_buyers=(
                    snapshot.unique_buyers
                ),
                volume_sol=round(
                    snapshot.volume_sol,
                    4,
                ),
                pressure=round(
                    snapshot.pressure_score,
                    2,
                ),
            )

            if self._flow_is_good(
                snapshot
            ):

                return snapshot

        # ========================================================
        # FALLBACK SANS LAUNCH TRACKER
        # ========================================================

        if self.launch_tracker is None:

            while True:

                remaining = (
                    deadline
                    - time.monotonic()
                )

                if remaining <= 0:

                    return (
                        self.order_flow.snapshot(
                            mint,
                            window_sec=60,
                        )
                    )

                await asyncio.sleep(
                    min(
                        0.025,
                        remaining,
                    )
                )

                snapshot = (
                    self.order_flow.snapshot(
                        mint,
                        window_sec=60,
                    )
                )

                if snapshot is None:
                    continue

                if (
                    snapshot.total_trades
                    > 0
                ):

                    if trace_id is not None:

                        get_tracer().mark_once(
                            trace_id,
                            "flow_first_trade",
                        )

                if (
                    snapshot.total_trades
                    >= self.min_trades
                ):

                    if trace_id is not None:

                        get_tracer().mark_once(
                            trace_id,
                            "flow_5_trades",
                        )

                if self._flow_is_good(
                    snapshot
                ):

                    return snapshot

        # ========================================================
        # LAUNCH TRACKER
        # ========================================================

        state = (
            self.launch_tracker.get(
                mint
            )
        )

        if state is None:

            log.warning(
                "sniping.flow_tracker_missing",
                mint=mint,
            )

            return None

        last_trade_count = (
            snapshot.total_trades
            if snapshot is not None
            else 0
        )

        # ========================================================
        # REACTIVE LOOP
        # ========================================================

        while True:

            remaining = (
                deadline
                - time.monotonic()
            )

            if remaining <= 0:

                final_snapshot = (
                    self.order_flow.snapshot(
                        mint,
                        window_sec=60,
                    )
                )

                if (
                    final_snapshot is not None
                    and final_snapshot.total_trades
                    > 0
                    and trace_id is not None
                ):

                    get_tracer().mark_once(
                        trace_id,
                        "flow_first_trade",
                    )

                return final_snapshot

            # ----------------------------------------------------
            # SNAPSHOT AVANT ATTENTE
            # ----------------------------------------------------

            snapshot = (
                self.order_flow.snapshot(
                    mint,
                    window_sec=60,
                )
            )

            if snapshot is not None:

                current_count = (
                    snapshot.total_trades
                )

                if (
                    current_count
                    > 0
                ):

                    if trace_id is not None:

                        get_tracer().mark_once(
                            trace_id,
                            "flow_first_trade",
                        )

                if (
                    current_count
                    >= self.min_trades
                ):

                    if trace_id is not None:

                        get_tracer().mark_once(
                            trace_id,
                            "flow_5_trades",
                        )

                if (
                    current_count
                    > last_trade_count
                ):

                    previous_count = (
                        last_trade_count
                    )

                    last_trade_count = (
                        current_count
                    )

                    elapsed_ms = (
                        time.monotonic()
                        - launch_started_at
                    ) * 1000.0

                    log.info(
                        "sniping.flow_trade_received",
                        mint=mint,
                        previous_trades=(
                            previous_count
                        ),
                        current_trades=(
                            current_count
                        ),
                        new_trades=(
                            current_count
                            - previous_count
                        ),
                        tracker_trades=(
                            state.trade_count
                        ),
                        buys=(
                            snapshot.buy_count
                        ),
                        sells=(
                            snapshot.sell_count
                        ),
                        unique_buyers=(
                            snapshot.unique_buyers
                        ),
                        unique_sellers=(
                            snapshot.unique_sellers
                        ),
                        volume_sol=round(
                            snapshot.volume_sol,
                            6,
                        ),
                        buy_volume_sol=round(
                            snapshot.buy_volume_sol,
                            6,
                        ),
                        sell_volume_sol=round(
                            snapshot.sell_volume_sol,
                            6,
                        ),
                        net_flow_sol=round(
                            snapshot.net_sol_flow,
                            6,
                        ),
                        pressure=round(
                            snapshot.pressure_score,
                            2,
                        ),
                        elapsed_ms=round(
                            elapsed_ms,
                            3,
                        ),
                    )

                if self._flow_is_good(
                    snapshot
                ):

                    return snapshot

            # ----------------------------------------------------
            # ATTENTE DU PROCHAIN TRADE
            # ----------------------------------------------------

            state = (
                await self.launch_tracker.wait_for_trade(
                    mint=mint,
                    previous_trade_count=(
                        last_trade_count
                    ),
                    timeout_sec=remaining,
                )
            )

            # ----------------------------------------------------
            # TIMEOUT
            # ----------------------------------------------------

            if state is None:

                final_snapshot = (
                    self.order_flow.snapshot(
                        mint,
                        window_sec=60,
                    )
                )

                # ------------------------------------------------------------
                # Le wait_for_trade() retourne None en cas de timeout.
                # Il ne faut donc JAMAIS accéder à state.trade_count ici.
                #
                # Le LaunchTracker peut également avoir été nettoyé entre-temps.
                # On récupère donc l'état actuel de manière défensive.
                # ------------------------------------------------------------

                current_state = self.launch_tracker.get(mint)

                tracker_trade_count = (
                    current_state.trade_count
                    if current_state is not None
                    else 0
                )

                order_flow_trade_count = (
                    final_snapshot.total_trades
                    if final_snapshot is not None
                    else 0
                )

                if (
                    final_snapshot is not None
                    and final_snapshot.total_trades > 0
                    and trace_id is not None
                ):

                    get_tracer().mark_once(
                        trace_id,
                        "flow_first_trade",
                    )

                if (
                    final_snapshot is not None
                    and final_snapshot.total_trades >= self.min_trades
                    and trace_id is not None
                ):

                    get_tracer().mark_once(
                        trace_id,
                        "flow_5_trades",
                    )

                log.info(
                    "sniping.flow_timeout_snapshot",
                    mint=mint,
                    order_flow_count=order_flow_trade_count,
                    launch_tracker_count=tracker_trade_count,
                    tracker_state_present=(
                        current_state is not None
                    ),
                )

                return final_snapshot

            # ----------------------------------------------------
            # NOUVEAU TRADE
            # ----------------------------------------------------

            snapshot = (
                self.order_flow.snapshot(
                    mint,
                    window_sec=60,
                )
            )

            if snapshot is None:
                continue

            if (
                snapshot.total_trades
                > 0
            ):

                if trace_id is not None:

                    get_tracer().mark_once(
                        trace_id,
                        "flow_first_trade",
                    )

            if (
                snapshot.total_trades
                >= self.min_trades
            ):

                if trace_id is not None:

                    get_tracer().mark_once(
                        trace_id,
                        "flow_5_trades",
                    )

            last_trade_count = max(
                last_trade_count,
                snapshot.total_trades,
                state.trade_count,
            )

            log.info(
                "sniping.flow_trade_update",
                mint=mint,
                order_flow_count=(
                    snapshot.total_trades
                ),
                launch_tracker_count=(
                    state.trade_count
                ),
                buys=snapshot.buy_count,
                sells=snapshot.sell_count,
                unique_buyers=(
                    snapshot.unique_buyers
                ),
                unique_sellers=(
                    snapshot.unique_sellers
                ),
                volume_sol=round(
                    snapshot.volume_sol,
                    4,
                ),
                buy_volume_sol=round(
                    snapshot.buy_volume_sol,
                    4,
                ),
                sell_volume_sol=round(
                    snapshot.sell_volume_sol,
                    4,
                ),
                net_flow_sol=round(
                    snapshot.net_sol_flow,
                    4,
                ),
                pressure=round(
                    snapshot.pressure_score,
                    2,
                ),
            )

            if self._flow_is_good(
                snapshot
            ):

                return snapshot

    # ============================================================
    # CLEANUP
    # ============================================================

    def _cleanup_launch(
        self,
        mint: str,
        remove_tracker: bool = True,
    ) -> None:

        self._tracked_launches.pop(
            mint,
            None,
        )

        if (
            remove_tracker
            and self.launch_tracker is not None
        ):

            self.launch_tracker.remove(
                mint
            )