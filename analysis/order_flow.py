"""
order_flow.py

Analyse temps réel du flux BUY / SELL Pump.fun.

OrderFlowAnalyzer est la source de vérité des trades observés.

Il conserve les trades par mint et produit des snapshots
sur différentes fenêtres temporelles.

Pressure score :

    35 % BUY/SELL count
    30 % volume pressure
    20 % whale flow
    15 % velocity

Score final borné entre -100 et +100.
"""

from __future__ import annotations

import time

from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from utils.config_loader import Config
from utils.logger import setup_logger


log = setup_logger("order_flow")


# ================================================================
# TRADE
# ================================================================

@dataclass
class Trade:

    ts: float

    side: str

    size_sol: float

    size_usd: float

    size_token: float

    trader: str

    price: float

    is_whale: bool = False


# ================================================================
# SNAPSHOT
# ================================================================

@dataclass
class OrderFlowSnapshot:

    mint: str

    window_sec: int

    total_trades: int

    buy_count: int

    sell_count: int

    buy_sell_ratio: float

    net_sol_flow: float

    net_usd_flow: float

    volume_sol: float

    volume_usd: float

    buy_volume_sol: float

    sell_volume_sol: float

    buy_volume_usd: float

    sell_volume_usd: float

    whale_buy_count: int

    whale_sell_count: int

    whale_net_sol: float

    unique_traders: int

    unique_buyers: int

    unique_sellers: int

    avg_trade_size_sol: float

    trades_per_minute: float

    large_wallets_active: list[str]

    pressure_score: float

    fetched_at: float = field(
        default_factory=time.time
    )


# ================================================================
# ANALYZER
# ================================================================

class OrderFlowAnalyzer:

    # Un trade >= 1 SOL est considéré comme whale.
    WHALE_THRESHOLD_SOL = 1.0

    def __init__(self) -> None:

        self.cfg = Config.get()

        self._trades: dict[
            str,
            deque[Trade],
        ] = {}

        self._wallet_registry: dict[
            str,
            dict[str, int],
        ] = {}

    # ============================================================
    # RECORD TRADE
    # ============================================================

    # ============================================================
    # RECORD TRADE
    # ============================================================

    def record_trade(
        self,
        mint: str,
        trade: Trade,
    ) -> None:

        if not mint:
            return

        if trade is None:
            return

        trade.side = (
            str(trade.side)
            .lower()
            .strip()
        )

        if trade.side not in {
            "buy",
            "sell",
        }:

            log.warning(
                "order_flow.invalid_trade_side",
                mint=mint,
                side=trade.side,
            )

            return

        if mint not in self._trades:

            self._trades[mint] = deque(
                maxlen=10000
            )

        self._trades[mint].append(
            trade
        )

        current_count = len(
            self._trades[mint]
        )

        # --------------------------------------------------------
        # Registry des gros wallets.
        # --------------------------------------------------------

        if (
            trade.size_sol
            >= self.WHALE_THRESHOLD_SOL
            and trade.trader
        ):

            self._wallet_registry.setdefault(
                mint,
                {},
            )

            wallet_counts = (
                self._wallet_registry[mint]
            )

            wallet_counts[
                trade.trader
            ] = (
                wallet_counts.get(
                    trade.trader,
                    0,
                )
                + 1
            )

        # --------------------------------------------------------
        # PATCH N°2B
        #
        # Log suffisamment détaillé pour suivre :
        #
        # trade #1
        # trade #2
        # ...
        # trade #5
        #
        # sans modifier les métriques.
        # --------------------------------------------------------

        log.info(
            "order_flow.trade_received",
            mint=mint,
            trade_number=current_count,
            side=trade.side,
            trader=trade.trader,
            size_sol=round(
                trade.size_sol,
                6,
            ),
            is_whale=trade.is_whale,
        )
        
    # ============================================================
    # SNAPSHOT
    # ============================================================

    def snapshot(
        self,
        mint: str,
        window_sec: int = 300,
    ) -> Optional[OrderFlowSnapshot]:

        if not mint:
            return None

        trades = self._trades.get(mint)

        if not trades:
            return None

        now = time.time()

        recent = [
            trade
            for trade in trades
            if (
                now - trade.ts
                <= window_sec
            )
        ]

        if not recent:
            return None

        # --------------------------------------------------------
        # BUY / SELL
        # --------------------------------------------------------

        buys = [
            trade
            for trade in recent
            if trade.side == "buy"
        ]

        sells = [
            trade
            for trade in recent
            if trade.side == "sell"
        ]

        buy_count = len(buys)
        sell_count = len(sells)

        # --------------------------------------------------------
        # VOLUME
        # --------------------------------------------------------

        buy_volume_sol = sum(
            trade.size_sol
            for trade in buys
        )

        sell_volume_sol = sum(
            trade.size_sol
            for trade in sells
        )

        buy_volume_usd = sum(
            trade.size_usd
            for trade in buys
        )

        sell_volume_usd = sum(
            trade.size_usd
            for trade in sells
        )

        net_sol = (
            buy_volume_sol
            - sell_volume_sol
        )

        net_usd = (
            buy_volume_usd
            - sell_volume_usd
        )

        volume_sol = (
            buy_volume_sol
            + sell_volume_sol
        )

        volume_usd = (
            buy_volume_usd
            + sell_volume_usd
        )

        # --------------------------------------------------------
        # WHALE FLOW
        # --------------------------------------------------------

        whale_buys = [
            trade
            for trade in buys
            if trade.is_whale
        ]

        whale_sells = [
            trade
            for trade in sells
            if trade.is_whale
        ]

        whale_buy_volume = sum(
            trade.size_sol
            for trade in whale_buys
        )

        whale_sell_volume = sum(
            trade.size_sol
            for trade in whale_sells
        )

        whale_net = (
            whale_buy_volume
            - whale_sell_volume
        )

        # --------------------------------------------------------
        # UNIQUE WALLETS
        # --------------------------------------------------------

        unique_traders = len({
            trade.trader
            for trade in recent
            if trade.trader
        })

        unique_buyers = len({
            trade.trader
            for trade in buys
            if trade.trader
        })

        unique_sellers = len({
            trade.trader
            for trade in sells
            if trade.trader
        })

        # --------------------------------------------------------
        # AVERAGE TRADE
        # --------------------------------------------------------

        avg_size = (
            volume_sol
            / len(recent)
            if recent
            else 0.0
        )

        # --------------------------------------------------------
        # VELOCITY
        # --------------------------------------------------------

        trades_per_minute = (
            len(recent)
            / (window_sec / 60)
            if window_sec > 0
            else 0.0
        )

        # ========================================================
        # PRESSURE SCORE
        # ========================================================

        # --------------------------------------------------------
        # 35 % COUNT SCORE
        # --------------------------------------------------------

        count_score = (
            (
                buy_count
                - sell_count
            )
            / max(
                1,
                buy_count + sell_count,
            )
            * 100
        )

        count_score = max(
            -100,
            min(
                100,
                count_score,
            ),
        )

        # --------------------------------------------------------
        # 20 % WHALE SCORE
        # --------------------------------------------------------

        whale_score = (
            whale_net
            / max(
                1.0,
                volume_sol,
            )
            * 100
        )

        whale_score = max(
            -100,
            min(
                100,
                whale_score,
            ),
        )

        # --------------------------------------------------------
        # 15 % VELOCITY SCORE
        # --------------------------------------------------------

        velocity_score = (
            (
                trades_per_minute
                / 30.0
            )
            * 100
            - 50
        )

        velocity_score = max(
            -100,
            min(
                100,
                velocity_score,
            ),
        )

        # --------------------------------------------------------
        # 30 % VOLUME PRESSURE
        # --------------------------------------------------------

        if volume_sol > 0:

            volume_score = (
                net_sol
                / volume_sol
                * 100
            )

        else:

            volume_score = 0.0

        volume_score = max(
            -100,
            min(
                100,
                volume_score,
            ),
        )

        # --------------------------------------------------------
        # FINAL PRESSURE
        # --------------------------------------------------------

        pressure = (
            0.35 * count_score
            + 0.30 * volume_score
            + 0.20 * whale_score
            + 0.15 * velocity_score
        )

        pressure = max(
            -100,
            min(
                100,
                pressure,
            ),
        )

        # --------------------------------------------------------
        # LARGE WALLETS
        # --------------------------------------------------------

        wallets_in_window: dict[
            str,
            int,
        ] = {}

        for trade in recent:

            if (
                trade.is_whale
                and trade.trader
            ):

                wallets_in_window[
                    trade.trader
                ] = (
                    wallets_in_window.get(
                        trade.trader,
                        0,
                    )
                    + 1
                )

        top_wallets = sorted(
            wallets_in_window.items(),
            key=lambda item: -item[1],
        )[:5]

        snapshot = OrderFlowSnapshot(

            mint=mint,

            window_sec=window_sec,

            total_trades=len(recent),

            buy_count=buy_count,

            sell_count=sell_count,

            buy_sell_ratio=(
                buy_count
                / max(
                    1,
                    sell_count,
                )
            ),

            net_sol_flow=net_sol,

            net_usd_flow=net_usd,

            volume_sol=volume_sol,

            volume_usd=volume_usd,

            buy_volume_sol=buy_volume_sol,

            sell_volume_sol=sell_volume_sol,

            buy_volume_usd=buy_volume_usd,

            sell_volume_usd=sell_volume_usd,

            whale_buy_count=len(
                whale_buys
            ),

            whale_sell_count=len(
                whale_sells
            ),

            whale_net_sol=whale_net,

            unique_traders=unique_traders,

            unique_buyers=unique_buyers,

            unique_sellers=unique_sellers,

            avg_trade_size_sol=avg_size,

            trades_per_minute=trades_per_minute,

            large_wallets_active=[
                wallet[0]
                for wallet in top_wallets
            ],

            pressure_score=pressure,
        )

        log.debug(
            "order_flow.snapshot",
            mint=mint,
            window_sec=window_sec,
            trades=snapshot.total_trades,
            buys=snapshot.buy_count,
            sells=snapshot.sell_count,
            unique_buyers=snapshot.unique_buyers,
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
            whale_net_sol=round(
                snapshot.whale_net_sol,
                4,
            ),
            pressure=round(
                snapshot.pressure_score,
                2,
            ),
        )

        return snapshot

    # ============================================================
    # MULTI WINDOW
    # ============================================================

    def get_multi_window(
        self,
        mint: str,
    ) -> dict[
        int,
        Optional[OrderFlowSnapshot],
    ]:

        return {

            60: self.snapshot(
                mint,
                60,
            ),

            300: self.snapshot(
                mint,
                300,
            ),

            900: self.snapshot(
                mint,
                900,
            ),

            3600: self.snapshot(
                mint,
                3600,
            ),
        }