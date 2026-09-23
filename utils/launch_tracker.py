from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


from utils.logger import setup_logger


log = setup_logger(
    "launch_tracker"
)


@dataclass
class LaunchState:

    mint: str

    created_at: float

    event: dict

    trade_count: int = 0

    decision_made: bool = False

    ready: asyncio.Event = field(
        default_factory=asyncio.Event
    )

    trade_event: asyncio.Event = field(
        default_factory=asyncio.Event
    )


class LaunchTracker:

    def __init__(
        self,
        min_trades: int = 5,
        timeout_sec: float = 3.0,
    ) -> None:

        self.min_trades = min_trades

        self.timeout_sec = timeout_sec

        self._launches: dict[
            str,
            LaunchState,
        ] = {}

        # --------------------------------------------------------
        # Trades reçus AVANT register().
        #
        # OrderFlow reste la source de vérité.
        # Ce compteur sert uniquement à réveiller correctement
        # le LaunchTracker lorsque le CREATE arrive après les
        # premiers trades.
        # --------------------------------------------------------

        self._pending_trades: dict[
            str,
            int,
        ] = {}

    # ============================================================
    # REGISTER
    # ============================================================

    def register(
        self,
        mint: str,
        event: dict,
        existing_trades: int = 0,
        created_at: float | None = None,
    ) -> LaunchState:

        pending_trades = (
            self._pending_trades.pop(
                mint,
                0,
            )
        )

        state = self._launches.get(
            mint
        )

        if state is not None:

            state.trade_count = max(
                state.trade_count,
                existing_trades,
                pending_trades,
            )

            if (
                state.trade_count
                >= self.min_trades
            ):

                state.ready.set()

            log.info(
                "launch_tracker.register",
                mint=mint,
                existing_trades=existing_trades,
                pending_trades=pending_trades,
                tracker_trades=state.trade_count,
                ready=state.ready.is_set(),
                reused=True,
            )

            return state

        state = LaunchState(
            mint=mint,
            created_at=(
                created_at
                if created_at is not None
                else time.time()
            ),
            event=event,
            trade_count=max(
                existing_trades,
                pending_trades,
            ),
        )

        self._launches[
            mint
        ] = state

        if (
            state.trade_count
            >= self.min_trades
        ):

            state.ready.set()

        log.info(
            "launch_tracker.register",
            mint=mint,
            existing_trades=existing_trades,
            pending_trades=pending_trades,
            tracker_trades=state.trade_count,
            ready=state.ready.is_set(),
            reused=False,
        )

        return state

    # ============================================================
    # RECORD TRADE
    # ============================================================

    def record_trade(
        self,
        mint: str,
    ) -> int:

        if not mint:
            return 0

        state = self._launches.get(
            mint
        )

        # --------------------------------------------------------
        # Trade reçu AVANT register().
        # --------------------------------------------------------

        if state is None:

            pending = (
                self._pending_trades.get(
                    mint,
                    0,
                )
                + 1
            )

            self._pending_trades[
                mint
            ] = pending

            log.info(
                "launch_tracker.trade_before_register",
                mint=mint,
                pending_trades=pending,
            )

            return pending

        # --------------------------------------------------------
        # Launch déjà enregistré.
        # --------------------------------------------------------

        state.trade_count += 1

        state.trade_event.set()

        if (
            state.trade_count
            >= self.min_trades
        ):

            state.ready.set()

        log.info(
            "launch_tracker.trade_recorded",
            mint=mint,
            tracker_trades=state.trade_count,
            ready=state.ready.is_set(),
        )

        return state.trade_count

    # ============================================================
    # WAIT FOR TRADE
    # ============================================================

    async def wait_for_trade(
        self,
        mint: str,
        previous_trade_count: int,
        timeout_sec: float | None = None,
    ) -> LaunchState | None:

        state = self._launches.get(
            mint
        )

        if state is None:
            return None

        # --------------------------------------------------------
        # Vérification immédiate.
        # --------------------------------------------------------

        if (
            state.trade_count
            > previous_trade_count
        ):

            return state

        # --------------------------------------------------------
        # Éviter la race :
        #
        # clear()
        # recheck
        # wait()
        # --------------------------------------------------------

        state.trade_event.clear()

        if (
            state.trade_count
            > previous_trade_count
        ):

            return state

        timeout = (
            self.timeout_sec
            if timeout_sec is None
            else max(
                0.0,
                timeout_sec,
            )
        )

        if timeout <= 0:
            return None

        try:

            await asyncio.wait_for(
                state.trade_event.wait(),
                timeout=timeout,
            )

        except asyncio.TimeoutError:

            return None

        return state

    # ============================================================
    # WAIT FOR CONFIRMATION
    # ============================================================

    async def wait_for_confirmation(
        self,
        mint: str,
    ) -> LaunchState | None:

        state = self._launches.get(
            mint
        )

        if state is None:
            return None

        if (
            state.trade_count
            >= self.min_trades
        ):

            return state

        try:

            await asyncio.wait_for(
                state.ready.wait(),
                timeout=self.timeout_sec,
            )

        except asyncio.TimeoutError:

            pass

        return state

    # ============================================================
    # GET
    # ============================================================

    def get(
        self,
        mint: str,
    ) -> LaunchState | None:

        return self._launches.get(
            mint
        )

    # ============================================================
    # MARK DECISION
    # ============================================================

    def mark_decision(
        self,
        mint: str,
    ) -> bool:

        state = self._launches.get(
            mint
        )

        if state is None:
            return False

        if state.decision_made:
            return False

        state.decision_made = True

        log.info(
            "launch_tracker.decision_made",
            mint=mint,
        )

        return True

    # ============================================================
    # REMOVE
    # ============================================================

    def remove(
        self,
        mint: str,
    ) -> None:

        self._launches.pop(
            mint,
            None,
        )

        self._pending_trades.pop(
            mint,
            None,
        )