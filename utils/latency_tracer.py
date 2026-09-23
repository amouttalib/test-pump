"""
latency_tracer.py

Instrumentation de latence du pipeline de sniping.

Stages possibles :

    detect
    parsed
    registered
    flow_watch_started
    flow_first_trade
    flow_5_trades
    flow_gate
    scored
    buy_signal
    signal_emitted
    built
    submitted
    confirmed

Le tracer utilise time.perf_counter() afin de mesurer des durées
monotoniques indépendantes des changements d'horloge système.
"""

from __future__ import annotations

import time

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

from utils.logger import setup_logger


log = setup_logger("latency")


_MAX_TRACES_PER_OP = 500


@dataclass
class Trace:

    op: str

    token: str

    start: float

    marks: dict[str, float] = field(
        default_factory=dict
    )

    finished: bool = False


class LatencyTracer:
    """
    Traceur de latence asyncio.

    Toutes les mesures sont en millisecondes.

    Les marks contiennent le temps écoulé depuis le début
    du trace.
    """

    def __init__(self) -> None:

        self._completed: dict[
            str,
            deque,
        ] = defaultdict(
            lambda: deque(
                maxlen=_MAX_TRACES_PER_OP
            )
        )

        self._active: dict[
            int,
            Trace,
        ] = {}

        self._next_id: int = 0

        self.alert_thresholds: dict[
            str,
            float,
        ] = {
            "snipe": 2000.0,
            "buy": 5000.0,
            "sell": 5000.0,
            "exit": 3000.0,
        }

    # ============================================================
    # START
    # ============================================================

    def start(
        self,
        op: str,
        token: str = "",
    ) -> int:

        tid = self._next_id

        self._next_id += 1

        self._active[
            tid
        ] = Trace(
            op=op,
            token=token,
            start=time.perf_counter(),
        )

        return tid

    # ============================================================
    # MARK
    # ============================================================

    def mark(
        self,
        trace_id: int,
        stage: str,
        at: Optional[float] = None,
    ) -> None:

        tr = self._active.get(
            trace_id
        )

        if tr is None:
            return

        # --------------------------------------------------------
        # Un stage ne doit être enregistré qu'une seule fois.
        # --------------------------------------------------------

        if stage in tr.marks:
            return

        now = (
            time.perf_counter()
            if at is None
            else at
        )

        tr.marks[
            stage
        ] = (
            now
            - tr.start
        ) * 1000.0

    # ============================================================
    # MARK ONCE
    # ============================================================

    def mark_once(
        self,
        trace_id: int,
        stage: str,
        at: Optional[float] = None,
    ) -> None:
        """
        Enregistre un stage une seule fois.

        Cette méthode est volontairement un alias explicite
        de mark(), afin de rendre l'intention claire dans le
        code métier.

        Le paramètre 'at' permet de fournir un timestamp
        monotonic précis capturé avant un asyncio.create_task().

        Exemple :

            detected_at = time.monotonic()

            tracer.mark_once(
                trace_id,
                "detect",
                at=detected_at,
            )
        """

        tr = self._active.get(
            trace_id
        )

        if tr is None:
            return

        if stage in tr.marks:
            return

        now = (
            time.perf_counter()
            if at is None
            else at
        )

        tr.marks[
            stage
        ] = (
            now
            - tr.start
        ) * 1000.0

    # ============================================================
    # FINISH
    # ============================================================

    def finish(
        self,
        trace_id: int,
    ) -> Optional[float]:

        tr = self._active.pop(
            trace_id,
            None,
        )

        if tr is None:
            return None

        tr.finished = True

        total_ms = (
            time.perf_counter()
            - tr.start
        ) * 1000.0

        marks = dict(
            tr.marks
        )

        self._completed[
            tr.op
        ].append(
            (
                total_ms,
                marks,
            )
        )

        threshold = (
            self.alert_thresholds.get(
                tr.op,
                999999.0,
            )
        )

        stage_log = {
            key: round(
                value,
                2,
            )
            for key, value in marks.items()
        }

        if total_ms > threshold:

            log.warning(
                "latency.slow_op",
                op=tr.op,
                token=tr.token,
                total_ms=round(
                    total_ms,
                    2,
                ),
                threshold=threshold,
                stages=stage_log,
            )

        else:

            log.info(
                "latency.trace",
                op=tr.op,
                token=tr.token,
                total_ms=round(
                    total_ms,
                    2,
                ),
                stages=stage_log,
            )

        return total_ms

    # ============================================================
    # STATS
    # ============================================================

    def stats(self) -> dict:

        result: dict = {}

        for op, traces in (
            self._completed.items()
        ):

            if not traces:
                continue

            totals = [
                trace[0]
                for trace in traces
            ]

            entry = {
                "count": len(
                    totals
                ),
                "total_ms": self._percentiles(
                    totals
                ),
                "stages": {},
            }

            # ----------------------------------------------------
            # Les stages sont calculés indépendamment pour chaque
            # trace.
            #
            # Exemple :
            #
            # detect = 0.2
            # parsed = 1.3
            # first_trade = 152.4
            # five_trades = 483.1
            # flow_gate = 487.8
            #
            # On calcule :
            #
            # start -> detect
            # detect -> parsed
            # parsed -> first_trade
            # first_trade -> five_trades
            # five_trades -> flow_gate
            #
            # et non pas selon un ordre global de stages.
            # ----------------------------------------------------

            pair_deltas: dict[
                str,
                list[float],
            ] = defaultdict(list)

            first_stage_deltas: dict[
                str,
                list[float],
            ] = defaultdict(list)

            for _, marks in traces:

                if not marks:
                    continue

                ordered = sorted(
                    marks.items(),
                    key=lambda item: item[1],
                )

                # ------------------------------------------------
                # start -> premier stage
                # ------------------------------------------------

                first_stage, first_time = (
                    ordered[0]
                )

                first_stage_deltas[
                    f"start->{first_stage}"
                ].append(
                    first_time
                )

                # ------------------------------------------------
                # stage A -> stage B
                # ------------------------------------------------

                for index in range(
                    len(ordered) - 1
                ):

                    stage_a, time_a = (
                        ordered[index]
                    )

                    stage_b, time_b = (
                        ordered[
                            index + 1
                        ]
                    )

                    delta = (
                        time_b
                        - time_a
                    )

                    if delta >= 0:

                        pair_deltas[
                            f"{stage_a}->{stage_b}"
                        ].append(
                            delta
                        )

            # ----------------------------------------------------
            # Percentiles start -> premier stage
            # ----------------------------------------------------

            for stage_name, values in (
                first_stage_deltas.items()
            ):

                entry[
                    "stages"
                ][stage_name] = (
                    self._percentiles(
                        values
                    )
                )

            # ----------------------------------------------------
            # Percentiles stage -> stage
            # ----------------------------------------------------

            for stage_name, values in (
                pair_deltas.items()
            ):

                entry[
                    "stages"
                ][stage_name] = (
                    self._percentiles(
                        values
                    )
                )

            result[
                op
            ] = entry

        return result

    # ============================================================
    # PERCENTILES
    # ============================================================

    @staticmethod
    def _percentiles(
        values: list[float],
    ) -> dict[str, float]:

        if not values:

            return {
                "p50": 0,
                "p95": 0,
                "p99": 0,
            }

        s = sorted(
            values
        )

        n = len(s)

        def pct(
            p: float,
        ) -> float:

            idx = max(
                0,
                min(
                    n - 1,
                    int(
                        p
                        / 100
                        * (
                            n - 1
                        )
                    ),
                ),
            )

            return round(
                s[idx],
                1,
            )

        return {
            "p50": pct(50),
            "p95": pct(95),
            "p99": pct(99),
        }


# ================================================================
# SINGLETON
# ================================================================

_tracer: Optional[
    LatencyTracer
] = None


def get_tracer() -> LatencyTracer:

    global _tracer

    if _tracer is None:

        _tracer = LatencyTracer()

    return _tracer