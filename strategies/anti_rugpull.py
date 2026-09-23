"""
anti_rugpull.py

Anti-rugpull en deux étapes :

1. check_fast()
   Vérifications compatibles avec un token qui vient d'être créé.

2. check_delayed()
   Vérifications complètes après quelques secondes d'activité.

La classe implémente également l'interface BaseStrategy afin de pouvoir
être utilisée partout où une BaseStrategy est attendue.
"""

from __future__ import annotations

from dataclasses import dataclass

from chains.base_chain import BaseChainAdapter
from strategies.base_strategy import (
    BaseStrategy,
    Signal,
    SignalType,
)

from utils.config_loader import Config
from utils.logger import setup_logger


log = setup_logger("anti_rugpull")


@dataclass
class RugpullVerdict:
    is_safe: bool
    reasons: list[str]
    score: float


class AntiRugpullStrategy(BaseStrategy):

    name = "anti_rugpull"

    def __init__(
        self,
        adapter: BaseChainAdapter,
    ) -> None:

        super().__init__(adapter)

        self.cfg = (
            Config.get()
            .get_nested(
                "strategies",
                "anti_rugpull",
                default={},
            )
        )

        self.min_holders = self.cfg.get(
            "min_holders",
            50,
        )

        self.max_top10_pct = self.cfg.get(
            "max_top10_holders_pct",
            0.40,
        )

        self.min_liquidity_usd = self.cfg.get(
            "min_liquidity_usd",
            10_000,
        )

        self.require_renounced_mint = self.cfg.get(
            "require_renounced_mint",
            False,
        )

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        AntiRugpullStrategy n'est pas une stratégie autonome qui tourne
        en permanence.

        Les contrôles sont déclenchés par Executor avant un BUY.

        On garde donc cette méthode pour satisfaire BaseStrategy.
        """

        try:
            await self._stop_loop()
        except asyncio.CancelledError:
            raise

    async def _stop_loop(self) -> None:
        """
        Boucle passive.

        Cette stratégie est principalement utilisée comme gate par Executor.
        """

        import asyncio

        while True:
            await asyncio.sleep(3600)

    async def evaluate(
        self,
        token_address: str,
        *args,
        **kwargs,
    ) -> Signal:
        """
        Interface BaseStrategy.

        Effectue le contrôle rapide et retourne un Signal représentant
        le résultat du contrôle.

        Cette méthode n'est normalement pas utilisée par Executor :
        Executor utilise directement check().
        """

        verdict = await self.check(token_address)

        if verdict.is_safe:
            signal_type = SignalType.BUY
            confidence = max(
                0.0,
                min(1.0, 1.0 - verdict.score),
            )
            reason = "Anti-rug check passed"
        else:
            signal_type = SignalType.HOLD
            confidence = max(
                0.0,
                min(1.0, verdict.score),
            )
            reason = (
                "Anti-rug check failed: "
                + ", ".join(verdict.reasons)
            )

        return Signal(
            strategy=self.name,
            chain=getattr(
                self.adapter,
                "chain",
                "unknown",
            ),
            token_address=token_address,
            signal_type=signal_type,
            suggested_size_pct=0.0,
            confidence=confidence,
            reason=reason,
        )

    # ------------------------------------------------------------------
    # Anti-rug API
    # ------------------------------------------------------------------

    async def check(
        self,
        token_address: str,
        skip_honeypot: bool = False,
    ) -> RugpullVerdict:
        """
        Point d'entrée utilisé par Executor.

        Pour l'instant, on effectue le contrôle rapide.

        skip_honeypot est conservé pour compatibilité avec Executor et
        permettra d'ajouter plus tard un contrôle honeypot coûteux.
        """

        if not token_address:
            return RugpullVerdict(
                is_safe=False,
                reasons=["missing_token_address"],
                score=1.0,
            )

        try:
            verdict = await self.check_fast(
                token_address
            )

            if not verdict.is_safe:
                return verdict

            # Le contrôle delayed peut être activé par configuration.
            delayed_enabled = self.cfg.get(
                "delayed_check_enabled",
                False,
            )

            if delayed_enabled:
                return await self.check_delayed(
                    token_address
                )

            return verdict

        except Exception as e:

            log.warning(
                "anti_rugpull.check_failed",
                token=token_address,
                error=str(e),
            )

            # Fail closed :
            # si le système de sécurité tombe, on bloque le BUY.
            return RugpullVerdict(
                is_safe=False,
                reasons=[
                    f"anti_rug_check_error: {e}"
                ],
                score=1.0,
            )

    async def check_fast(
        self,
        token_address: str,
    ) -> RugpullVerdict:
        """
        Vérifications rapides compatibles avec un token
        qui vient juste d'être créé.

        Pour le moment, aucune vérification supplémentaire
        n'est effectuée ici.
        """

        return RugpullVerdict(
            is_safe=True,
            reasons=[],
            score=0.0,
        )

    async def check_delayed(
        self,
        token_address: str,
    ) -> RugpullVerdict:
        """
        Vérifications complètes après quelques secondes
        d'activité.

        Pour le moment, aucune vérification supplémentaire
        n'est effectuée ici.
        """

        return RugpullVerdict(
            is_safe=True,
            reasons=[],
            score=0.0,
        )