from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from typing import Optional

import base58


PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def _event_discriminator(name: str) -> bytes:
    return hashlib.sha256(
        f"event:{name}".encode()
    ).digest()[:8]


TRADE_EVENT_DISCRIMINATOR = _event_discriminator(
    "TradeEvent"
)


@dataclass(slots=True)
class ParsedPumpTrade:
    mint: str
    sol_amount_lamports: int
    token_amount_raw: int
    is_buy: bool
    user: str
    timestamp: int


def _pubkey(raw: bytes) -> str:
    return base58.b58encode(raw).decode("ascii")


def parse_trade_event(
    data: bytes,
) -> Optional[ParsedPumpTrade]:
    """
    Parse the stable leading portion of Pump.fun TradeEvent.

    Layout:

      discriminator     8
      mint              32
      sol_amount         8
      token_amount       8
      is_buy             1
      user              32
      timestamp          8

    Total minimum: 97 bytes.

    Later versions of TradeEvent may append additional fields.
    We intentionally only require the stable leading portion.
    """

    minimum = (
        8
        + 32
        + 8
        + 8
        + 1
        + 32
        + 8
    )

    if len(data) < minimum:
        return None

    if data[:8] != TRADE_EVENT_DISCRIMINATOR:
        return None

    offset = 8

    mint = _pubkey(
        data[offset:offset + 32]
    )
    offset += 32

    sol_amount = int.from_bytes(
        data[offset:offset + 8],
        "little",
    )
    offset += 8

    token_amount = int.from_bytes(
        data[offset:offset + 8],
        "little",
    )
    offset += 8

    is_buy = bool(
        data[offset]
    )
    offset += 1

    user = _pubkey(
        data[offset:offset + 32]
    )
    offset += 32

    timestamp = int.from_bytes(
        data[offset:offset + 8],
        "little",
        signed=True,
    )

    return ParsedPumpTrade(
        mint=mint,
        sol_amount_lamports=sol_amount,
        token_amount_raw=token_amount,
        is_buy=is_buy,
        user=user,
        timestamp=timestamp,
    )


def parse_trade_from_logs(
    logs: list[str],
) -> Optional[ParsedPumpTrade]:
    """
    Extract the first Pump.fun TradeEvent from raw
    logsSubscribe logs.

    IMPORTANT:
    This function intentionally keeps the existing public API.

    A future patch can introduce parse_trades_from_logs()
    to process multiple TradeEvent objects contained in the
    same transaction.
    """

    for line in logs:

        if "Program data:" not in line:
            continue

        encoded = line.split(
            "Program data:",
            1,
        )[1].strip()

        try:

            raw = base64.b64decode(
                encoded
            )

        except Exception:

            continue

        parsed = parse_trade_event(
            raw
        )

        if parsed is not None:

            return parsed

    return None