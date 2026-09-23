"""
pumpfun_ix.py
=============
Pump.fun instruction helpers + low-latency CREATE detection.

Current responsibilities
------------------------
1. Build legacy pump.fun buy/sell instructions used by the current executor.
2. Build the ATA creation instruction used by the current executor.
3. Detect and decode pump.fun CreateEvent directly from logsSubscribe logs.
4. Avoid false positives from unrelated "Program data:" log entries.

CREATE detection strategy
-------------------------
Preferred:
    logsSubscribe
        -> "Instruction: Create" / "Instruction: CreateV2"
        -> Anchor CreateEvent discriminator
        -> decode CreateEvent
        -> mint
        -> derive bonding curve PDA

Fallback:
    enriched text logs:
        mint=...
        bonding_curve=...
        user=...

Last resort:
    caller performs getTransaction.

IMPORTANT
---------
Do not add transaction-level deduplication here. The websocket producer
(sol/adapter.py) owns notification deduplication because it knows the
subscription lifecycle.

The parser itself must remain pure: same logs + same signature => same result.
"""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass
from typing import Optional

import base58


# ============================================================================
# Program constants
# ============================================================================

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

SYSTEM_PROGRAM = "11111111111111111111111111111111"

RENT_SYSVAR = "SysvarRent111111111111111111111111111111111"

# Wrapped SOL used by the current Pump.fun V2 interface.
WSOL_MINT = "So11111111111111111111111111111111111111112"

# Token-2022 program used by create_v2 coins.
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


# ============================================================================
# Discriminators
# ============================================================================

def _discriminator(method: str) -> bytes:
    """
    Anchor instruction discriminator.

    sha256("global:<instruction>")[:8]
    """
    return hashlib.sha256(
        f"global:{method}".encode("utf-8")
    ).digest()[:8]


def _event_discriminator(event_name: str) -> bytes:
    """
    Anchor event discriminator.

    sha256("event:<EventName>")[:8]
    """
    return hashlib.sha256(
        f"event:{event_name}".encode("utf-8")
    ).digest()[:8]


BUY_DISCRIMINATOR = _discriminator("buy")
SELL_DISCRIMINATOR = _discriminator("sell")
CREATE_DISCRIMINATOR = _discriminator("create")

# This is the discriminator that MUST be checked before interpreting
# a `Program data:` payload as a CreateEvent.
CREATE_EVENT_DISCRIMINATOR = _event_discriminator("CreateEvent")


# ============================================================================
# PDA derivation
# ============================================================================

_GLOBAL_PDA_CACHE: Optional[str] = None


def derive_global_pda() -> str:
    """
    Pump.fun global PDA.

    Seeds:
        ["global"]
    """
    global _GLOBAL_PDA_CACHE

    if _GLOBAL_PDA_CACHE is None:
        from solders.pubkey import Pubkey

        pda, _ = Pubkey.find_program_address(
            [b"global"],
            Pubkey.from_string(PUMP_FUN_PROGRAM),
        )

        _GLOBAL_PDA_CACHE = str(pda)

    return _GLOBAL_PDA_CACHE


def __getattr__(name):
    """
    Backwards compatibility for code importing PUMP_FUN_GLOBAL.
    """
    if name == "PUMP_FUN_GLOBAL":
        return derive_global_pda()

    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def derive_bonding_curve_pda(mint: str) -> str:
    """
    Pump.fun bonding curve PDA.

    Seeds:
        ["bonding-curve", mint]
    """
    from solders.pubkey import Pubkey

    mint_pk = Pubkey.from_string(mint)

    pda, _ = Pubkey.find_program_address(
        [
            b"bonding-curve",
            bytes(mint_pk),
        ],
        Pubkey.from_string(PUMP_FUN_PROGRAM),
    )

    return str(pda)


def derive_fee_recipient() -> str:
    """
    Legacy fee recipient.

    Kept for backwards compatibility with the current executor.

    NOTE:
    Current Pump.fun V2 uses fee recipients selected from global
    configuration. This function should be replaced when BUY/SELL V2
    is implemented.
    """
    return "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM"


# ============================================================================
# Instruction account model
# ============================================================================

@dataclass
class PumpfunAccounts:
    """Resolved account set for the current legacy buy/sell builder."""

    global_account: str
    fee_recipient: str
    mint: str
    bonding_curve: str
    associated_user: str
    user: str


def resolve_buy_accounts(
    buyer_wallet: str,
    mint: str,
) -> PumpfunAccounts:
    """
    Resolve accounts used by the current legacy buy/sell builders.

    The ATA is derived locally and does not require an RPC call.
    """
    from solders.pubkey import Pubkey

    owner = Pubkey.from_string(buyer_wallet)
    mint_pk = Pubkey.from_string(mint)

    ata, _ = Pubkey.find_program_address(
        [
            bytes(owner),
            bytes(Pubkey.from_string(TOKEN_PROGRAM)),
            bytes(mint_pk),
        ],
        Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM),
    )

    return PumpfunAccounts(
        global_account=derive_global_pda(),
        fee_recipient=derive_fee_recipient(),
        mint=mint,
        bonding_curve=derive_bonding_curve_pda(mint),
        associated_user=str(ata),
        user=buyer_wallet,
    )


# ============================================================================
# Legacy BUY instruction
# ============================================================================

def build_buy_ix(
    buyer_wallet: str,
    mint: str,
    token_amount_raw: int,
    max_sol_cost_lamports: int,
):
    """
    Build the current legacy pump.fun `buy` instruction.

    IMPORTANT:
    This function is intentionally preserved for the current executor.
    A separate V2 migration should replace this function once the current
    CREATE pipeline has been validated.
    """
    from solders.instruction import AccountMeta, Instruction
    from solders.pubkey import Pubkey

    if token_amount_raw <= 0:
        raise ValueError("token_amount_raw must be > 0")

    if max_sol_cost_lamports <= 0:
        raise ValueError("max_sol_cost_lamports must be > 0")

    accs = resolve_buy_accounts(
        buyer_wallet,
        mint,
    )

    metas = [
        AccountMeta(
            pubkey=Pubkey.from_string(accs.global_account),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.fee_recipient),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.mint),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.bonding_curve),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.associated_user),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.user),
            is_signer=True,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(SYSTEM_PROGRAM),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(TOKEN_PROGRAM),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(RENT_SYSVAR),
            is_signer=False,
            is_writable=False,
        ),
    ]

    data = (
        BUY_DISCRIMINATOR
        + int(token_amount_raw).to_bytes(8, "little")
        + int(max_sol_cost_lamports).to_bytes(8, "little")
    )

    return Instruction(
        program_id=Pubkey.from_string(PUMP_FUN_PROGRAM),
        accounts=metas,
        data=data,
    )


# ============================================================================
# Legacy SELL instruction
# ============================================================================

def build_sell_ix(
    seller_wallet: str,
    mint: str,
    token_amount_raw: int,
):
    """
    Build the current legacy pump.fun `sell` instruction.

    Kept for compatibility with the current executor.
    V2 migration will be handled separately.
    """
    from solders.instruction import AccountMeta, Instruction
    from solders.pubkey import Pubkey

    if token_amount_raw <= 0:
        raise ValueError("token_amount_raw must be > 0")

    accs = resolve_buy_accounts(
        seller_wallet,
        mint,
    )

    metas = [
        AccountMeta(
            pubkey=Pubkey.from_string(accs.global_account),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.fee_recipient),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.mint),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.bonding_curve),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.associated_user),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.user),
            is_signer=True,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(SYSTEM_PROGRAM),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(TOKEN_PROGRAM),
            is_signer=False,
            is_writable=False,
        ),
    ]

    data = (
        SELL_DISCRIMINATOR
        + int(token_amount_raw).to_bytes(8, "little")
    )

    return Instruction(
        program_id=Pubkey.from_string(PUMP_FUN_PROGRAM),
        accounts=metas,
        data=data,
    )


# ============================================================================
# ATA creation
# ============================================================================

def build_create_ata_ix(
    buyer_wallet: str,
    mint: str,
):
    """
    Build the Associated Token Program CreateIdempotent instruction.

    This preserves the interface expected by the current executor.

    NOTE:
    The current executor's BUY path still assumes the legacy SPL Token
    program. Token-2022 support will be handled with the V2 execution
    migration.
    """
    from solders.instruction import AccountMeta, Instruction
    from solders.pubkey import Pubkey

    accs = resolve_buy_accounts(
        buyer_wallet,
        mint,
    )

    # Associated Token Program CreateIdempotent:
    #
    # payer
    # associated_token
    # owner
    # mint
    # system_program
    # token_program
    #
    # The previous implementation had an incorrect account layout.
    metas = [
        AccountMeta(
            pubkey=Pubkey.from_string(accs.user),
            is_signer=True,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.associated_user),
            is_signer=False,
            is_writable=True,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.user),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(accs.mint),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(SYSTEM_PROGRAM),
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=Pubkey.from_string(TOKEN_PROGRAM),
            is_signer=False,
            is_writable=False,
        ),
    ]

    # Associated Token Program:
    # CreateIdempotent discriminator = 1
    data = b"\x01"

    return Instruction(
        program_id=Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM),
        accounts=metas,
        data=data,
    )


# ============================================================================
# CREATE EVENT parsing
# ============================================================================

# Base58 public key:
# - excludes 0/O/I/l
# - normal Solana pubkeys are 32-byte values encoded to 32-44 chars.
_BASE58_PUBKEY_RE = r"[1-9A-HJ-NP-Za-km-z]{32,44}"

_KV_RE = re.compile(
    rf"(\w+)=({_BASE58_PUBKEY_RE})"
)


def _extract_kv_pubkeys(
    lines: list[str],
) -> dict[str, str]:
    """
    Extract pubkey-looking key=value pairs from enriched logs.

    This is a fallback only. The preferred source is the Anchor
    CreateEvent payload.
    """
    found: dict[str, str] = {}

    for line in lines:
        for key, value in _KV_RE.findall(line):
            found[key] = value

    return found


class _BorshReader:
    """
    Minimal zero-allocation-ish Borsh reader for CreateEvent.

    We only decode the fields needed by the sniper:
        name
        symbol
        uri
        mint
        bonding_curve
        user

    Remaining fields are deliberately ignored.
    """

    __slots__ = ("_data", "_offset")

    def __init__(
        self,
        data: bytes,
        offset: int = 0,
    ):
        self._data = data
        self._offset = offset

    @property
    def offset(self) -> int:
        return self._offset

    def remaining(self) -> int:
        return len(self._data) - self._offset

    def read_u32(self) -> int:
        if self.remaining() < 4:
            raise ValueError("CreateEvent payload truncated: u32")

        value = int.from_bytes(
            self._data[
                self._offset:self._offset + 4
            ],
            "little",
        )

        self._offset += 4
        return value

    def read_i64(self) -> int:
        if self.remaining() < 8:
            raise ValueError("CreateEvent payload truncated: i64")

        value = int.from_bytes(
            self._data[
                self._offset:self._offset + 8
            ],
            "little",
            signed=True,
        )

        self._offset += 8
        return value

    def read_u64(self) -> int:
        if self.remaining() < 8:
            raise ValueError("CreateEvent payload truncated: u64")

        value = int.from_bytes(
            self._data[
                self._offset:self._offset + 8
            ],
            "little",
        )

        self._offset += 8
        return value

    def read_bool(self) -> bool:
        if self.remaining() < 1:
            raise ValueError("CreateEvent payload truncated: bool")

        value = self._data[self._offset] != 0
        self._offset += 1
        return value

    def read_bytes(self, length: int) -> bytes:
        if length < 0:
            raise ValueError("negative length")

        if self.remaining() < length:
            raise ValueError(
                f"CreateEvent payload truncated: need {length} bytes"
            )

        value = self._data[
            self._offset:self._offset + length
        ]

        self._offset += length
        return value

    def read_pubkey(self) -> str:
        return base58.b58encode(
            self.read_bytes(32)
        ).decode("ascii")

    def read_string(self) -> str:
        length = self.read_u32()

        # Safety guard against malformed/random Program data.
        if length > 1024 * 1024:
            raise ValueError(
                f"unreasonable Borsh string length: {length}"
            )

        raw = self.read_bytes(length)

        return raw.decode(
            "utf-8",
            errors="strict",
        )


@dataclass
class ParsedLaunch:
    """
    Normalized CREATE result.

    mint:
        Token mint.

    bonding_curve:
        Pump.fun bonding curve PDA.

    user:
        User that created the token.

    signature:
        Solana transaction signature.

    raw_logs:
        Original logs for diagnostics.

    needs_full_fetch:
        True when the logs did not contain enough information and the
        caller should perform getTransaction.
    """

    mint: Optional[str]
    bonding_curve: Optional[str]
    user: Optional[str]
    signature: str
    raw_logs: list[str]
    needs_full_fetch: bool


def _decode_create_event_payload(
    raw: bytes,
) -> Optional[ParsedLaunch]:
    """
    Decode a raw Anchor CreateEvent payload.

    Expected beginning:

        8 bytes:
            CreateEvent discriminator

        Borsh:
            name: string
            symbol: string
            uri: string
            mint: pubkey
            bonding_curve: pubkey
            user: pubkey
            ...

    The current Pump.fun CreateEvent contains additional fields after
    user. We do not need to decode those fields for launch detection.

    This is important: we intentionally do NOT assume that the final
    96 bytes are mint/bonding_curve/user.
    """

    if len(raw) < 8:
        return None

    if raw[:8] != CREATE_EVENT_DISCRIMINATOR:
        return None

    try:
        reader = _BorshReader(
            raw,
            offset=8,
        )

        # These three strings are part of CreateEvent and allow us to
        # correctly locate the pubkey fields.
        _name = reader.read_string()
        _symbol = reader.read_string()
        _uri = reader.read_string()

        mint = reader.read_pubkey()
        bonding_curve = reader.read_pubkey()
        user = reader.read_pubkey()

        # Basic sanity checks.
        if not mint:
            return None

        if mint == PUMP_FUN_PROGRAM:
            return None

        if mint == SYSTEM_PROGRAM:
            return None

        return ParsedLaunch(
            mint=mint,
            bonding_curve=bonding_curve,
            user=user,
            signature="",
            raw_logs=[],
            needs_full_fetch=False,
        )

    except (
        UnicodeDecodeError,
        ValueError,
        IndexError,
    ):
        return None


def _decode_program_data_line(
    line: str,
) -> Optional[ParsedLaunch]:
    """
    Decode one `Program data:` log line.

    We support the normal base64 representation emitted by Solana
    logsSubscribe.

    A payload is accepted ONLY if its first 8 bytes are the
    CreateEvent discriminator.
    """

    marker = "Program data:"

    if marker not in line:
        return None

    encoded = line.split(
        marker,
        1,
    )[1].strip()

    if not encoded:
        return None

    try:
        raw = base64.b64decode(
            encoded,
            validate=True,
        )
    except Exception:
        return None

    return _decode_create_event_payload(raw)


def _parse_create_event_from_logs(
    logs: list[str],
    signature: str,
) -> Optional[ParsedLaunch]:
    """
    Search all Program data lines for an actual CreateEvent.

    We intentionally inspect EVERY Program data line because a transaction
    may emit several events.
    """

    for line in logs:
        parsed = _decode_program_data_line(line)

        if parsed is None:
            continue

        parsed.signature = signature
        parsed.raw_logs = logs

        return parsed

    return None


# ============================================================================
# Text-enriched CreateEvent fallback
# ============================================================================

def _parse_enriched_create_event(
    logs: list[str],
    signature: str,
) -> Optional[ParsedLaunch]:
    """
    Parse provider-enriched CreateEvent logs.

    Example supported shape:

        CreateEvent:
        ... mint=<pubkey> bonding_curve=<pubkey> user=<pubkey>

    This is deliberately a fallback because provider formatting is not
    guaranteed.
    """

    has_create_event = any(
        "CreateEvent" in line
        for line in logs
    )

    if not has_create_event:
        return None

    kv = _extract_kv_pubkeys(logs)

    mint = kv.get("mint")

    if not mint:
        return None

    # Derive the bonding curve instead of trusting a provider-provided
    # value whenever possible.
    try:
        bonding_curve = derive_bonding_curve_pda(mint)
    except Exception:
        bonding_curve = kv.get("bonding_curve")

    if not bonding_curve:
        return None

    return ParsedLaunch(
        mint=mint,
        bonding_curve=bonding_curve,
        user=kv.get("user"),
        signature=signature,
        raw_logs=logs,
        needs_full_fetch=False,
    )


# ============================================================================
# Public CREATE parser
# ============================================================================

def parse_create_from_logs(
    logs: list[str],
    signature: str,
) -> ParsedLaunch:
    """
    Extract a Pump.fun CREATE directly from logsSubscribe.

    Parsing order:

        1. Anchor CreateEvent Program data
        2. Provider-enriched CreateEvent text
        3. getTransaction fallback

    We NEVER interpret an arbitrary Program data payload based solely
    on its length.
    """

    if not logs:
        return ParsedLaunch(
            mint=None,
            bonding_curve=None,
            user=None,
            signature=signature,
            raw_logs=logs,
            needs_full_fetch=True,
        )

    # ------------------------------------------------------------------
    # 1. Real Anchor CreateEvent
    # ------------------------------------------------------------------

    parsed = _parse_create_event_from_logs(
        logs,
        signature,
    )

    if parsed is not None:
        # Bonding curve is deterministic. Prefer our local derivation
        # to eliminate provider/parser inconsistencies.
        try:
            parsed.bonding_curve = derive_bonding_curve_pda(
                parsed.mint
            )
        except Exception:
            pass

        return parsed

    # ------------------------------------------------------------------
    # 2. Provider-enriched text event
    # ------------------------------------------------------------------

    parsed = _parse_enriched_create_event(
        logs,
        signature,
    )

    if parsed is not None:
        return parsed

    # ------------------------------------------------------------------
    # 3. Last resort: getTransaction
    # ------------------------------------------------------------------

    return ParsedLaunch(
        mint=None,
        bonding_curve=None,
        user=None,
        signature=signature,
        raw_logs=logs,
        needs_full_fetch=True,
    )


# ============================================================================
# CREATE detection
# ============================================================================

def is_create_log(
    logs: list[str],
) -> bool:
    """
    Fast pre-filter for CREATE transactions.

    This function intentionally remains cheap because it runs for every
    Pump.fun logsNotification.

    It recognizes:

        Instruction: Create
        Instruction: CreateV2
        CreateEvent

    It does NOT claim that the transaction is a valid CREATE.

    `parse_create_from_logs()` remains the authoritative parser.
    """

    for line in logs:
        if "Instruction: Create" in line:
            return True

        if "Instruction: CreateV2" in line:
            return True

        if "CreateEvent" in line:
            return True

    return False


# ============================================================================
# Instruction-data verification helpers
# ============================================================================

def is_buy_instruction_data(
    data_bytes: bytes,
) -> bool:
    return (
        len(data_bytes) >= 8
        and data_bytes[:8] == BUY_DISCRIMINATOR
    )


def is_sell_instruction_data(
    data_bytes: bytes,
) -> bool:
    return (
        len(data_bytes) >= 8
        and data_bytes[:8] == SELL_DISCRIMINATOR
    )


def is_create_instruction_data(
    data_bytes: bytes,
) -> bool:
    return (
        len(data_bytes) >= 8
        and data_bytes[:8] == CREATE_DISCRIMINATOR
    )


def is_create_event_data(
    data_bytes: bytes,
) -> bool:
    return (
        len(data_bytes) >= 8
        and data_bytes[:8] == CREATE_EVENT_DISCRIMINATOR
    )