#!/usr/bin/env python3
import argparse
import base64
import json
import math
import os
import sqlite3
import statistics
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


METEORA_API_BASE = "https://dlmm.datapi.meteora.ag"
SOLANA_MAINNET_RPC_DEFAULT = "https://api.mainnet-beta.solana.com"
DLMM_PROGRAM_ID = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"

ORCA_WHIRLPOOL_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
ORCA_SOL_USDC_POOL = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE"
ORCA_SOL_MINT = "So11111111111111111111111111111111111111112"
ORCA_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

DEFI_LLAMA_ORCA_SOL_USDC_CHART = "a5c85bc8-eb41-45c0-a520-d18d7529c0d8"
DEFI_LLAMA_YIELDS_CHART_URL = f"https://yields.llama.fi/chart/{DEFI_LLAMA_ORCA_SOL_USDC_CHART}"

# Anchor discriminator for account "LbPair" from Meteora DLMM IDL.
LBPAIR_DISCRIMINATOR = bytes([33, 11, 49, 98, 181, 101, 177, 13])
BINARRAY_DISCRIMINATOR = bytes([92, 142, 92, 220, 5, 148, 70, 181])

# Orca Whirlpools discriminators (Codama-generated TS SDK).
WHIRLPOOL_DISCRIMINATOR = bytes([63, 149, 209, 12, 225, 128, 99, 9])
FIXED_TICK_ARRAY_DISCRIMINATOR = bytes([69, 97, 189, 190, 110, 7, 66, 187])

FEE_DENOMINATOR = 1_000_000_000
MAX_FEE_RATE_1E9 = 100_000_000  # 10% in 1e9 precision

TICKS_PER_TICK_ARRAY = 88
BINS_PER_BIN_ARRAY = 70

# Decimal precision for liquidity math (Orca).
getcontext().prec = 50


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def utc_today_date_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def sleep_with_jitter(seconds: float) -> None:
    # Small deterministic jitter to avoid hammering with identical cadence.
    jitter = (time.time_ns() % 10_000_000) / 10_000_000 * 0.05
    time.sleep(max(0.0, seconds + jitter))


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_s: float = 20.0,
    retries: int = 5,
    backoff_s: float = 0.7,
    sleep_s: float = 0.2,
) -> Any:
    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            sleep_with_jitter(sleep_s)
            resp = session.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=timeout_s,
                headers={"accept": "application/json"},
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            resp.raise_for_status()
            return resp.json()
        except BaseException as exc:
            last_err = exc
            if attempt == retries:
                break
            sleep_with_jitter(backoff_s * (2 ** (attempt - 1)))
    raise RuntimeError(f"request failed after {retries} attempts: {url}") from last_err


def ensure_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS observations (
          pool_address TEXT NOT NULL,
          day TEXT NOT NULL,
          pool_name TEXT,
          token_x_symbol TEXT,
          token_y_symbol TEXT,
          tvl_usd REAL,
          fees_usd REAL,
          apr_simple_pct REAL,
          api_dynamic_fee_pct REAL,
          api_apr_24h_pct REAL,
          api_fee_tvl_ratio REAL,
          rpc_url TEXT,
          rpc_slot INTEGER,
          rpc_owner TEXT,
          rpc_executable INTEGER,
          rpc_rent_epoch TEXT,
          rpc_data_base64 TEXT,
          rpc_data_len INTEGER,
          lbpair_discriminator_ok INTEGER,
          base_factor INTEGER,
          base_fee_power_factor INTEGER,
          variable_fee_control INTEGER,
          bin_step INTEGER,
          volatility_accumulator INTEGER,
          fee_rate_calc_1e9 INTEGER,
          fee_pct_calc REAL,
          error TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (pool_address, day)
        );
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_observations_day ON observations(day);"
    )

    # fees_usd теперь хранит комиссии ЗА ВЫЧЕТОМ доли протокола (то, что реально
    # достаётся поставщику ликвидности). Валовые и долю протокола храним рядом,
    # чтобы расхождение было видно и чтобы старые строки можно было пересчитать.
    existing = {r[1] for r in conn.execute("PRAGMA table_info(observations);")}
    for col in ("fees_gross_usd", "protocol_fees_usd"):
        if col not in existing:
            conn.execute(f"ALTER TABLE observations ADD COLUMN {col} REAL")

    # Lightweight migration for early schema versions (INTEGER -> TEXT for rentEpoch).
    try:
        cols = {r[1]: (r[2] or "").upper() for r in conn.execute("PRAGMA table_info(observations);")}
        if cols.get("rpc_rent_epoch") == "INTEGER":
            with conn:
                conn.execute(
                    """
                    CREATE TABLE observations_new (
                      pool_address TEXT NOT NULL,
                      day TEXT NOT NULL,
                      pool_name TEXT,
                      token_x_symbol TEXT,
                      token_y_symbol TEXT,
                      tvl_usd REAL,
                      fees_usd REAL,
                      apr_simple_pct REAL,
                      api_dynamic_fee_pct REAL,
                      api_apr_24h_pct REAL,
                      api_fee_tvl_ratio REAL,
                      rpc_url TEXT,
                      rpc_slot INTEGER,
                      rpc_owner TEXT,
                      rpc_executable INTEGER,
                      rpc_rent_epoch TEXT,
                      rpc_data_base64 TEXT,
                      rpc_data_len INTEGER,
                      lbpair_discriminator_ok INTEGER,
                      base_factor INTEGER,
                      base_fee_power_factor INTEGER,
                      variable_fee_control INTEGER,
                      bin_step INTEGER,
                      volatility_accumulator INTEGER,
                      fee_rate_calc_1e9 INTEGER,
                      fee_pct_calc REAL,
                      error TEXT,
                      updated_at TEXT NOT NULL,
                      PRIMARY KEY (pool_address, day)
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT INTO observations_new
                    SELECT
                      pool_address, day, pool_name, token_x_symbol, token_y_symbol,
                      tvl_usd, fees_usd, apr_simple_pct, api_dynamic_fee_pct, api_apr_24h_pct, api_fee_tvl_ratio,
                      rpc_url, rpc_slot, rpc_owner, rpc_executable, CAST(rpc_rent_epoch AS TEXT),
                      rpc_data_base64, rpc_data_len, lbpair_discriminator_ok,
                      base_factor, base_fee_power_factor, variable_fee_control, bin_step, volatility_accumulator,
                      fee_rate_calc_1e9, fee_pct_calc, error, updated_at
                    FROM observations;
                    """
                )
                conn.execute("DROP TABLE observations;")
                conn.execute("ALTER TABLE observations_new RENAME TO observations;")
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_observations_day ON observations(day);"
                )
    except Exception:
        # Never block data collection on migration issues.
        pass

    return conn


def upsert_observation(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    cols = sorted(row.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)
    updates = ", ".join([f"{c}=excluded.{c}" for c in cols if c not in ("pool_address", "day")])
    values = [row[c] for c in cols]
    conn.execute(
        f"""
        INSERT INTO observations ({col_list})
        VALUES ({placeholders})
        ON CONFLICT(pool_address, day) DO UPDATE SET {updates};
        """,
        values,
    )


def parse_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def safe_get(d: Dict[str, Any], *path: str) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def is_sol_usdc_pool(pool_obj: Dict[str, Any]) -> bool:
    sx = safe_get(pool_obj, "token_x", "symbol")
    sy = safe_get(pool_obj, "token_y", "symbol")
    if sx and sy:
        return {sx.upper(), sy.upper()} == {"SOL", "USDC"}
    name = pool_obj.get("name") or ""
    return "SOL" in name.upper() and "USDC" in name.upper()


def meteora_list_pools(
    session: requests.Session,
    *,
    query: str,
    page_size: int,
    filter_by: Optional[str],
    sleep_s: float,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"query": query, "page_size": page_size}
    if filter_by:
        params["filter_by"] = filter_by
    data = request_json(
        session,
        "GET",
        f"{METEORA_API_BASE}/pools",
        params=params,
        sleep_s=sleep_s,
    )
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    if isinstance(data, list):
        return data
    raise RuntimeError("unexpected /pools response shape")


def meteora_pool_details(
    session: requests.Session, address: str, *, sleep_s: float
) -> Dict[str, Any]:
    return request_json(
        session,
        "GET",
        f"{METEORA_API_BASE}/pools/{address}",
        sleep_s=sleep_s,
    )


def meteora_pool_history_1d(
    session: requests.Session, address: str, *, limit: int, sleep_s: float
) -> List[Dict[str, Any]]:
    data = request_json(
        session,
        "GET",
        f"{METEORA_API_BASE}/pools/{address}/volume/history",
        params={"interval": "1d", "limit": limit},
        sleep_s=sleep_s,
    )
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return data["data"]
    if isinstance(data, list):
        return data
    raise RuntimeError("unexpected history response shape")


def pick_last_closed_day(history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not history:
        return None
    # API note: last point is current unclosed day with fees=0, should be dropped.
    candidates = history[:]
    if candidates and parse_float(candidates[-1].get("fees")) == 0.0:
        candidates = candidates[:-1]
    # Also defensively drop any trailing zeros.
    while candidates and parse_float(candidates[-1].get("fees")) == 0.0:
        candidates = candidates[:-1]
    if not candidates:
        return None
    return candidates[-1]


def pick_last_closed_days(history: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    if n <= 0:
        return []
    candidates = history[:]
    if candidates and parse_float(candidates[-1].get("fees")) == 0.0:
        candidates = candidates[:-1]
    while candidates and parse_float(candidates[-1].get("fees")) == 0.0:
        candidates = candidates[:-1]
    if not candidates:
        return []
    return candidates[-n:]


def compute_tvl_usd(pool_details: Dict[str, Any]) -> Optional[float]:
    ax = parse_float(pool_details.get("token_x_amount"))
    ay = parse_float(pool_details.get("token_y_amount"))
    px = parse_float(safe_get(pool_details, "token_x", "price"))
    py = parse_float(safe_get(pool_details, "token_y", "price"))
    if None in (ax, ay, px, py):
        return None
    return ax * px + ay * py


@dataclass
class RpcAccountInfo:
    raw_json: Dict[str, Any]
    slot: Optional[int]
    owner: Optional[str]
    executable: Optional[bool]
    rent_epoch: Optional[int]
    data_base64: Optional[str]
    data_len: Optional[int]


def solana_get_account_info(
    session: requests.Session, rpc_url: str, pubkey: str, *, sleep_s: float
) -> RpcAccountInfo:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [pubkey, {"encoding": "base64", "commitment": "confirmed"}],
    }
    raw = request_json(
        session,
        "POST",
        rpc_url,
        json_body=payload,
        sleep_s=sleep_s,
        timeout_s=25.0,
        retries=5,
    )
    value = safe_get(raw, "result", "value") or {}
    data = value.get("data")
    data_base64: Optional[str] = None
    data_len: Optional[int] = None
    if isinstance(data, list) and data and isinstance(data[0], str):
        data_base64 = data[0]
        try:
            data_len = len(base64.b64decode(data_base64))
        except Exception:
            data_len = None
    return RpcAccountInfo(
        raw_json=raw,
        slot=safe_get(raw, "result", "context", "slot"),
        owner=value.get("owner"),
        executable=value.get("executable"),
        rent_epoch=value.get("rentEpoch"),
        data_base64=data_base64,
        data_len=data_len,
    )


def solana_rpc_call(
    session: requests.Session,
    rpc_url: str,
    method: str,
    params: List[Any],
    *,
    sleep_s: float,
    timeout_s: float = 25.0,
    retries: int = 5,
) -> Any:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    return request_json(
        session,
        "POST",
        rpc_url,
        json_body=payload,
        sleep_s=sleep_s,
        timeout_s=timeout_s,
        retries=retries,
    )


def _chunks(xs: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def solana_get_multiple_accounts(
    session: requests.Session,
    rpc_url: str,
    pubkeys: List[str],
    *,
    sleep_s: float,
    chunk_size: int = 100,
) -> Dict[str, Optional[bytes]]:
    out: Dict[str, Optional[bytes]] = {}
    for chunk in _chunks(pubkeys, chunk_size):
        raw = solana_rpc_call(
            session,
            rpc_url,
            "getMultipleAccounts",
            [chunk, {"encoding": "base64", "commitment": "confirmed"}],
            sleep_s=sleep_s,
        )
        values = safe_get(raw, "result", "value") or []
        if not isinstance(values, list) or len(values) != len(chunk):
            raise RuntimeError("unexpected getMultipleAccounts response shape")
        for pk, v in zip(chunk, values):
            if not isinstance(v, dict):
                out[pk] = None
                continue
            data = v.get("data")
            if isinstance(data, list) and data and isinstance(data[0], str):
                try:
                    out[pk] = base64.b64decode(data[0])
                except Exception:
                    out[pk] = None
            else:
                out[pk] = None
    return out


def solana_get_program_accounts(
    session: requests.Session,
    rpc_url: str,
    program_id: str,
    *,
    filters: List[Dict[str, Any]],
    data_slice: Optional[Dict[str, int]],
    sleep_s: float,
    with_context: bool = False,
) -> List[Dict[str, Any]]:
    cfg: Dict[str, Any] = {
        "encoding": "base64",
        "commitment": "confirmed",
        "filters": filters,
    }
    if data_slice is not None:
        cfg["dataSlice"] = data_slice
    if with_context:
        cfg["withContext"] = True
    raw = solana_rpc_call(
        session,
        rpc_url,
        "getProgramAccounts",
        [program_id, cfg],
        sleep_s=sleep_s,
        timeout_s=40.0,
        retries=5,
    )
    res: Any = raw.get("result") if isinstance(raw, dict) else None
    if with_context:
        res = safe_get(raw, "result", "value")
    if not isinstance(res, list):
        raise RuntimeError("unexpected getProgramAccounts response shape")
    return res


_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def base58_encode(b: bytes) -> str:
    if not b:
        return ""
    zeros = 0
    for ch in b:
        if ch == 0:
            zeros += 1
        else:
            break
    n = int.from_bytes(b, "big", signed=False)
    out = bytearray()
    while n > 0:
        n, rem = divmod(n, 58)
        out.append(_B58_ALPHABET[rem])
    out.reverse()
    return ("1" * zeros) + out.decode("ascii")


@dataclass
class LbPairFeeParams:
    discriminator_ok: bool
    base_factor: Optional[int]
    base_fee_power_factor: Optional[int]
    variable_fee_control: Optional[int]
    bin_step: Optional[int]
    volatility_accumulator: Optional[int]
    fee_rate_1e9: Optional[int]
    fee_pct: Optional[float]


def _read_u16_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 2], "little", signed=False)


def _read_u32_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 4], "little", signed=False)


def _read_u8(buf: bytes, off: int) -> int:
    return buf[off]


def _read_i32_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 4], "little", signed=True)


def _read_i64_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 8], "little", signed=True)


def _read_u64_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 8], "little", signed=False)


def _read_u128_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 16], "little", signed=False)


def _read_i128_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off : off + 16], "little", signed=True)


def decode_lbpair_fee_params(account_data: bytes) -> LbPairFeeParams:
    if len(account_data) < 8 + 80:
        return LbPairFeeParams(
            discriminator_ok=False,
            base_factor=None,
            base_fee_power_factor=None,
            variable_fee_control=None,
            bin_step=None,
            volatility_accumulator=None,
            fee_rate_1e9=None,
            fee_pct=None,
        )

    disc = account_data[:8]
    discriminator_ok = disc == LBPAIR_DISCRIMINATOR
    body = account_data[8:]

    try:
        base_factor = _read_u16_le(body, 0)
        variable_fee_control = _read_u32_le(body, 8)
        base_fee_power_factor = _read_u8(body, 26)
        volatility_accumulator = _read_u32_le(body, 32)
        bin_step = _read_u16_le(body, 72)
    except Exception:
        return LbPairFeeParams(
            discriminator_ok=discriminator_ok,
            base_factor=None,
            base_fee_power_factor=None,
            variable_fee_control=None,
            bin_step=None,
            volatility_accumulator=None,
            fee_rate_1e9=None,
            fee_pct=None,
        )

    fee_rate_1e9: Optional[int] = None
    fee_pct: Optional[float] = None

    # Compute base + variable fee as per docs.
    try:
        base_fee_rate = (
            int(base_factor)
            * int(bin_step)
            * 10
            * (10 ** int(base_fee_power_factor))
        )

        vfc = int(variable_fee_control)
        if vfc == 0:
            variable_fee_rate = 0
        else:
            x = int(volatility_accumulator) * int(bin_step)
            numerator = vfc * (x * x)
            variable_fee_rate = int(math.ceil(numerator / 100_000_000_000))

        total = base_fee_rate + variable_fee_rate
        if total > MAX_FEE_RATE_1E9:
            total = MAX_FEE_RATE_1E9
        fee_rate_1e9 = int(total)
        fee_pct = fee_rate_1e9 / FEE_DENOMINATOR * 100.0
    except Exception:
        fee_rate_1e9 = None
        fee_pct = None

    return LbPairFeeParams(
        discriminator_ok=discriminator_ok,
        base_factor=base_factor,
        base_fee_power_factor=base_fee_power_factor,
        variable_fee_control=variable_fee_control,
        bin_step=bin_step,
        volatility_accumulator=volatility_accumulator,
        fee_rate_1e9=fee_rate_1e9,
        fee_pct=fee_pct,
    )


@dataclass
class LbPairState:
    discriminator_ok: bool
    active_id: Optional[int]
    bin_step: Optional[int]


def decode_lbpair_state(account_data: bytes) -> LbPairState:
    if len(account_data) < 8 + 80:
        return LbPairState(discriminator_ok=False, active_id=None, bin_step=None)
    disc = account_data[:8]
    discriminator_ok = disc == LBPAIR_DISCRIMINATOR
    body = account_data[8:]
    try:
        # Offsets validated against Meteora DLMM IDL:
        # StaticParameters (32) + VariableParameters (32) + bump_seed(1) + bin_step_seed(2) + pair_type(1) = 68.
        active_id = _read_i32_le(body, 68)
        bin_step2 = _read_u16_le(body, 72)
        return LbPairState(
            discriminator_ok=discriminator_ok, active_id=active_id, bin_step=bin_step2
        )
    except Exception:
        return LbPairState(discriminator_ok=discriminator_ok, active_id=None, bin_step=None)


@dataclass
class MeteoraBinRow:
    bin_id: int
    amount_x: int
    amount_y: int


def decode_binarray_bins_subset(
    account_data: bytes, *, want_bin_ids: Optional[set]
) -> Tuple[Optional[int], Optional[str], List[MeteoraBinRow]]:
    """
    Decode Meteora BinArray. Returns (bin_array_index, lb_pair_pubkey, bins_subset).
    - If want_bin_ids is None: returns all bins (still as list).
    """
    if len(account_data) < 8 + 48:
        return None, None, []
    if account_data[:8] != BINARRAY_DISCRIMINATOR:
        return None, None, []
    # Layout: disc(8) + index(i64) + version(u8) + pad7 + lb_pair(pubkey) + bins[70]
    idx = _read_i64_le(account_data, 8)
    lb_pair_bytes = account_data[24:56]
    lb_pair = base58_encode(lb_pair_bytes)
    bins_off = 56
    bin_size = 144
    out: List[MeteoraBinRow] = []
    for i in range(BINS_PER_BIN_ARRAY):
        bin_id = idx * BINS_PER_BIN_ARRAY + i
        if want_bin_ids is not None and bin_id not in want_bin_ids:
            continue
        off = bins_off + i * bin_size
        if off + 16 > len(account_data):
            break
        ax = _read_u64_le(account_data, off + 0)
        ay = _read_u64_le(account_data, off + 8)
        if ax == 0 and ay == 0:
            continue
        out.append(MeteoraBinRow(bin_id=bin_id, amount_x=ax, amount_y=ay))
    return idx, lb_pair, out


@dataclass
class OrcaWhirlpoolState:
    discriminator_ok: bool
    tick_spacing: Optional[int]
    liquidity: Optional[int]
    sqrt_price_x64: Optional[int]
    tick_current_index: Optional[int]
    token_mint_a: Optional[str]
    token_mint_b: Optional[str]
    token_vault_a: Optional[str]
    token_vault_b: Optional[str]
    fee_rate: Optional[int]


def decode_orca_whirlpool(account_data: bytes) -> OrcaWhirlpoolState:
    if len(account_data) < 8 + 120:
        return OrcaWhirlpoolState(
            discriminator_ok=False,
            tick_spacing=None,
            liquidity=None,
            sqrt_price_x64=None,
            tick_current_index=None,
            token_mint_a=None,
            token_mint_b=None,
            token_vault_a=None,
            token_vault_b=None,
            fee_rate=None,
        )
    disc_ok = account_data[:8] == WHIRLPOOL_DISCRIMINATOR
    try:
        off = 8
        off += 32  # whirlpoolsConfig
        off += 1  # bump
        tick_spacing = _read_u16_le(account_data, off)
        off += 2
        off += 2  # feeTierIndexSeed
        fee_rate = _read_u16_le(account_data, off)
        off += 2
        off += 2  # protocolFeeRate
        liquidity = _read_u128_le(account_data, off)
        off += 16
        sqrt_price_x64 = _read_u128_le(account_data, off)
        off += 16
        tick_current_index = _read_i32_le(account_data, off)
        off += 4
        off += 8 + 8  # protocolFeeOwedA/B
        token_mint_a = base58_encode(account_data[off : off + 32])
        off += 32
        token_vault_a = base58_encode(account_data[off : off + 32])
        off += 32
        off += 16  # feeGrowthGlobalA
        token_mint_b = base58_encode(account_data[off : off + 32])
        off += 32
        token_vault_b = base58_encode(account_data[off : off + 32])
        return OrcaWhirlpoolState(
            discriminator_ok=disc_ok,
            tick_spacing=tick_spacing,
            liquidity=liquidity,
            sqrt_price_x64=sqrt_price_x64,
            tick_current_index=tick_current_index,
            token_mint_a=token_mint_a,
            token_mint_b=token_mint_b,
            token_vault_a=token_vault_a,
            token_vault_b=token_vault_b,
            fee_rate=fee_rate,
        )
    except Exception:
        return OrcaWhirlpoolState(
            discriminator_ok=disc_ok,
            tick_spacing=None,
            liquidity=None,
            sqrt_price_x64=None,
            tick_current_index=None,
            token_mint_a=None,
            token_mint_b=None,
            token_vault_a=None,
            token_vault_b=None,
            fee_rate=None,
        )


def decode_spl_mint_decimals(account_data: bytes) -> Optional[int]:
    # SPL Mint layout: decimals is u8 at offset 44.
    if len(account_data) < 45:
        return None
    return _read_u8(account_data, 44)


def decode_spl_token_account_amount(account_data: bytes) -> Optional[int]:
    # SPL Token Account layout: amount u64 at offset 64.
    if len(account_data) < 72:
        return None
    return _read_u64_le(account_data, 64)


@dataclass
class OrcaTick:
    tick_index: int
    initialized: bool
    liquidity_net: int
    liquidity_gross: int


def decode_orca_fixed_tick_array(
    account_data: bytes, *, tick_spacing: int
) -> Tuple[Optional[int], List[OrcaTick]]:
    if len(account_data) < 8 + 4 + 32:
        return None, []
    if account_data[:8] != FIXED_TICK_ARRAY_DISCRIMINATOR:
        return None, []
    start_tick_index = _read_i32_le(account_data, 8)
    ticks_off = 12
    tick_size = 113
    out: List[OrcaTick] = []
    for i in range(TICKS_PER_TICK_ARRAY):
        off = ticks_off + i * tick_size
        if off + tick_size > len(account_data):
            break
        initialized = account_data[off] != 0
        liq_net = _read_i128_le(account_data, off + 1)
        liq_gross = _read_u128_le(account_data, off + 17)
        if not initialized and liq_net == 0 and liq_gross == 0:
            continue
        tick_index = start_tick_index + i * tick_spacing
        out.append(
            OrcaTick(
                tick_index=tick_index,
                initialized=initialized,
                liquidity_net=liq_net,
                liquidity_gross=liq_gross,
            )
        )
    return start_tick_index, out


def _ceil_div(a: int, b: int) -> int:
    return -(-a // b)


def _align_tick_floor(tick: int, spacing: int) -> int:
    return (tick // spacing) * spacing


def _align_tick_ceil(tick: int, spacing: int) -> int:
    return _ceil_div(tick, spacing) * spacing


def _orca_tick_array_start_index(tick_index: int, tick_spacing: int) -> int:
    span = tick_spacing * TICKS_PER_TICK_ARRAY
    return (tick_index // span) * span


def _meteora_bin_window(active_id: int, bin_step: int, pct: float) -> Tuple[int, int]:
    if bin_step <= 0:
        return active_id, active_id
    r = 1.0 + (bin_step / 10_000.0)
    if r <= 1.0:
        return active_id, active_id
    lo = math.log(1.0 - pct) / math.log(r)
    hi = math.log(1.0 + pct) / math.log(r)
    dmin = int(math.ceil(lo))
    dmax = int(math.floor(hi))
    return active_id + dmin, active_id + dmax


def _orca_tick_window(
    tick_current_index: int, tick_spacing: int, pct: float
) -> Tuple[int, int]:
    ln_base = math.log(1.0001)
    lo = math.log(1.0 - pct) / ln_base
    hi = math.log(1.0 + pct) / ln_base
    tmin = int(math.floor(tick_current_index + lo))
    tmax = int(math.ceil(tick_current_index + hi))
    return _align_tick_floor(tmin, tick_spacing), _align_tick_ceil(tmax, tick_spacing)


def _sqrt_price_from_tick(tick_index: int) -> Decimal:
    x = math.exp(math.log(1.0001) * (tick_index / 2.0))
    return Decimal(str(x))


def _sqrt_price_x64_to_decimal(sqrt_price_x64: int) -> Decimal:
    return Decimal(sqrt_price_x64) / Decimal(2**64)


def _orca_price_ui_from_sqrt_price(
    sqrt_price_x64: int, decimals_a: int, decimals_b: int
) -> float:
    sp = float(Decimal(sqrt_price_x64) / Decimal(2**64))
    price_raw = sp * sp
    return float(price_raw * (10 ** (decimals_a - decimals_b)))


def _amounts_for_liquidity_interval(
    liquidity: int, sqrt_pa: Decimal, sqrt_pb: Decimal, sqrt_p: Decimal
) -> Tuple[Decimal, Decimal]:
    if liquidity <= 0:
        return Decimal(0), Decimal(0)
    L = Decimal(liquidity)
    if sqrt_p <= sqrt_pa:
        amt_a = L * (sqrt_pb - sqrt_pa) / (sqrt_pa * sqrt_pb)
        return amt_a, Decimal(0)
    if sqrt_p >= sqrt_pb:
        amt_b = L * (sqrt_pb - sqrt_pa)
        return Decimal(0), amt_b
    amt_a = L * (sqrt_pb - sqrt_p) / (sqrt_p * sqrt_pb)
    amt_b = L * (sqrt_p - sqrt_pa)
    return amt_a, amt_b


def _safe_token_decimals_from_api(token_obj: Any) -> Optional[int]:
    if not isinstance(token_obj, dict):
        return None
    d = token_obj.get("decimals")
    try:
        if d is None:
            return None
        return int(d)
    except Exception:
        return None


def liquidity(
    *,
    db_path: str,
    query: str,
    tvl_min_usd: float,
    page_size: int,
    sleep_s: float,
    rpc_url: str,
    orca_pool: str,
    thresholds_pct: List[float],
    raw_bins_each_side: int,
) -> int:
    _ = ensure_db(db_path)  # keep consistent environment; do not write.
    session = requests.Session()

    pools = meteora_list_pools(
        session,
        query=query,
        page_size=page_size,
        filter_by=None,
        sleep_s=sleep_s,
    )
    sol_usdc = [p for p in pools if is_sol_usdc_pool(p)]
    if not sol_usdc:
        eprint("No SOL-USDC pools found in Meteora /pools response.")
        return 2

    def tvl_of(p: Dict[str, Any]) -> float:
        return float(p.get("tvl") or 0.0)

    top_pool = max(sol_usdc, key=tvl_of)
    selected = [p for p in sol_usdc if tvl_of(p) >= tvl_min_usd]
    if all((p.get("address") != top_pool.get("address")) for p in selected):
        selected.append(top_pool)
    selected.sort(key=tvl_of, reverse=True)

    must_include = "5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6"
    if all((p.get("address") != must_include) for p in selected):
        for p in sol_usdc:
            if p.get("address") == must_include:
                selected.append(p)
                break
        else:
            selected.append({"address": must_include, "name": "SOL-USDC (requested)"})

    # -------- Orca on-chain state --------
    orca_info = solana_get_account_info(session, rpc_url, orca_pool, sleep_s=sleep_s)
    if not orca_info.data_base64:
        eprint("Orca: getAccountInfo returned no base64 data for whirlpool.")
        return 2
    whirlpool_bytes = base64.b64decode(orca_info.data_base64)
    whirl = decode_orca_whirlpool(whirlpool_bytes)
    if (
        (not whirl.discriminator_ok)
        or whirl.tick_spacing is None
        or whirl.sqrt_price_x64 is None
        or whirl.tick_current_index is None
        or whirl.liquidity is None
    ):
        eprint("Orca: failed to decode Whirlpool account (unexpected layout).")
        return 2
    if not whirl.token_mint_a or not whirl.token_mint_b or not whirl.token_vault_a or not whirl.token_vault_b:
        eprint("Orca: missing token mint/vault fields in decoded Whirlpool.")
        return 2

    mint_accounts = solana_get_multiple_accounts(
        session,
        rpc_url,
        [whirl.token_mint_a, whirl.token_mint_b],
        sleep_s=sleep_s,
        chunk_size=100,
    )
    dec_a = decode_spl_mint_decimals(mint_accounts.get(whirl.token_mint_a) or b"")
    dec_b = decode_spl_mint_decimals(mint_accounts.get(whirl.token_mint_b) or b"")
    if dec_a is None or dec_b is None:
        eprint("Orca: failed to decode SPL mint decimals.")
        return 2

    vault_accounts = solana_get_multiple_accounts(
        session,
        rpc_url,
        [whirl.token_vault_a, whirl.token_vault_b],
        sleep_s=sleep_s,
        chunk_size=100,
    )
    vault_a_amt = decode_spl_token_account_amount(
        vault_accounts.get(whirl.token_vault_a) or b""
    )
    vault_b_amt = decode_spl_token_account_amount(
        vault_accounts.get(whirl.token_vault_b) or b""
    )
    if vault_a_amt is None or vault_b_amt is None:
        eprint("Orca: failed to decode SPL token vault balances.")
        return 2

    price_ui_b_per_a = _orca_price_ui_from_sqrt_price(
        int(whirl.sqrt_price_x64), dec_a, dec_b
    )

    def orca_tvl_usd_and_price_sol() -> Tuple[float, float]:
        mint_a = whirl.token_mint_a or ""
        mint_b = whirl.token_mint_b or ""
        amt_a_ui = vault_a_amt / (10**dec_a)
        amt_b_ui = vault_b_amt / (10**dec_b)
        if mint_a == ORCA_SOL_MINT and mint_b == ORCA_USDC_MINT:
            sol_price = price_ui_b_per_a
            return float(amt_a_ui * sol_price + amt_b_ui), float(sol_price)
        if mint_a == ORCA_USDC_MINT and mint_b == ORCA_SOL_MINT:
            sol_price = 1.0 / price_ui_b_per_a if price_ui_b_per_a else float("nan")
            return float(amt_b_ui * sol_price + amt_a_ui), float(sol_price)
        tvl = amt_a_ui * price_ui_b_per_a + amt_b_ui
        return float(tvl), float("nan")

    orca_tvl, orca_sol_price = orca_tvl_usd_and_price_sol()

    print("Liquidity density snapshot (read-only).")
    print(f"RPC: {rpc_url}")
    print("")
    print("Chosen data sources:")
    print("- Meteora bin distribution: on-chain `BinArray` accounts (DLMM program) + TVL/USD prices from Meteora Data API `/pools/{address}`.")
    print("- Orca tick distribution: on-chain `Whirlpool` + `TickArray` accounts + on-chain vault balances for TVL (priced by on-chain pool price).")
    print("")

    # -------- Orca: list tick arrays (light) then fetch only needed --------
    tick_spacing = int(whirl.tick_spacing)
    tick_current = int(whirl.tick_current_index)
    active_liq = int(whirl.liquidity)
    sqrt_p = _sqrt_price_x64_to_decimal(int(whirl.sqrt_price_x64))

    max_pct = max(thresholds_pct) if thresholds_pct else 3.0
    orca_tmin, orca_tmax = _orca_tick_window(tick_current, tick_spacing, max_pct / 100.0)
    need_start_min = _orca_tick_array_start_index(orca_tmin, tick_spacing)
    need_start_max = _orca_tick_array_start_index(orca_tmax, tick_spacing)

    tick_arrays_light = solana_get_program_accounts(
        session,
        rpc_url,
        ORCA_WHIRLPOOL_PROGRAM_ID,
        filters=[
            {"dataSize": 9988},
            {"memcmp": {"offset": 9956, "bytes": orca_pool}},
        ],
        data_slice={"offset": 8, "length": 4},
        sleep_s=sleep_s,
    )
    start_to_pubkey: Dict[int, str] = {}
    for it in tick_arrays_light:
        pk = it.get("pubkey")
        acc = it.get("account") or {}
        data = acc.get("data")
        if not isinstance(pk, str):
            continue
        if not (isinstance(data, list) and data and isinstance(data[0], str)):
            continue
        try:
            b = base64.b64decode(data[0])
            if len(b) != 4:
                continue
            start = int.from_bytes(b, "little", signed=True)
            start_to_pubkey[start] = pk
        except Exception:
            continue

    required_starts: List[int] = []
    s = need_start_min
    span = tick_spacing * TICKS_PER_TICK_ARRAY
    while s <= need_start_max:
        required_starts.append(s)
        s += span

    required_tick_array_pubkeys = [
        start_to_pubkey[s] for s in required_starts if s in start_to_pubkey
    ]
    tick_array_bytes = solana_get_multiple_accounts(
        session,
        rpc_url,
        required_tick_array_pubkeys,
        sleep_s=sleep_s,
        chunk_size=50,
    )
    liq_net_by_tick: Dict[int, int] = {}
    initialized_ticks_sample: List[OrcaTick] = []
    for _pk, data_bytes in tick_array_bytes.items():
        if not data_bytes:
            continue
        _start, ticks = decode_orca_fixed_tick_array(
            data_bytes, tick_spacing=tick_spacing
        )
        for t in ticks:
            liq_net_by_tick[t.tick_index] = int(t.liquidity_net)
            if len(initialized_ticks_sample) < 30 and t.initialized:
                initialized_ticks_sample.append(t)

    def orca_window_value_usd(pct: float) -> Tuple[float, int, int]:
        tmin, tmax = _orca_tick_window(tick_current, tick_spacing, pct)
        cur_lower = _align_tick_floor(tick_current, tick_spacing)

        L_by_interval: Dict[int, int] = {}
        if cur_lower >= tmin and cur_lower < tmax:
            L_by_interval[cur_lower] = active_liq

        # Upward
        L = active_liq
        t = cur_lower
        while t + tick_spacing <= tmax:
            boundary = t + tick_spacing
            L = L + int(liq_net_by_tick.get(boundary, 0))
            if boundary >= tmin and boundary < tmax:
                L_by_interval[boundary] = int(L)
            t += tick_spacing

        # Downward
        L = active_liq
        t = cur_lower
        while t > tmin:
            boundary = t
            L = L - int(liq_net_by_tick.get(boundary, 0))
            lower = t - tick_spacing
            if lower >= tmin and lower < tmax:
                L_by_interval[lower] = int(L)
            t -= tick_spacing

        amt_a = Decimal(0)
        amt_b = Decimal(0)
        tt = tmin
        while tt < tmax:
            Lint = int(L_by_interval.get(tt, 0))
            if Lint != 0:
                sqrt_pa = _sqrt_price_from_tick(tt)
                sqrt_pb = _sqrt_price_from_tick(tt + tick_spacing)
                a, b = _amounts_for_liquidity_interval(Lint, sqrt_pa, sqrt_pb, sqrt_p)
                amt_a += a
                amt_b += b
            tt += tick_spacing

        a_ui = float(amt_a / Decimal(10**dec_a))
        b_ui = float(amt_b / Decimal(10**dec_b))
        mint_a = whirl.token_mint_a or ""
        mint_b = whirl.token_mint_b or ""
        if mint_a == ORCA_SOL_MINT and mint_b == ORCA_USDC_MINT:
            return float(a_ui * orca_sol_price + b_ui), tmin, tmax
        if mint_a == ORCA_USDC_MINT and mint_b == ORCA_SOL_MINT:
            return float(b_ui * orca_sol_price + a_ui), tmin, tmax
        return float(a_ui * price_ui_b_per_a + b_ui), tmin, tmax

    orca_shares: Dict[float, float] = {}
    for pct in thresholds_pct:
        val, _tmin, _tmax = orca_window_value_usd(pct / 100.0)
        share = (
            val / orca_tvl
            if (orca_tvl and orca_tvl > 0 and math.isfinite(val))
            else float("nan")
        )
        orca_shares[pct] = share

    def meteora_pool_density(addr: str) -> Tuple[Optional[float], Dict[float, float], Dict[str, Any]]:
        details = meteora_pool_details(session, addr, sleep_s=sleep_s)
        tvl = compute_tvl_usd(details)
        if tvl is None or tvl <= 0:
            return None, {}, {"error": "tvl missing/invalid from Meteora pool details"}

        token_x = details.get("token_x") if isinstance(details, dict) else None
        token_y = details.get("token_y") if isinstance(details, dict) else None
        px = parse_float(safe_get(details, "token_x", "price"))
        py = parse_float(safe_get(details, "token_y", "price"))
        dec_x = _safe_token_decimals_from_api(token_x)
        dec_y = _safe_token_decimals_from_api(token_y)
        mx = safe_get(details, "token_x", "mint")
        my = safe_get(details, "token_y", "mint")

        rpc_info = solana_get_account_info(session, rpc_url, addr, sleep_s=sleep_s)
        if not rpc_info.data_base64:
            return float(tvl), {}, {"error": "no base64 data from getAccountInfo(lb_pair)"}
        lb_bytes = base64.b64decode(rpc_info.data_base64)
        st = decode_lbpair_state(lb_bytes)
        if st.active_id is None or st.bin_step is None:
            return float(tvl), {}, {"error": "failed to decode active_id/bin_step from LbPair"}
        active_id = int(st.active_id)
        bin_step = int(st.bin_step)

        if dec_x is None or dec_y is None:
            mints_to_fetch: List[str] = []
            if isinstance(mx, str):
                mints_to_fetch.append(mx)
            if isinstance(my, str):
                mints_to_fetch.append(my)
            if mints_to_fetch:
                mint_map = solana_get_multiple_accounts(
                    session, rpc_url, mints_to_fetch, sleep_s=sleep_s, chunk_size=100
                )
                if dec_x is None and isinstance(mx, str):
                    dec_x = decode_spl_mint_decimals(mint_map.get(mx) or b"")
                if dec_y is None and isinstance(my, str):
                    dec_y = decode_spl_mint_decimals(mint_map.get(my) or b"")

        if None in (px, py, dec_x, dec_y):
            return float(tvl), {}, {
                "error": "missing token prices/decimals needed to value bin liquidity",
                "active_id": active_id,
                "bin_step": bin_step,
            }

        want_bin_ids: set = set()
        for pct in thresholds_pct:
            bmin, bmax = _meteora_bin_window(active_id, bin_step, pct / 100.0)
            for b in range(bmin, bmax + 1):
                want_bin_ids.add(b)

        arr_idx_min = min(want_bin_ids) // BINS_PER_BIN_ARRAY
        arr_idx_max = max(want_bin_ids) // BINS_PER_BIN_ARRAY
        want_arr_indexes = set(range(arr_idx_min, arr_idx_max + 1))

        binarrays_light = solana_get_program_accounts(
            session,
            rpc_url,
            DLMM_PROGRAM_ID,
            filters=[
                {"dataSize": 10136},
                {"memcmp": {"offset": 24, "bytes": addr}},
            ],
            data_slice={"offset": 8, "length": 16},
            sleep_s=sleep_s,
        )
        if not binarrays_light:
            # Defensive fallback: if account size changes, retry without dataSize filter
            # and rely on discriminator check during full decode.
            binarrays_light = solana_get_program_accounts(
                session,
                rpc_url,
                DLMM_PROGRAM_ID,
                filters=[
                    {"memcmp": {"offset": 24, "bytes": addr}},
                ],
                data_slice={"offset": 8, "length": 16},
                sleep_s=sleep_s,
            )
        idx_to_pubkey: Dict[int, str] = {}
        for it in binarrays_light:
            pk = it.get("pubkey")
            acc = it.get("account") or {}
            data = acc.get("data")
            if not isinstance(pk, str):
                continue
            if not (isinstance(data, list) and data and isinstance(data[0], str)):
                continue
            try:
                b = base64.b64decode(data[0])
                if len(b) < 8:
                    continue
                idx = int.from_bytes(b[:8], "little", signed=True)
                if idx in want_arr_indexes:
                    idx_to_pubkey[idx] = pk
            except Exception:
                continue

        need_pubkeys = [idx_to_pubkey[i] for i in sorted(want_arr_indexes) if i in idx_to_pubkey]
        full_map = solana_get_multiple_accounts(
            session, rpc_url, need_pubkeys, sleep_s=sleep_s, chunk_size=30
        )

        bin_amount_x: Dict[int, int] = {}
        bin_amount_y: Dict[int, int] = {}
        raw_bins_near_active: List[Dict[str, Any]] = []
        for _pk, b in full_map.items():
            if not b:
                continue
            _idx, _lb_pair, rows = decode_binarray_bins_subset(b, want_bin_ids=want_bin_ids)
            for r in rows:
                bin_amount_x[r.bin_id] = bin_amount_x.get(r.bin_id, 0) + int(r.amount_x)
                bin_amount_y[r.bin_id] = bin_amount_y.get(r.bin_id, 0) + int(r.amount_y)
                if abs(r.bin_id - active_id) <= raw_bins_each_side and len(raw_bins_near_active) < (2 * raw_bins_each_side + 1):
                    raw_bins_near_active.append(
                        {"bin_id": r.bin_id, "amount_x": r.amount_x, "amount_y": r.amount_y}
                    )

        shares: Dict[float, float] = {}
        for pct in thresholds_pct:
            bmin, bmax = _meteora_bin_window(active_id, bin_step, pct / 100.0)
            val = 0.0
            for b in range(bmin, bmax + 1):
                ax = bin_amount_x.get(b, 0)
                ay = bin_amount_y.get(b, 0)
                if ax == 0 and ay == 0:
                    continue
                ax_ui = ax / (10**int(dec_x))
                ay_ui = ay / (10**int(dec_y))
                val += ax_ui * float(px) + ay_ui * float(py)
            shares[pct] = val / float(tvl) if tvl else float("nan")

        extra = {
            "active_id": active_id,
            "bin_step": bin_step,
            "tvl_usd": float(tvl),
            "raw_bins_near_active": sorted(raw_bins_near_active, key=lambda x: x["bin_id"]),
        }
        return float(tvl), shares, extra

    print("Table (share of pool TVL value in window around current price).")
    hdr = (
        f"{'platform':<8}  {'pool':<44}  {'tvl_usd':>12}  "
        + "  ".join([f"±{p:.1f}%".rjust(8) for p in thresholds_pct])
    )
    print(hdr)
    print("-" * len(hdr))
    print(
        f"{'Orca':<8}  {orca_pool:<44}  {orca_tvl:>12.0f}  "
        + "  ".join([f"{(orca_shares.get(p) or float('nan'))*100:>7.2f}%" for p in thresholds_pct])
    )

    meteora_rows: List[Tuple[str, float, Dict[float, float], Dict[str, Any]]] = []
    for p in selected:
        addr = p.get("address") or p.get("lb_pair") or p.get("public_key")
        if not addr:
            continue
        tvl, shares, extra = meteora_pool_density(addr)
        if tvl is None:
            continue
        meteora_rows.append((addr, float(tvl), shares, extra))
        print(
            f"{'Meteora':<8}  {addr:<44}  {tvl:>12.0f}  "
            + "  ".join([f"{(shares.get(pp) or float('nan'))*100:>7.2f}%" for pp in thresholds_pct])
        )

    print("")
    print("Raw fragments (to prove inputs are real, not invented):")
    print("")
    print(f"Orca whirlpool {orca_pool}:")
    print(f"- tick_current_index={tick_current} tick_spacing={tick_spacing} active_liquidity={active_liq}")
    print(f"- vaults: A={whirl.token_vault_a} amount={vault_a_amt} (decimals={dec_a}), B={whirl.token_vault_b} amount={vault_b_amt} (decimals={dec_b})")
    if math.isfinite(orca_sol_price):
        print(f"- price (SOL USD) from sqrtPrice: ${orca_sol_price:.4f}")
    if whirl.fee_rate is not None:
        print(f"- fee_rate (u16): {whirl.fee_rate}")
    if initialized_ticks_sample:
        initialized_ticks_sample.sort(key=lambda t: t.tick_index)
        print("- sample initialized ticks (tick_index, liquidityNet, liquidityGross):")
        for t in initialized_ticks_sample[:12]:
            print(f"  - {t.tick_index:>8}  net={t.liquidity_net}  gross={t.liquidity_gross}")
    print("")

    for (addr, _tvl, _shares, extra) in meteora_rows[:5]:
        print(f"Meteora lb_pair {addr}: active_id={extra.get('active_id')} bin_step={extra.get('bin_step')} (bps)")
        raw_bins = extra.get("raw_bins_near_active") or []
        if raw_bins:
            print(f"- sample bins near active (bin_id, amount_x, amount_y) [base units]:")
            for r in raw_bins[: (2 * raw_bins_each_side + 1)]:
                print(f"  - {r['bin_id']:>8}  x={r['amount_x']}  y={r['amount_y']}")
        print("")

    try:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_path = f"/tmp/meteora-watch-liquidity-{stamp}.json"
        payload = {
            "rpc_url": rpc_url,
            "orca": {
                "pool": orca_pool,
                "tick_current_index": tick_current,
                "tick_spacing": tick_spacing,
                "active_liquidity": active_liq,
                "tvl_usd": orca_tvl,
                "shares": {str(k): v for k, v in orca_shares.items()},
                "vault_a": {"pubkey": whirl.token_vault_a, "amount": vault_a_amt, "decimals": dec_a},
                "vault_b": {"pubkey": whirl.token_vault_b, "amount": vault_b_amt, "decimals": dec_b},
            },
            "meteora": [
                {
                    "pool": a,
                    "tvl_usd": tvl,
                    "active_id": extra.get("active_id"),
                    "bin_step": extra.get("bin_step"),
                    "shares": {str(k): v for k, v in shares.items()},
                    "raw_bins_near_active": extra.get("raw_bins_near_active"),
                }
                for (a, tvl, shares, extra) in meteora_rows
            ],
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"Saved /tmp dump: {out_path}")
    except Exception as exc:
        eprint(f"WARN: failed to write /tmp dump: {exc}")

    if meteora_rows:
        top_m = meteora_rows[0]
        m_sh = top_m[2]
        print("")
        print("Quick check vs observed APR ratio (~1.8x):")
        for pct in thresholds_pct:
            sm = m_sh.get(pct)
            so = orca_shares.get(pct)
            if sm is None or so is None or sm <= 0 or so <= 0:
                continue
            ratio = sm / so
            print(f"- window ±{pct:.1f}%: Meteora_share/Orca_share = {ratio:.2f}x")
        print("Interpretation: if fee-per-working-dollar is similar, reported APR (fees / total TVL) should scale roughly with the working-share.")

    return 0


def collect(
    *,
    db_path: str,
    query: str,
    tvl_min_usd: float,
    page_size: int,
    sleep_s: float,
    history_limit: int,
    rpc_url: str,
    print_rpc_for_pool: Optional[str],
    backfill_days: int,
) -> int:
    conn = ensure_db(db_path)
    session = requests.Session()

    pools = meteora_list_pools(
        session,
        query=query,
        page_size=page_size,
        filter_by=None,  # filter locally to keep behavior stable
        sleep_s=sleep_s,
    )

    sol_usdc = [p for p in pools if is_sol_usdc_pool(p)]
    if not sol_usdc:
        eprint("No SOL-USDC pools found in /pools response.")
        return 2

    def tvl_of(p: Dict[str, Any]) -> float:
        return float(p.get("tvl") or 0.0)

    top_pool = max(sol_usdc, key=tvl_of)
    selected = []
    for p in sol_usdc:
        if tvl_of(p) >= tvl_min_usd:
            selected.append(p)
    # Ensure top TVL pool is always included.
    if all((p.get("address") != top_pool.get("address")) for p in selected):
        selected.append(top_pool)
    # Stable ordering by TVL desc.
    selected.sort(key=tvl_of, reverse=True)

    print(f"Found SOL-USDC pools: {len(sol_usdc)}; tracking: {len(selected)} (tvl>={tvl_min_usd:,.0f} + top TVL).")
    print(f"DB: {db_path}")

    ok = 0
    skipped = 0

    for p in selected:
        addr = p.get("address") or p.get("lb_pair") or p.get("public_key")
        if not addr:
            skipped += 1
            eprint("WARN: pool without address field, skipping.")
            continue

        name = p.get("name")
        token_x_symbol = safe_get(p, "token_x", "symbol")
        token_y_symbol = safe_get(p, "token_y", "symbol")

        try:
            details = meteora_pool_details(session, addr, sleep_s=sleep_s)
            history = meteora_pool_history_1d(
                session, addr, limit=history_limit, sleep_s=sleep_s
            )
            closed_points = pick_last_closed_days(history, backfill_days or 1)
            if not closed_points:
                raise RuntimeError("no closed-day point in history")

            tvl = compute_tvl_usd(details)
            if tvl is None or tvl <= 0:
                raise RuntimeError("tvl missing/invalid from pool details")

            api_dynamic_fee_pct = parse_float(details.get("dynamic_fee_pct"))
            api_apr_24h_pct = parse_float(details.get("apr"))
            api_fee_tvl_ratio = parse_float(details.get("fee_tvl_ratio"))

            rpc_info = solana_get_account_info(
                session, rpc_url, addr, sleep_s=sleep_s
            )
            fee_params: Optional[LbPairFeeParams] = None
            if rpc_info.data_base64:
                try:
                    account_bytes = base64.b64decode(rpc_info.data_base64)
                    fee_params = decode_lbpair_fee_params(account_bytes)
                except Exception:
                    fee_params = None

            if print_rpc_for_pool and addr == print_rpc_for_pool:
                print("\n=== RAW RPC getAccountInfo response (truncated to 4000 chars) ===")
                raw_txt = json.dumps(rpc_info.raw_json, ensure_ascii=False)
                print(raw_txt[:4000] + ("..." if len(raw_txt) > 4000 else ""))
                print("=== END RAW RPC ===\n")

            for pt in closed_points:
                day = str(pt.get("timestamp_str") or "").split("T")[0]
                if not day or len(day) != 10:
                    raise RuntimeError(f"unexpected timestamp_str: {pt.get('timestamp_str')!r}")

                fees = parse_float(pt.get("fees"))
                if fees is None:
                    raise RuntimeError("fees missing in history point")

                # Поле `fees` — ВАЛОВЫЕ комиссии, взятые с трейдеров; часть их забирает
                # протокол и до поставщика ликвидности она не доходит. Проверено на
                # 9 днях подряд: protocol_fees / fees держится ровно на 11.1-11.2%,
                # такая устойчивость бывает только когда одно является частью другого.
                # Без вычета доходность завышается примерно на 11% — а именно на этих
                # числах принимается решение о переезде позиции, так что ошибка не
                # безобидная.
                protocol_fees = parse_float(pt.get("protocol_fees")) or 0.0
                fees_lp = fees - protocol_fees
                if fees_lp < 0:
                    # Не должно случаться; если случилось — данные странные, не гадаем.
                    log.warning(
                        "protocol_fees (%.2f) больше fees (%.2f) за %s — беру валовые",
                        protocol_fees, fees, day,
                    )
                    fees_lp = fees

                apr_simple_pct = fees_lp / tvl * 365.0 * 100.0
                updated_at = datetime.now(timezone.utc).isoformat()

                row: Dict[str, Any] = {
                    "pool_address": addr,
                    "day": day,
                    "pool_name": details.get("name") or name,
                    "token_x_symbol": safe_get(details, "token_x", "symbol") or token_x_symbol,
                    "token_y_symbol": safe_get(details, "token_y", "symbol") or token_y_symbol,
                    "tvl_usd": float(tvl),
                    "fees_usd": float(fees_lp),
                    "fees_gross_usd": float(fees),
                    "protocol_fees_usd": float(protocol_fees),
                    "apr_simple_pct": float(apr_simple_pct),
                    "api_dynamic_fee_pct": api_dynamic_fee_pct,
                    "api_apr_24h_pct": api_apr_24h_pct,
                    "api_fee_tvl_ratio": api_fee_tvl_ratio,
                    "rpc_url": rpc_url,
                    "rpc_slot": rpc_info.slot,
                    "rpc_owner": rpc_info.owner,
                    "rpc_executable": 1 if rpc_info.executable else 0 if rpc_info.executable is not None else None,
                    "rpc_rent_epoch": str(rpc_info.rent_epoch) if rpc_info.rent_epoch is not None else None,
                    "rpc_data_base64": rpc_info.data_base64,
                    "rpc_data_len": rpc_info.data_len,
                    "lbpair_discriminator_ok": 1 if (fee_params and fee_params.discriminator_ok) else 0 if fee_params else None,
                    "base_factor": fee_params.base_factor if fee_params else None,
                    "base_fee_power_factor": fee_params.base_fee_power_factor if fee_params else None,
                    "variable_fee_control": fee_params.variable_fee_control if fee_params else None,
                    "bin_step": fee_params.bin_step if fee_params else None,
                    "volatility_accumulator": fee_params.volatility_accumulator if fee_params else None,
                    "fee_rate_calc_1e9": fee_params.fee_rate_1e9 if fee_params else None,
                    "fee_pct_calc": fee_params.fee_pct if fee_params else None,
                    "error": None,
                    "updated_at": updated_at,
                }

                with conn:
                    upsert_observation(conn, row)

                ok += 1

                fee_calc_txt = (
                    f"{row['fee_pct_calc']:.6f}%"
                    if row.get("fee_pct_calc") is not None
                    else "n/a"
                )
                fee_api_txt = (
                    f"{row['api_dynamic_fee_pct']:.6f}%"
                    if row.get("api_dynamic_fee_pct") is not None
                    else "n/a"
                )

                print(
                    f"- {addr} | day={day} | tvl=${tvl:,.0f} | fees=${fees:,.2f} | apr={apr_simple_pct:,.2f}% | fee(calc)={fee_calc_txt} vs api_dynamic_fee_pct={fee_api_txt}"
                )
        except Exception as exc:
            skipped += 1
            eprint(f"WARN: {addr}: {exc}")
            continue

    print(f"Done. Written/updated rows: {ok}. Skipped pools: {skipped}.")
    return 0 if ok > 0 else 1


def _median_or_none(xs: List[float]) -> Optional[float]:
    xs2 = [x for x in xs if x is not None and not math.isnan(x)]
    if not xs2:
        return None
    return float(statistics.median(xs2))


def _fmt(v: Any, *, digits: int = 2) -> str:
    if v is None:
        return "n/a"
    try:
        if isinstance(v, (int,)):
            return str(v)
        fv = float(v)
        return f"{fv:.{digits}f}"
    except Exception:
        return "n/a"


def fetch_orca_apy_base_by_day(session: requests.Session, *, sleep_s: float) -> Dict[str, float]:
    data = request_json(session, "GET", DEFI_LLAMA_YIELDS_CHART_URL, sleep_s=sleep_s, retries=5)
    # Expected shape: { "data": [ { "timestamp": 123, "apyBase": 1.23, ...}, ... ] }
    rows = data.get("data") if isinstance(data, dict) else None
    out: Dict[str, float] = {}
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, dict):
            continue
        ts = r.get("timestamp")
        apy_base = r.get("apyBase")
        if ts is None or apy_base is None:
            continue
        try:
            if isinstance(ts, (int, float)):
                dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            elif isinstance(ts, str):
                # DeFiLlama often returns ISO timestamps like "2026-07-28T18:01:37.535Z".
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            else:
                continue
            day = dt.date().isoformat()
            out[day] = float(apy_base)
        except Exception:
            continue
    return out


def report(
    *,
    db_path: str,
    pool: str,
    limit_days: int,
    sleep_s: float,
) -> int:
    conn = ensure_db(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT day, tvl_usd, fees_usd, apr_simple_pct, api_dynamic_fee_pct, fee_pct_calc
        FROM observations
        WHERE pool_address = ?
        ORDER BY day DESC
        LIMIT ?;
        """,
        (pool, limit_days),
    )
    rows = cur.fetchall()
    if not rows:
        eprint(f"No data for pool {pool} in {db_path}. Run collect first.")
        return 2

    rows_rev = list(reversed(rows))
    aprs = [r[3] for r in rows_rev if r[3] is not None]
    med = _median_or_none([float(x) for x in aprs]) if aprs else None

    session = requests.Session()
    orca_map = fetch_orca_apy_base_by_day(session, sleep_s=sleep_s)

    overlap_days: List[str] = []
    orca_vals: List[float] = []
    for (day, _tvl, _fees, _apr, _api_fee, _calc_fee) in rows_rev:
        if day in orca_map:
            overlap_days.append(day)
            orca_vals.append(orca_map[day])
    orca_med = _median_or_none(orca_vals) if orca_vals else None

    print(f"Report for pool: {pool}")
    print(f"DB: {db_path}")
    print("")
    print(f"{'day':<10}  {'tvl_usd':>12}  {'fees_usd':>10}  {'apr_simple%':>11}  {'fee_api%':>9}  {'fee_calc%':>9}  {'orca_apyBase%':>13}")
    print("-" * 96)
    for (day, tvl, fees, apr, api_fee, calc_fee) in rows_rev:
        orca = orca_map.get(day)
        print(
            f"{day:<10}  {(_fmt(tvl, digits=0) if tvl is not None else 'n/a'):>12}  {(_fmt(fees, digits=2) if fees is not None else 'n/a'):>10}  {(_fmt(apr, digits=2) if apr is not None else 'n/a'):>11}  {(_fmt(api_fee, digits=6) if api_fee is not None else 'n/a'):>9}  {(_fmt(calc_fee, digits=6) if calc_fee is not None else 'n/a'):>9}  {(_fmt(orca, digits=2) if orca is not None else 'n/a'):>13}"
        )

    print("-" * 96)
    if med is not None:
        print(f"Meteora median apr_simple% over {len(aprs)} days: {med:.2f}%")
    else:
        print("Meteora median: n/a")
    if orca_med is not None:
        print(f"Orca (DeFiLlama) median apyBase% over {len(orca_vals)} overlapping days: {orca_med:.2f}%")
    else:
        print("Orca (DeFiLlama) median apyBase%: n/a")

    if len(aprs) < 3:
        print("NOTE: fewer than 3 days of Meteora data — too early to judge.")
    if len(orca_vals) < 3:
        print("NOTE: fewer than 3 overlapping Orca days — too early to judge.")
    return 0


def rpc_dump(*, rpc_url: str, pool: str, sleep_s: float) -> int:
    session = requests.Session()
    rpc_info = solana_get_account_info(session, rpc_url, pool, sleep_s=sleep_s)

    print(f"RPC URL: {rpc_url}")
    print(f"Pool (LbPair): {pool}")
    print("")
    print("=== RAW RPC getAccountInfo response ===")
    print(json.dumps(rpc_info.raw_json, ensure_ascii=False, indent=2))
    print("=== END RAW RPC ===")
    print("")

    if not rpc_info.data_base64:
        print("No base64 data in RPC response; cannot decode.")
        return 1

    try:
        account_bytes = base64.b64decode(rpc_info.data_base64)
    except Exception as exc:
        print(f"Base64 decode failed: {exc}")
        return 1

    fee_params = decode_lbpair_fee_params(account_bytes)
    print("=== DECODED (best-effort) ===")
    print(f"data_len_bytes: {len(account_bytes)}")
    print(f"lbpair_discriminator_ok: {fee_params.discriminator_ok}")
    print(f"base_factor: {fee_params.base_factor}")
    print(f"base_fee_power_factor: {fee_params.base_fee_power_factor}")
    print(f"variable_fee_control: {fee_params.variable_fee_control}")
    print(f"bin_step: {fee_params.bin_step}")
    print(f"volatility_accumulator: {fee_params.volatility_accumulator}")
    print(f"fee_rate_calc_1e9: {fee_params.fee_rate_1e9}")
    print(f"fee_pct_calc: {fee_params.fee_pct}")
    print("=== END DECODED ===")

    # Convenience: save to /tmp for offline inspection (not in repo).
    try:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_path = f"/tmp/meteora-watch-rpc-{pool}-{stamp}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rpc_info.raw_json, f, ensure_ascii=False, indent=2)
        print("")
        print(f"Saved raw RPC JSON to: {out_path}")
    except Exception as exc:
        eprint(f"WARN: failed to write /tmp dump: {exc}")

    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only daily watcher for Meteora DLMM SOL-USDC pools (SQLite)."
    )
    parser.add_argument(
        "--db",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "meteora_watch.sqlite"),
        help="Path to SQLite DB (default: ./meteora_watch.sqlite)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=float(os.environ.get("MW_SLEEP_S", "0.25")),
        help="Base sleep between HTTP requests (seconds).",
    )

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_collect = sub.add_parser("collect", help="Collect one daily data point per pool.")
    p_collect.add_argument(
        "--query",
        default=os.environ.get("MW_QUERY", "SOL-USDC"),
        help="Meteora /pools query (default: SOL-USDC).",
    )
    p_collect.add_argument(
        "--tvl-min",
        type=float,
        default=float(os.environ.get("MW_TVL_MIN", "500000")),
        help="Track SOL-USDC pools with TVL >= this (USD), plus the top TVL pool.",
    )
    p_collect.add_argument("--page-size", type=int, default=1000)
    p_collect.add_argument("--history-limit", type=int, default=30)
    p_collect.add_argument(
        "--rpc-url",
        default=os.environ.get("SOLANA_RPC_URL", SOLANA_MAINNET_RPC_DEFAULT),
        help="Solana RPC URL (default: api.mainnet-beta.solana.com).",
    )
    p_collect.add_argument(
        "--backfill-days",
        type=int,
        default=int(os.environ.get("MW_BACKFILL_DAYS", "1")),
        help="Write last N closed days from history (default: 1 = only the last closed day).",
    )
    p_collect.add_argument(
        "--print-rpc",
        default=os.environ.get("MW_PRINT_RPC_POOL"),
        help="If set to a pool address, print raw getAccountInfo response for that pool.",
    )

    p_report = sub.add_parser("report", help="Print per-day table + medians + Orca comparison.")
    p_report.add_argument("--pool", required=True, help="Pool address (LbPair pubkey).")
    p_report.add_argument("--days", type=int, default=30, help="Max rows (latest N days).")

    p_rpc = sub.add_parser("rpc-dump", help="Print raw Solana RPC response + decoded fee params.")
    p_rpc.add_argument("--pool", required=True, help="Pool address (LbPair pubkey).")
    p_rpc.add_argument(
        "--rpc-url",
        default=os.environ.get("SOLANA_RPC_URL", SOLANA_MAINNET_RPC_DEFAULT),
        help="Solana RPC URL (default: api.mainnet-beta.solana.com).",
    )

    p_liq = sub.add_parser(
        "liquidity",
        help="Read-only snapshot: share of liquidity near current price (Meteora bins vs Orca ticks).",
    )
    p_liq.add_argument(
        "--query",
        default=os.environ.get("MW_QUERY", "SOL-USDC"),
        help="Meteora /pools query (default: SOL-USDC).",
    )
    p_liq.add_argument(
        "--tvl-min",
        type=float,
        default=float(os.environ.get("MW_TVL_MIN", "500000")),
        help="Include Meteora SOL-USDC pools with TVL >= this, plus top TVL, plus requested pool.",
    )
    p_liq.add_argument("--page-size", type=int, default=1000)
    p_liq.add_argument(
        "--rpc-url",
        default=os.environ.get("SOLANA_RPC_URL", SOLANA_MAINNET_RPC_DEFAULT),
        help="Solana RPC URL (default: api.mainnet-beta.solana.com).",
    )
    p_liq.add_argument(
        "--orca-pool",
        default=ORCA_SOL_USDC_POOL,
        help="Orca SOL-USDC Whirlpool address.",
    )
    p_liq.add_argument(
        "--thresholds",
        default="0.5,1,3",
        help="Comma-separated percent thresholds (default: 0.5,1,3).",
    )
    p_liq.add_argument(
        "--raw-bins",
        type=int,
        default=6,
        help="How many Meteora bins on each side of active to print (default: 6).",
    )

    args = parser.parse_args(argv)

    if args.cmd == "collect":
        return collect(
            db_path=args.db,
            query=args.query,
            tvl_min_usd=args.tvl_min,
            page_size=args.page_size,
            sleep_s=args.sleep,
            history_limit=args.history_limit,
            rpc_url=args.rpc_url,
            print_rpc_for_pool=args.print_rpc,
            backfill_days=args.backfill_days,
        )
    if args.cmd == "report":
        return report(
            db_path=args.db,
            pool=args.pool,
            limit_days=args.days,
            sleep_s=args.sleep,
        )
    if args.cmd == "rpc-dump":
        return rpc_dump(rpc_url=args.rpc_url, pool=args.pool, sleep_s=args.sleep)
    if args.cmd == "liquidity":
        try:
            thresholds = [
                float(x.strip())
                for x in str(args.thresholds).split(",")
                if x.strip()
            ]
        except Exception:
            thresholds = [0.5, 1.0, 3.0]
        thresholds = [x for x in thresholds if x > 0]
        if not thresholds:
            thresholds = [0.5, 1.0, 3.0]
        return liquidity(
            db_path=args.db,
            query=args.query,
            tvl_min_usd=args.tvl_min,
            page_size=args.page_size,
            sleep_s=args.sleep,
            rpc_url=args.rpc_url,
            orca_pool=args.orca_pool,
            thresholds_pct=thresholds,
            raw_bins_each_side=int(args.raw_bins),
        )
    raise RuntimeError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())

