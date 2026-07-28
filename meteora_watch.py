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
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


METEORA_API_BASE = "https://dlmm.datapi.meteora.ag"
SOLANA_MAINNET_RPC_DEFAULT = "https://api.mainnet-beta.solana.com"
DLMM_PROGRAM_ID = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"

DEFI_LLAMA_ORCA_SOL_USDC_CHART = "a5c85bc8-eb41-45c0-a520-d18d7529c0d8"
DEFI_LLAMA_YIELDS_CHART_URL = f"https://yields.llama.fi/chart/{DEFI_LLAMA_ORCA_SOL_USDC_CHART}"

# Anchor discriminator for account "LbPair" from Meteora DLMM IDL.
LBPAIR_DISCRIMINATOR = bytes([33, 11, 49, 98, 181, 101, 177, 13])

FEE_DENOMINATOR = 1_000_000_000
MAX_FEE_RATE_1E9 = 100_000_000  # 10% in 1e9 precision


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

                apr_simple_pct = fees / tvl * 365.0 * 100.0
                updated_at = datetime.now(timezone.utc).isoformat()

                row: Dict[str, Any] = {
                    "pool_address": addr,
                    "day": day,
                    "pool_name": details.get("name") or name,
                    "token_x_symbol": safe_get(details, "token_x", "symbol") or token_x_symbol,
                    "token_y_symbol": safe_get(details, "token_y", "symbol") or token_y_symbol,
                    "tvl_usd": float(tvl),
                    "fees_usd": float(fees),
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
    raise RuntimeError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())

