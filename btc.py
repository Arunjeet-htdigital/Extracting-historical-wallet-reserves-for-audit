import os
import time
import random
import requests
from datetime import datetime, timezone
from collections import defaultdict
import pandas as pd
from typing import Optional, Dict, Tuple, List

# =========================
# CONFIG
# =========================
ALCHEMY_KEY = os.environ["ALCHEMY_KEY"]

# Blockstream Esplora (Bitcoin mainnet)
ESPLORA_API = "https://blockstream.info/api"

# Retry policy
MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 0.8  # exponential backoff base
JITTER_SECONDS = 0.25       # small random jitter

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "btc-snapshot/1.0"})


# =========================
# RETRY WRAPPER
# =========================
def request_with_retry(method: str, url: str, *, params=None, json=None, timeout=30):
    """
    Retries up to MAX_RETRIES with exponential backoff + jitter.
    Handles common transient network errors, including WinError 10054.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.request(method, url, params=params, json=json, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_exc = e
        except requests.exceptions.HTTPError as e:
            # Retry on typical transient status codes
            status = getattr(e.response, "status_code", None)
            if status in (429, 500, 502, 503, 504):
                last_exc = e
            else:
                raise

        if attempt < MAX_RETRIES:
            backoff = (BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))) + random.uniform(0, JITTER_SECONDS)
            time.sleep(backoff)

    raise RuntimeError(f"Request failed after {MAX_RETRIES} retries: {url}") from last_exc


# =========================
# PRICES (Alchemy Prices API)
# =========================
def historical_price_usd(symbol: str, start_iso: str, end_iso: str) -> dict:
    url = f"https://api.g.alchemy.com/prices/v1/{ALCHEMY_KEY}/tokens/historical"
    payload = {"symbol": symbol, "startTime": start_iso, "endTime": end_iso}
    r = request_with_retry("POST", url, json=payload, timeout=30)
    return r.json()

def _extract_price_points(payload: dict) -> list[dict]:
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("prices"), list):
        return data["prices"]
    if isinstance(data, list):
        return data
    if isinstance(payload.get("prices"), list):
        return payload["prices"]
    return []

def fx_rate_usd_end_of_day(symbol: str, ondate: str) -> float:
    start = f"{ondate}T00:00:00Z"
    end = f"{ondate}T23:59:59Z"
    payload = historical_price_usd(symbol, start, end)
    points = _extract_price_points(payload)
    if not points:
        raise RuntimeError(f"No price points returned for {symbol} on {ondate}")

    def parse_ts(p):
        ts = p.get("timestamp")
        if not ts:
            return datetime.min.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    latest = max(points, key=parse_ts)
    return float(latest["value"])


# =========================
# ESPLORA HELPERS (BTC)
# =========================
def esplora_get_json(path: str):
    r = request_with_retry("GET", f"{ESPLORA_API}{path}", timeout=30)
    return r.json()

def get_address_txs(address: str, last_seen_txid: Optional[str] = None) -> List[dict]:
    if last_seen_txid:
        return esplora_get_json(f"/address/{address}/txs/chain/{last_seen_txid}")
    return esplora_get_json(f"/address/{address}/txs")

def tx_block_time(tx: dict) -> Optional[int]:
    st = tx.get("status", {})
    if st.get("confirmed"):
        return st.get("block_time")
    return None


# =========================
# BALANCE RECONSTRUCTION
# =========================
def btc_balance_at_timestamp(address: str, ts_cutoff: int, max_pages: int = 200) -> int:
    all_txs = []
    last = None

    for _ in range(max_pages):
        page = get_address_txs(address, last_seen_txid=last)
        if not page:
            break
        all_txs.extend(page)
        last = page[-1]["txid"]

    eligible = []
    for tx in all_txs:
        bt = tx_block_time(tx)
        if bt is not None and bt <= ts_cutoff:
            eligible.append(tx)

    eligible.sort(key=lambda t: tx_block_time(t) or 0)

    utxos: Dict[Tuple[str, int], int] = {}

    for tx in eligible:
        txid = tx["txid"]

        # Spend
        for vin in tx.get("vin", []):
            prev = vin.get("prevout")
            if not prev:
                continue
            if prev.get("scriptpubkey_address") == address:
                prev_txid = vin.get("txid")
                prev_vout = vin.get("vout")
                if prev_txid is not None and prev_vout is not None:
                    utxos.pop((prev_txid, prev_vout), None)

        # Receive
        for idx, vout in enumerate(tx.get("vout", [])):
            if vout.get("scriptpubkey_address") == address:
                utxos[(txid, idx)] = int(vout.get("value", 0))

    return sum(utxos.values())


# =========================
# MAIN (BTC)
# =========================
def btc_snapshot_with_usd(address: str, ondate: str) -> pd.DataFrame:
    data = defaultdict(list)

    y, m, d = ondate.split("-")
    dt = datetime(int(y), int(m), int(d), 23, 59, 59, tzinfo=timezone.utc)
    ts = int(dt.timestamp())

    sats = btc_balance_at_timestamp(address, ts)
    btc_amt = sats / 100_000_000

    btc_usd_rate = fx_rate_usd_end_of_day("BTC", ondate)
    btc_usd_value = btc_amt * btc_usd_rate

    data["wallet"].append(address)
    data["date"].append(ondate)
    data["btc"].append(btc_amt)
    data["btc_usd_rate"].append(btc_usd_rate)
    data["btc_usd_value"].append(btc_usd_value)

    return data



if __name__ == "__main__":

    addresses=["Wallets here"]
        
    dates="2025-12-31"

    data_f = defaultdict(list)

    for addr in addresses:
        row = btc_snapshot_with_usd(addr, dates)
        for i in row.keys():
            data_f[i].append(row[i][0])
    
    df=pd.DataFrame(data_f)
    df.to_csv(r"btc_chain_value_corprime", index=False)

    print(df)
