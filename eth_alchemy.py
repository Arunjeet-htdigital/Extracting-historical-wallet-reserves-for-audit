import os
import requests
from datetime import datetime, timezone
from collections import defaultdict
import pandas as pd

# =========================
# CONFIG
# =========================
ALCHEMY_KEY = os.environ["ALCHEMY_KEY"]
ALCHEMY_RPC = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"

ETHERSCAN_KEY = os.environ.get("ETHERSCAN_KEY")

TOKENS = {
    "USDC": {"address": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "decimals": 6},
    "USDT": {"address": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "decimals": 6},
    "WETH": {"address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "decimals": 18},
}

STABLE_FALLBACK = {"USDC": 1.0, "USDT": 1.0}


# =========================
# EVM RPC HELPERS
# =========================
def rpc(method, params):
    r = requests.post(
        ALCHEMY_RPC,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"])
    return j["result"]


def get_block_by_time_etherscan(ts_unix: int) -> int:
    """
    Closest block BEFORE a unix timestamp (seconds).
    Uses Etherscan 'getblocknobytime' endpoint.
    """
    if not ETHERSCAN_KEY:
        raise RuntimeError("Set ETHERSCAN_KEY.")

    url = "https://api.etherscan.io/v2/api"
    params = {
        "chainid": 1,
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": ts_unix,
        "closest": "before",
        "apikey": ETHERSCAN_KEY,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "1":
        raise RuntimeError(j)
    return int(j["result"])


def eth_balance_at_block(address: str, block_number: int) -> int:
    """Returns wei."""
    bal_hex = rpc("eth_getBalance", [address, hex(block_number)])
    return int(bal_hex, 16)


def erc20_balance_at_block(holder: str, token_contract: str, block_number: int) -> int:
    """
    ERC20 balanceOf(holder) at block_number.
    Returns raw token units (divide by decimals).
    """
    holder_clean = holder.lower().replace("0x", "").rjust(64, "0")
    data = "0x70a08231" + holder_clean  # balanceOf(address) selector + padded address
    call_obj = {"to": token_contract, "data": data}
    out = rpc("eth_call", [call_obj, hex(block_number)])
    return int(out, 16)


# =========================
# PRICES (Alchemy Prices API)
# =========================
def historical_price_usd(symbol: str, start_iso: str, end_iso: str) -> dict:
    """
    Alchemy Prices API historical endpoint.
    """
    url = f"https://api.g.alchemy.com/prices/v1/{ALCHEMY_KEY}/tokens/historical"
    payload = {"symbol": symbol, "startTime": start_iso, "endTime": end_iso}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def _extract_price_points(payload: dict) -> list[dict]:
    """
    Supports both possible response shapes:
      - {"data": {"symbol": "...", "prices": [{"value": "...", "timestamp": "..."}]}}
      - older/alternate shapes some users see in practice
    """
    data = payload.get("data")

    if isinstance(data, dict) and isinstance(data.get("prices"), list):
        return data["prices"]
    if isinstance(data, list):
        return data
    if isinstance(payload.get("prices"), list):
        return payload["prices"]

    return []


def fx_rate_usd_end_of_day(symbol: str, ondate: str) -> float:
    """
    Picks the latest available price point in the day [00:00:00Z, 23:59:59Z].
    Falls back to 1.0 for stablecoins if none found.
    """
    start = f"{ondate}T00:00:00Z"
    end = f"{ondate}T23:59:59Z"

    try:
        payload = historical_price_usd(symbol, start, end)
        points = _extract_price_points(payload)

        if not points:
            if symbol in STABLE_FALLBACK:
                return STABLE_FALLBACK[symbol]
            raise RuntimeError(f"No price points returned for {symbol} on {ondate}")

        def parse_ts(p):
            ts = p.get("timestamp")
            if not ts:
                return datetime.min.replace(tzinfo=timezone.utc)
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))

        latest = max(points, key=parse_ts)
        return float(latest["value"])

    except Exception:
        if symbol in STABLE_FALLBACK:
            return STABLE_FALLBACK[symbol]
        raise


# =========================
# MAIN
# =========================
def eth_snapshot_with_usd(wallet: str, ondate: str) -> pd.DataFrame:
    """
    Returns a 1-row DataFrame with:
      - balances at closest block before 23:59:59 UTC on the date
      - USD FX rates for ETH/USDC/USDT/WETH on that date (latest point in the day)
      - USD values for ETH/USDC/USDT/WETH + total
    """
    data = defaultdict(list)

    # timestamp: end of day UTC
    year, month, day = ondate.split("-")
    dt = datetime(int(year), int(month), int(day), 23, 59, 59, tzinfo=timezone.utc)
    ts = int(dt.timestamp())

    # block snapshot
    block = get_block_by_time_etherscan(ts)

    # balances
    wei = eth_balance_at_block(wallet, block)
    eth_amt = wei / 10**18

    usdc_raw = erc20_balance_at_block(wallet, TOKENS["USDC"]["address"], block)
    usdt_raw = erc20_balance_at_block(wallet, TOKENS["USDT"]["address"], block)
    weth_raw = erc20_balance_at_block(wallet, TOKENS["WETH"]["address"], block)

    usdc_amt = usdc_raw / (10 ** TOKENS["USDC"]["decimals"])
    usdt_amt = usdt_raw / (10 ** TOKENS["USDT"]["decimals"])
    weth_amt = weth_raw / (10 ** TOKENS["WETH"]["decimals"])

    # FX rates (USD) — latest available point in the day
    eth_usd_rate = fx_rate_usd_end_of_day("ETH", ondate)
    usdc_usd_rate = fx_rate_usd_end_of_day("USDC", ondate)
    usdt_usd_rate = fx_rate_usd_end_of_day("USDT", ondate)

    # For WETH, use ETH price unless you explicitly want WETH symbol pricing
    # (WETH is 1:1 with ETH)
    weth_usd_rate = eth_usd_rate

    # USD values
    eth_usd_value = eth_amt * eth_usd_rate
    usdc_usd_value = usdc_amt * usdc_usd_rate
    usdt_usd_value = usdt_amt * usdt_usd_rate
    weth_usd_value = weth_amt * weth_usd_rate

    total_usd_value = eth_usd_value + usdc_usd_value + usdt_usd_value + weth_usd_value

    # dataframe row
    data["wallet"].append(wallet)
    data["date"].append(ondate)
    data["block_closest_before"].append(block)

    data["eth"].append(eth_amt)
    data["usdc"].append(usdc_amt)
    data["usdt"].append(usdt_amt)
    data["weth"].append(weth_amt)

    data["eth_usd_rate"].append(eth_usd_rate)
    data["usdc_usd_rate"].append(usdc_usd_rate)
    data["usdt_usd_rate"].append(usdt_usd_rate)
    data["weth_usd_rate"].append(weth_usd_rate)

    data["eth_usd_value"].append(eth_usd_value)
    data["usdc_usd_value"].append(usdc_usd_value)
    data["usdt_usd_value"].append(usdt_usd_value)
    data["weth_usd_value"].append(weth_usd_value)

    data["total_usd_value"].append(total_usd_value)

    return data


if __name__ == "__main__":


    addresses=["MY WALLETS"
    ]

    dates="2025-12-31"

    data_f = defaultdict(list)

    for addr in addresses:
        row = eth_snapshot_with_usd(addr, dates)
        for i in row.keys():
            data_f[i].append(row[i][0])
    
    df=pd.DataFrame(data_f)
    df.to_csv(r"ether_chain_value_corprime", index=False)



#$env:ALCHEMY_KEY="va4qc5t8sBBvSFG77bJFw"
#$env:ETHERSCAN_KEY="SM8GKZNUWRX9VDX41Y64EAVDIBI7AZ7XIK"