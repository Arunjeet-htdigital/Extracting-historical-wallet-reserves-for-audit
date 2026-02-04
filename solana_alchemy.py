import os
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

# =========================
# CONFIG
# =========================
ALCHEMY_KEY = os.environ["ALCHEMY_KEY"]
ALCHEMY_RPC = f"https://solana-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"

# Token programs
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"        # classic SPL
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"    # token-2022

# Mints (mainnet)
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
USX_MINT  = "6FrrzDk5mQARGc1TDYoyVnSyRdds1t4PbtohCD6p3tgG"

# jitoSOL mint (mainnet)
JITOSOL_MINT = "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn"

STABLES = {"USDC", "USDT", "USX"}  # treat as $1 if price API doesn't return

# CoinGecko IDs for fallback pricing (if Alchemy doesn't support the symbol)
COINGECKO_IDS = {
    "JITOSOL": "jito-staked-sol",
    # You can add more here later if needed
}


# =========================
# RPC HELPERS
# =========================
def rpc(method: str, params):
    r = requests.post(
        ALCHEMY_RPC,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return data["result"]


def find_last_signature_before_ts(address: str, ts_unix: int, page_limit: int = 1000) -> Optional[str]:
    """
    Finds the last transaction signature at or before ts_unix for `address`.
    Note: Solana has slots; we approximate by last tx touching the address before timestamp.
    """
    before = None
    while True:
        cfg = {"limit": page_limit}
        if before:
            cfg["before"] = before

        sigs = rpc("getSignaturesForAddress", [address, cfg])
        if not sigs:
            return None

        for s in sigs:
            bt = s.get("blockTime")
            if bt is None:
                continue
            if bt <= ts_unix:
                return s["signature"]

        before = sigs[-1]["signature"]


def sol_balance_from_transaction(wallet: str, signature: str) -> int:
    """
    Returns wallet SOL balance (lamports) right after `signature` tx is applied.
    """
    tx = rpc("getTransaction", [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
    if not tx or "transaction" not in tx or "meta" not in tx:
        raise RuntimeError(f"Could not fetch transaction details for {signature}")

    account_keys = tx["transaction"]["message"]["accountKeys"]
    pubkeys = [(k["pubkey"] if isinstance(k, dict) else k) for k in account_keys]

    try:
        idx = pubkeys.index(wallet)
    except ValueError:
        raise RuntimeError("Wallet not found in transaction accountKeys; cannot read SOL snapshot from this tx.")

    post_balances = tx["meta"]["postBalances"]
    return int(post_balances[idx])  # lamports


def get_token_accounts_by_owner(owner: str, debug: bool = False) -> List[dict]:
    """
    Returns token accounts owned by `owner` across BOTH token programs.
    """
    all_accounts: List[dict] = []
    for program_id in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
        res = rpc(
            "getTokenAccountsByOwner",
            [
                owner,
                {"programId": program_id},
                {"encoding": "jsonParsed"},
            ],
        )
        vals = res.get("value", [])
        if debug:
            print(f"{program_id} -> {len(vals)} token accounts")
        all_accounts.extend(vals)
    return all_accounts


def get_token_accounts_by_mint(owner: str, mint: str) -> List[dict]:
    """
    Most reliable check: filter by mint directly.
    """
    res = rpc("getTokenAccountsByOwner", [owner, {"mint": mint}, {"encoding": "jsonParsed"}])
    return res.get("value", [])


def mint_to_token_accounts(owner: str, debug: bool = False) -> Dict[str, List[str]]:
    """
    Map mint -> list of token account pubkeys.
    """
    out: Dict[str, List[str]] = {}
    for item in get_token_accounts_by_owner(owner, debug=debug):
        pubkey = item["pubkey"]
        parsed = item["account"]["data"].get("parsed", {})
        info = parsed.get("info", {})
        mint = info.get("mint")
        if mint:
            out.setdefault(mint, []).append(pubkey)
    return out


def spl_balance_at_ts_via_last_tx(token_account: str, mint: str, ts_unix: int) -> Optional[float]:
    """
    Approx balance-at-time for ONE token account using the last tx that touched
    that token account at/before ts_unix.
    """
    sig = find_last_signature_before_ts(token_account, ts_unix)
    if not sig:
        return None

    tx = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
    if not tx or "transaction" not in tx or "meta" not in tx:
        return None

    account_keys = tx["transaction"]["message"]["accountKeys"]
    pubkeys = [(k["pubkey"] if isinstance(k, dict) else k) for k in account_keys]

    post_tb = tx["meta"].get("postTokenBalances") or []
    pre_tb = tx["meta"].get("preTokenBalances") or []

    def pick_balance(entries):
        for e in entries:
            if e.get("mint") != mint:
                continue
            idx = e.get("accountIndex")
            if idx is None:
                continue
            if 0 <= idx < len(pubkeys) and pubkeys[idx] == token_account:
                ui = e.get("uiTokenAmount", {})
                amt_raw = ui.get("amount")
                dec = ui.get("decimals")
                if amt_raw is not None and dec is not None:
                    return int(amt_raw) / (10 ** int(dec))
        return None

    bal = pick_balance(post_tb)
    if bal is None:
        bal = pick_balance(pre_tb)
    return bal


# =========================
# PRICES
# =========================
def historical_price_usd(symbol: str, start_iso: str, end_iso: str) -> dict:
    """
    Alchemy Prices API - historical prices for a token symbol.
    """
    url = f"https://api.g.alchemy.com/prices/v1/{ALCHEMY_KEY}/tokens/historical"
    payload = {"symbol": symbol, "startTime": start_iso, "endTime": end_iso}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def coingecko_price_on_date_usd(coin_id: str, ondate: str) -> Optional[float]:
    """
    CoinGecko history endpoint:
    GET /api/v3/coins/{id}/history?date=DD-MM-YYYY
    Returns None if unavailable.
    """
    try:
        yyyy, mm, dd = ondate.split("-")
        date_str = f"{dd}-{mm}-{yyyy}"
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
        r = requests.get(url, params={"date": date_str, "localization": "false"}, timeout=30)
        r.raise_for_status()
        j = r.json()
        price = j.get("market_data", {}).get("current_price", {}).get("usd")
        return float(price) if price is not None else None
    except Exception:
        return None


def price_on_date_usd(symbol: str, ondate: str) -> Optional[float]:
    """
    Try Alchemy Prices first; if unavailable, try CoinGecko for known IDs.
    """
    # 1) Alchemy
    try:
        prices = historical_price_usd(symbol, f"{ondate}T00:00:00Z", f"{ondate}T23:59:59Z")
        data = prices.get("data", [])
        if data:
            return float(data[0]["value"])
    except Exception:
        pass

    # 2) CoinGecko fallback
    coin_id = COINGECKO_IDS.get(symbol.upper())
    if coin_id:
        return coingecko_price_on_date_usd(coin_id, ondate)

    return None


def fallback_price(label: str) -> Optional[float]:
    """
    Stablecoin fallback.
    """
    if label in STABLES:
        return 1.0
    return None


# =========================
# MAIN FUNCTION
# =========================
def solana_wallet_value_on_date(wallet: str, ondate: str, debug: bool = True) -> Tuple[
    float, float, float, float, float, float, float
]:
    """
    Returns for the date at 23:59:59 UTC:
      sol_amount,
      sol_usd_price,
      sol_usd_value,
      usdc_usd_value,
      usdt_usd_value,
      usx_usd_value,
      jitosol_usd_value
    """

    # timestamp: end of day UTC
    year, month, day = ondate.split("-")
    dt = datetime(int(year), int(month), int(day), 23, 59, 59, tzinfo=timezone.utc)
    ts = int(dt.timestamp())

    # ---- SOL snapshot ----
    sig = find_last_signature_before_ts(wallet, ts)
    if not sig:
        if debug:
            print("No transaction history found for this wallet via getSignaturesForAddress.")
        sol_amount = 0.0
    else:
        lamports = sol_balance_from_transaction(wallet, sig)
        sol_amount = lamports / 1_000_000_000
        if debug:
            print("Last tx before timestamp:", sig)
            print("SOL:", sol_amount)

    # ---- SOL price & value ----
    sol_price = price_on_date_usd("SOL", ondate) or 0.0
    sol_usd_value = sol_amount * sol_price
    if debug:
        print("SOL price (USD):", sol_price)
        print("SOL USD value:", sol_usd_value)

    # ---- Token discovery ----
    if debug:
        print("\n=== MINT FILTER CHECK ===")
    for mint, label in [
        (USDC_MINT, "USDC"),
        (USDT_MINT, "USDT"),
        (USX_MINT, "USX"),
        (JITOSOL_MINT, "JITOSOL"),
    ]:
        accts = get_token_accounts_by_mint(wallet, mint)
        if debug:
            print(label, "mint accounts:", len(accts))
            if accts:
                print("  example token account:", accts[0]["pubkey"])

    m2tas = mint_to_token_accounts(wallet, debug=debug)

    # ---- Token balances and USD values ----
    def token_usd_value(mint: str, label: str, price_symbol: str) -> float:
        token_accounts = m2tas.get(mint, [])
        if not token_accounts:
            if debug:
                print(f"{label}: no token accounts found")
            return 0.0

        total = 0.0
        found_any = False
        for ta in token_accounts:
            bal = spl_balance_at_ts_via_last_tx(ta, mint, ts)
            if bal is not None:
                total += bal
                found_any = True

        if not found_any:
            if debug:
                print(f"{label}: couldn't derive balance snapshot (no suitable tx snapshot)")
            return 0.0

        px = price_on_date_usd(price_symbol, ondate)
        if px is None:
            px = fallback_price(label)

        if px is None:
            if debug:
                print(f"{label}: price not available (no fallback). Amount={total}")
            return 0.0

        usd_val = total * px
        if debug:
            print(f"{label} amount @ {ondate}:", total)
            print(f"{label} price (USD):", px)
            print(f"{label} USD value:", usd_val)
        return usd_val

    usdc_usd = token_usd_value(USDC_MINT, "USDC", "USDC")
    usdt_usd = token_usd_value(USDT_MINT, "USDT", "USDT")
    usx_usd  = token_usd_value(USX_MINT,  "USX",  "USX")
    jito_usd = token_usd_value(JITOSOL_MINT, "JITOSOL", "JITOSOL")

    return sol_amount, sol_price, sol_usd_value, usdc_usd, usdt_usd, usx_usd, jito_usd


# =========================
# RUN
# =========================
if __name__ == "__main__":

    wallets = ["Wallets here"
    ]

    ondate = "2025-12-31"
    for wallet in wallets:
        (
            sol,
            sol_usd_price,
            sol_usd_value,
            usdc_usd_value,
            usdt_usd_value,
            usx_usd_value,
            jitosol_usd_value,
        ) = solana_wallet_value_on_date(wallet, ondate, debug=True)

        print("\n=== SUMMARY ===")
        print(f"wallet: {wallet}")
        print("SOL:", sol)
        print("SOL USD price:", sol_usd_price)
        print("SOL USD value:", sol_usd_value)
        print("USDC USD value:", usdc_usd_value)
        print("USDT USD value:", usdt_usd_value)
        print("USX  USD value:", usx_usd_value)
        print("JITOSOL USD value:", jitosol_usd_value)
        print("#----------------------------------------------------------------------------------------------#")