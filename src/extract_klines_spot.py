# src/01_extract_klines_spot.py
import os
import requests
import pandas as pd

from src.config import (
    BINANCE_SPOT_BASE_URL, SYMBOLS, INTERVAL, KLINES_LIMIT,
    OUT_DIR_PROCESSED, compute_time_range_utc
)

def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = 1000):
    """
    Appelle l'endpoint Binance SPOT /api/v3/klines.
    Retourne une liste de bougies (klines).
    """
    url = f"{BINANCE_SPOT_BASE_URL}/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def klines_to_df(klines_json, symbol: str):
    """
    Convertit la réponse JSON klines en DataFrame structuré.
    """
    cols = [
        "open_time_ms", "open", "high", "low", "close", "volume_base",
        "close_time_ms", "quote_asset_volume", "trades_count",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"
    ]
    df = pd.DataFrame(klines_json, columns=cols)

    # Types numériques
    for c in ["open", "high", "low", "close", "volume_base", "quote_asset_volume",
              "taker_buy_base_vol", "taker_buy_quote_vol"]:
        df[c] = df[c].astype(float)

    df["trades_count"] = df["trades_count"].astype(int)

    # Dates lisibles (UTC)
    df["open_time_utc"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    df["close_time_utc"] = pd.to_datetime(df["close_time_ms"], unit="ms", utc=True)

    df["symbol"] = symbol
    df["market"] = "SPOT"
    df["interval"] = INTERVAL

    # Colonnes utiles pour Power BI (ordre clair)
    df = df[[
        "open_time_utc", "close_time_utc", "symbol", "market", "interval",
        "open", "high", "low", "close",
        "volume_base", "quote_asset_volume", "trades_count"
    ]]
    return df

def main():
    os.makedirs(OUT_DIR_PROCESSED, exist_ok=True)

    start_ms, end_ms = compute_time_range_utc()
    symbol = SYMBOLS[0]  # test sur la 1ère paire uniquement

    print(f"[INFO] Fetch klines for {symbol} interval={INTERVAL} limit={KLINES_LIMIT}")
    klines_json = fetch_klines(symbol, INTERVAL, start_ms, end_ms, limit=KLINES_LIMIT)

    df = klines_to_df(klines_json, symbol)
    out_path = os.path.join(OUT_DIR_PROCESSED, "fact_klines_1h_sample.csv")
    df.to_csv(out_path, index=False)

    print(f"[OK] Saved sample CSV: {out_path}")
    print(df.head(3))

if __name__ == "__main__":
    main()
