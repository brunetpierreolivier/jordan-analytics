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

def fetch_klines_paginated(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = 1000):
    """
    Récupère toutes les bougies entre start_ms et end_ms en plusieurs appels (pagination),
    car Binance limite la réponse à 1000 klines max par requête.
    """
    all_klines = []
    current_start = start_ms

    while True:
        chunk = fetch_klines(symbol, interval, current_start, end_ms, limit=limit)
        if not chunk:
            break

        all_klines.extend(chunk)

        # On avance : la dernière bougie retournée donne son open_time_ms en position 0
        last_open_time_ms = chunk[-1][0]

        # Si on n'a pas rempli le 'limit', c'est qu'on est arrivé au bout
        if len(chunk) < limit:
            break

        # Sinon on repart juste après la dernière bougie pour éviter les doublons
        current_start = last_open_time_ms + 1

        # Sécurité anti-boucle infinie (au cas où l'API renverrait toujours la même chose)
        if current_start >= end_ms:
            break

    return all_klines


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

    all_dfs = []
    for symbol in SYMBOLS:
        print(f"[INFO] Fetch klines for {symbol} interval={INTERVAL} limit={KLINES_LIMIT}")
        klines_json = fetch_klines_paginated(symbol, INTERVAL, start_ms, end_ms, limit=KLINES_LIMIT)

        df_symbol = klines_to_df(klines_json, symbol)
        all_dfs.append(df_symbol)

        print(f"[OK] {symbol}: {len(df_symbol)} rows")

    df_all = pd.concat(all_dfs, ignore_index=True)

    out_path = os.path.join(OUT_DIR_PROCESSED, "fact_klines_1h.csv")
    df_all.to_csv(out_path, index=False)

    print(f"[OK] Saved full CSV: {out_path}")
    print(df_all.head(3))

if __name__ == "__main__":
    main()
