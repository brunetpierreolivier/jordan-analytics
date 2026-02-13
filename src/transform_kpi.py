# src/transform_kpi.py
import os
import pandas as pd

from src.config import OUT_DIR_PROCESSED

FACT_PATH = os.path.join(OUT_DIR_PROCESSED, "fact_klines_1h.csv")

def build_dim_symbol(df_fact: pd.DataFrame) -> pd.DataFrame:
    """
    Construit une table dimension 'symbol' à partir de fact_klines.
    Ex:
      BTCUSDC -> base_asset=BTC, quote_asset=USDC
    """
    symbols = sorted(df_fact["symbol"].unique().tolist())
    rows = []
    for s in symbols:
        # Hypothèse simple : quote=USDC car on a choisi des paires *USDC
        # On coupe la fin "USDC" pour obtenir le base_asset.
        if s.endswith("USDC"):
            base = s.replace("USDC", "")
            quote = "USDC"
        else:
            # fallback générique si un jour tu ajoutes d'autres quotes
            base, quote = s[:-4], s[-4:]

        rows.append({
            "symbol": s,
            "base_asset": base,
            "quote_asset": quote
        })

    return pd.DataFrame(rows)

def compute_drawdown(series: pd.Series) -> pd.Series:
    """
    Drawdown = (prix / max_cumule) - 1
    """
    running_max = series.cummax()
    return (series / running_max) - 1

def build_agg_daily(df_fact: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège en journalier par symbol.
    - close du dernier point de la journée
    - value_traded = close * volume_base (somme jour) (proxy)
    - return_1d = close.pct_change()
    - vol_7d = std des returns journaliers sur 7 jours
    - max_dd_30d = minimum du drawdown sur 30 jours
    """
    df = df_fact.copy()

    # On crée une colonne date (UTC) pour grouper au jour
    df["date"] = df["open_time_utc"].dt.date

    # value traded horaire (proxy) = close * volume_base
    df["value_traded_hour"] = df["close"] * df["volume_base"]

    # Agrégation journalière
    daily = (
        df.sort_values(["symbol", "open_time_utc"])
          .groupby(["symbol", "market", "interval", "date"], as_index=False)
          .agg(
              close=("close", "last"),
              volume_base=("volume_base", "sum"),
              value_traded=("value_traded_hour", "sum"),
              trades_count=("trades_count", "sum")
          )
    )

    # KPI dérivés par symbol
    daily = daily.sort_values(["symbol", "date"]).reset_index(drop=True)

    # Return 1 jour
    daily["return_1d"] = daily.groupby("symbol")["close"].pct_change()

    # Volatilité 7 jours (std des returns sur fenêtre 7)
    daily["vol_7d"] = (
        daily.groupby("symbol")["return_1d"]
             .rolling(window=7, min_periods=3)
             .std()
             .reset_index(level=0, drop=True)
    )

    # Drawdown et max drawdown 30 jours
    daily["drawdown"] = daily.groupby("symbol")["close"].apply(compute_drawdown).reset_index(level=0, drop=True)
    daily["max_dd_30d"] = (
        daily.groupby("symbol")["drawdown"]
             .rolling(window=30, min_periods=5)
             .min()
             .reset_index(level=0, drop=True)
    )

    # Nettoyage : convertir date en string (Power BI friendly) ou datetime
    daily["date"] = pd.to_datetime(daily["date"])

    # Colonnes finales (ordre clair)
    daily = daily[[
        "date", "symbol", "market", "interval",
        "close", "return_1d", "vol_7d", "max_dd_30d",
        "volume_base", "value_traded", "trades_count"
    ]]

    return daily

def build_data_quality_hourly(df_fact: pd.DataFrame) -> pd.DataFrame:
    """
    Contrôle qualité au niveau heure (1h).
    Mesure :
    - heures attendues vs présentes
    - heures manquantes
    - doublons (même symbol + même open_time_utc)
    - taux de complétude
    """
    df = df_fact.copy()

    # Sécurité : on s'assure que open_time_utc est bien datetime
    df["open_time_utc"] = pd.to_datetime(df["open_time_utc"], utc=True)

    rows = []
    for symbol, g in df.groupby("symbol"):
        g = g.sort_values("open_time_utc")

        # Période réellement couverte dans les données
        start = g["open_time_utc"].min().floor("h")
        end = g["open_time_utc"].max().floor("h")

        # Série attendue (toutes les heures entre start et end)
        expected_index = pd.date_range(start=start, end=end, freq="h", tz="UTC")
        expected_hours = len(expected_index)

        # Heures présentes (attention doublons)
        present_unique = g["open_time_utc"].nunique()

        # Doublons = lignes - uniques
        duplicates = len(g) - present_unique

        # Heures manquantes = attendues - présentes
        missing = expected_hours - present_unique

        # Taux de complétude
        completeness = (present_unique / expected_hours) * 100 if expected_hours > 0 else 0.0

        rows.append({
            "symbol": symbol,
            "start_utc": start,
            "end_utc": end,
            "expected_hours": expected_hours,
            "present_unique_hours": present_unique,
            "missing_hours": missing,
            "duplicate_rows": duplicates,
            "completeness_pct": round(completeness, 2)
        })

    return pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)

def zscore(series: pd.Series) -> pd.Series:
    """
    Z-score = (x - moyenne) / écart-type
    Sert à repérer les valeurs "très éloignées" de la normale.
    """
    mean = series.mean(skipna=True)
    std = series.std(skipna=True)
    if std == 0 or pd.isna(std):
        return pd.Series([0] * len(series), index=series.index)
    return (series - mean) / std

def build_anomaly_events_daily(agg_daily: pd.DataFrame, z_thresh: float = 3.0) -> pd.DataFrame:
    """
    Crée une table d'événements d'anomalies à partir des données journalières.
    - Anomalie return : |zscore(return_1d)| >= z_thresh
    - Anomalie volume : zscore(value_traded) >= z_thresh
    """
    df = agg_daily.copy()
    df = df.sort_values(["symbol", "date"])

    events = []

    for symbol, g in df.groupby("symbol"):
        g = g.copy()

        # Z-scores par symbol (comparaison "à soi-même")
        g["z_return"] = zscore(g["return_1d"].fillna(0))
        g["z_value_traded"] = zscore(g["value_traded"].fillna(0))

        # Return anomalies (absolu)
        mask_ret = g["z_return"].abs() >= z_thresh
        for _, row in g.loc[mask_ret].iterrows():
            events.append({
                "date": row["date"],
                "symbol": symbol,
                "anomaly_type": "RETURN_SPIKE",
                "score": round(float(abs(row["z_return"])), 3),
                "metric": "return_1d",
                "metric_value": float(row["return_1d"]) if pd.notna(row["return_1d"]) else None
            })

        # Volume anomalies (positif)
        mask_vol = g["z_value_traded"] >= z_thresh
        for _, row in g.loc[mask_vol].iterrows():
            events.append({
                "date": row["date"],
                "symbol": symbol,
                "anomaly_type": "VOLUME_SPIKE",
                "score": round(float(row["z_value_traded"]), 3),
                "metric": "value_traded",
                "metric_value": float(row["value_traded"]) if pd.notna(row["value_traded"]) else None
            })

    events_df = pd.DataFrame(events)
    if events_df.empty:
        # Toujours retourner un DataFrame avec les colonnes attendues
        events_df = pd.DataFrame(columns=["date", "symbol", "anomaly_type", "score", "metric", "metric_value"])
    else:
        events_df["date"] = pd.to_datetime(events_df["date"])
        events_df = events_df.sort_values(["date", "symbol", "anomaly_type"]).reset_index(drop=True)

    return events_df


def main():
    os.makedirs(OUT_DIR_PROCESSED, exist_ok=True)

    df_fact = pd.read_csv(FACT_PATH, parse_dates=["open_time_utc", "close_time_utc"])
    dim_symbol = build_dim_symbol(df_fact)

    out_path = os.path.join(OUT_DIR_PROCESSED, "dim_symbol.csv")
    dim_symbol.to_csv(out_path, index=False)

    agg_daily = build_agg_daily(df_fact)
    out_path_daily = os.path.join(OUT_DIR_PROCESSED, "agg_daily.csv")
    agg_daily.to_csv(out_path_daily, index=False)

    data_quality = build_data_quality_hourly(df_fact)
    out_path_quality = os.path.join(OUT_DIR_PROCESSED, "data_quality.csv")
    data_quality.to_csv(out_path_quality, index=False)
    
    anomaly_events = build_anomaly_events_daily(agg_daily, z_thresh=3.0)
    out_path_anom = os.path.join(OUT_DIR_PROCESSED, "anomaly_events.csv")
    anomaly_events.to_csv(out_path_anom, index=False)

    print(f"[OK] Saved: {out_path_anom}")
    print(anomaly_events.head(10))


    print(f"[OK] Saved: {out_path_quality}")
    print(data_quality)

    print(f"[OK] Saved: {out_path_daily}")
    print(agg_daily.head(3))

    print(f"[OK] Saved: {out_path}")
    print(dim_symbol)

if __name__ == "__main__":
    main()
