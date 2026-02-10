# src/00_config.py
from datetime import datetime, timedelta, timezone

# --- Binance SPOT base URL ---
BINANCE_SPOT_BASE_URL = "https://api.binance.com"

# --- Projet : univers analysé (MVP) ---
SYMBOLS = [
    "BTCUSDC", "ETHUSDC", "BNBUSDC", "SOLUSDC",
    "XRPUSDC", "ADAUSDC", "DOGEUSDC", "AVAXUSDC"
]

# Klines interval : 1h (Power BI friendly)
INTERVAL = "1h"

# Période : 180 jours
DAYS_BACK = 180

# Limite max Binance par appel klines = 1000 (on paginera)
KLINES_LIMIT = 1000

# Répertoire de sortie (local)
OUT_DIR_PROCESSED = "data_processed"

def compute_time_range_utc(days_back: int = DAYS_BACK):
    """
    Retourne (start_ms, end_ms) en millisecondes UTC pour l'API Binance.
    - end = maintenant (UTC)
    - start = end - days_back
    """
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    return start_ms, end_ms
