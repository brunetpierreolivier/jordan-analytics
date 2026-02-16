# Jordan Analytics — Dashboard Power BI (Portfolio)

## Objectif
Construire un mini-projet Data Analyst orienté **dashboard + qualité de données** à partir de données de marché crypto (Binance Spot), avec :
- extraction automatisée
- tables BI-ready
- contrôles qualité (heures manquantes / doublons)
- détection d’anomalies (pics de variation / pics de volume)
- dashboard Power BI (Pilotage / Anomalies / Qualité)

## Données
Source : API Binance (marché Spot)  
Période : ~180 jours  
Granularité source : 1h (bougies OHLCV)  
Paires : BTCUSDC, ETHUSDC, BNBUSDC, SOLUSDC, XRPUSDC, ADAUSDC, DOGEUSDC, AVAXUSDC

## Pipeline (Python)
### 1) Extraction des données (fact)
Commande :
```powershell
python -m src.extract_klines_spot

Sortie :

data_processed/fact_klines_1h.csv

### 2) Transformation / tables BI-ready
Commande :
```powershell
python -m src.transform_kpi
Sorties :

data_processed/dim_symbol.csv : dimension des paires

data_processed/agg_daily.csv : agrégation journalière + KPI (return_1d, vol_7d, max_dd_30d, etc.)

data_processed/data_quality.csv : contrôle qualité au niveau heure (missing / duplicates / complétude)

data_processed/anomaly_events.csv : événements d’anomalies (z-score sur retours et volumes)

Dashboard Power BI

Fichier : Jordan_Analytics_NOVOMA.pbix (local)
Export PDF : docs/powerbi/exports/Jordan_Analytics_PowerBI.pdf

Pages

PILOTAGE : cours, KPIs, activité (value traded), anomalies (30j)

ANOMALIES : table d’événements + distribution par type + timeline

QUALITÉ : complétude horaire, heures manquantes, doublons

Captures

Voir docs