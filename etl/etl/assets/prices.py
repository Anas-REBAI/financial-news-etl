from dagster import asset
import time
import yfinance as yf
import pandas as pd
from etl.config.settings import TICKERS

@asset
def daily_asset_prices() -> pd.DataFrame:
    """
    Récupère les prix journaliers ajustés pour une liste de tickers à l'aide de yfinance.
    """
    valid_data = []
    failed_tickers = []

    for ticker in TICKERS:
        try:
            time.sleep(2)  # Pause pour éviter le blocage
            data = yf.download(ticker, period="1d", interval="1d", auto_adjust=False)

            if "Adj Close" in data.columns:
                adj_close = data["Adj Close"].reset_index()
                adj_close['Ticker'] = ticker
                valid_data.append(adj_close)
            else:
                print(f"⚠️ Pas de 'Adj Close' pour {ticker}, colonnes disponibles : {data.columns}")
                failed_tickers.append(ticker)
        except Exception as e:
            print(f"❌ Erreur pour {ticker} : {e}")
            failed_tickers.append(ticker)

    if not valid_data:
        print("⚠️ Aucune donnée valide récupérée. Retourne un DataFrame vide.")
        return pd.DataFrame()

    df_final = pd.concat(valid_data)
    print(f"✅ Données récupérées pour {len(TICKERS) - len(failed_tickers)}/{len(TICKERS)} tickers.")
    
    return df_final
