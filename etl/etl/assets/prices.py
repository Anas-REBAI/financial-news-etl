from dagster import Output, asset
import yfinance as yf
import pandas as pd
from etl.config.settings import TICKERS

@asset
def daily_asset_prices() -> Output[pd.DataFrame]:
    """
    Actif Dagster qui récupère les prix journaliers ajustés pour une liste de tickers via yfinance.
    """
    try:
        # Télécharger toutes les données en une seule requête pour réduire les appels API
        data = yf.download(TICKERS, period="5d", interval="1d", auto_adjust=False)
        
        # Vérification de la présence de la colonne 'Adj Close'
        if "Adj Close" not in data:
            raise ValueError("⚠️ 'Adj Close' non trouvé dans les données récupérées.")

        # Transformation du DataFrame en format long (melt) avec les tickers
        df_final = data["Adj Close"].reset_index().melt(id_vars=["Date"], var_name="Ticker", value_name="Adj Close")

        # Message de suivi dans Dagster UI
        return Output(df_final, metadata={
            "tickers_récupérés": df_final["Ticker"].nunique(),
            "nombre_lignes": df_final.shape[0],
        })

    except Exception as e:
        raise Exception(f"❌ Erreur lors de la récupération des données : {e}")