from dagster import Output, asset, MetadataValue
import yfinance as yf
import pandas as pd
from etl.config.settings import TICKERS

@asset
def daily_asset_prices() -> Output[pd.DataFrame]:
    """
    Retrieves daily adjusted prices for a list of tickers using yfinance.
    """
    try:
        # Download all data in a single request to minimize API calls
        data = yf.download(TICKERS, period="5d", interval="1d", auto_adjust=False)
        
        # Check for the presence of the 'Adj Close' column
        if "Adj Close" not in data:
            raise ValueError("⚠️ 'Adj Close' not found in the retrieved data.")

        # Transform the DataFrame into long format (melt) with tickers
        df_final = data["Adj Close"].reset_index().melt(id_vars=["Date"], var_name="Ticker", value_name="Adj Close")

        # Tracking message for Dagster UI
        return Output(df_final, metadata={
            "retrieved_tickers": df_final["Ticker"].nunique(),
            "number_of_rows": df_final.shape[0],
            "preview": MetadataValue.md(df_final.to_markdown()),
        })
    except Exception as e:
        raise Exception(f"❌ Error while retrieving data: {e}")