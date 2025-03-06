from dagster import Output, asset, MetadataValue, AssetCheckResult, AssetCheckSeverity
import yfinance as yf
import pandas as pd
from etl.config.settings import TICKERS
from etl.config.data_quality_checks import check_completeness, check_validity, check_consistency, check_uniqueness

@asset
def daily_asset_prices() -> Output[pd.DataFrame]:
    """
    Retrieves daily adjusted prices for a list of tickers using yfinance.
    Performs data quality checks to ensure the data is complete, valid, consistent, and unique.
    """
    try:
        # Download all data in a single request to minimize API calls
        data = yf.download(TICKERS, period="5d", interval="1d", auto_adjust=False)
        
        # Check for the presence of the 'Adj Close' column
        if "Adj Close" not in data:
            raise ValueError("⚠️ 'Adj Close' not found in the retrieved data.")

        # Transform the DataFrame into long format (melt) with tickers
        df_final = data["Adj Close"].reset_index().melt(id_vars=["Date"], var_name="Ticker", value_name="Adj Close")

        # Data Quality Checks
        if not check_completeness(df_final, required_columns=["Date", "Ticker", "Adj Close"]):
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="❌ Données incomplètes : certaines colonnes nécessaires sont manquantes ou vides."
            )
        if not check_validity(df_final, column="Adj Close", min_value=0):
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="❌ Données invalides : les prix ajustés doivent être positifs."
            )
        if not check_consistency(df_final, date_column="Date", group_column="Ticker"):
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARNING,
                description="⚠️ Données incohérentes : les dates ne sont pas dans l'ordre chronologique."
            )
        if not check_uniqueness(df_final, subset=["Ticker", "Date"]):
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARNING,
                description="⚠️ Données non uniques : il y a des doublons (même ticker et même date)."
            )

        # Tracking message for Dagster UI
        return Output(df_final, metadata={
            "retrieved_tickers": df_final["Ticker"].nunique(),
            "number_of_rows": df_final.shape[0],
            "preview": MetadataValue.md(df_final.to_markdown()),
            "data_quality_checks": {
                "completeness": check_completeness(df_final, required_columns=["Date", "Ticker", "Adj Close"]),
                "validity": check_validity(df_final, column="Adj Close", min_value=0),
                "consistency": check_consistency(df_final, date_column="Date", group_column="Ticker"),
                "uniqueness": check_uniqueness(df_final, subset=["Ticker", "Date"]),
            }
        })
    except Exception as e:
        raise Exception(f"❌ Error while retrieving data: {e}")