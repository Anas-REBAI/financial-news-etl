import numpy as np
import pandas as pd
from dagster import asset, Output, MetadataValue, AssetCheckResult, AssetCheckSeverity
from etl.config.data_quality_checks import check_completeness, check_validity, check_consistency, check_uniqueness

@asset
def daily_asset_returns(daily_asset_prices: pd.DataFrame) -> Output[pd.DataFrame]:
    """
    Computes daily simple and logarithmic returns for each asset.
    Performs data quality checks to ensure the data is complete, valid, consistent, and unique.
    """
    if daily_asset_prices.empty:
        return Output(pd.DataFrame(), metadata={"status": "Empty data"})
    
    # Convert date to datetime
    daily_asset_prices["Date"] = pd.to_datetime(daily_asset_prices["Date"])

    # Sort data chronologically
    daily_asset_prices = daily_asset_prices.sort_values(by=["Ticker", "Date"])

    # Compute simple return (%)
    daily_asset_prices["Simple Return"] = daily_asset_prices.groupby("Ticker")["Adj Close"].pct_change() * 100

    # Compute log return
    daily_asset_prices["Log Return"] = daily_asset_prices.groupby("Ticker")["Adj Close"].transform(lambda x: np.log(x / x.shift(1)))

    # Remove NaN values (first row for each Ticker)
    daily_asset_returns = daily_asset_prices.dropna().reset_index(drop=True)

    # Data Quality Checks
    if not check_completeness(daily_asset_returns, required_columns=["Date", "Ticker", "Simple Return", "Log Return"]):
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description="❌ Données incomplètes : certaines colonnes nécessaires sont manquantes ou vides."
        )
    if not check_validity(daily_asset_returns, column="Simple Return", min_value=-100, max_value=100):
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description="❌ Données invalides : les rendements doivent être entre -100% et +100%."
        )
    if not check_consistency(daily_asset_returns, date_column="Date", group_column="Ticker"):
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARNING,
            description="⚠️ Données incohérentes : les dates ne sont pas dans l'ordre chronologique."
        )
    if not check_uniqueness(daily_asset_returns, subset=["Ticker", "Date"]):
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARNING,
            description="⚠️ Données non uniques : il y a des doublons (même ticker et même date)."
        )

    return Output(
        daily_asset_returns,
        metadata={
            "tickers_calculated": len(daily_asset_returns["Ticker"].unique()),
            "row_count": daily_asset_returns.shape[0],
            "preview": MetadataValue.md(daily_asset_returns.to_markdown()),
            "data_quality_checks": {
                "completeness": check_completeness(daily_asset_returns, required_columns=["Date", "Ticker", "Simple Return", "Log Return"]),
                "validity": check_validity(daily_asset_returns, column="Simple Return", min_value=-100, max_value=100),
                "consistency": check_consistency(daily_asset_returns, date_column="Date", group_column="Ticker"),
                "uniqueness": check_uniqueness(daily_asset_returns, subset=["Ticker", "Date"]),
            }
        }
    )
