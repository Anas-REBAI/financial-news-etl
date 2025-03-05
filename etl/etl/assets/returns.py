import numpy as np
import pandas as pd
from dagster import asset, Output, MetadataValue

@asset
def daily_asset_returns(daily_asset_prices: pd.DataFrame) -> Output[pd.DataFrame]:
    """
    Computes daily simple and logarithmic returns for each asset.
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

    return Output(
        daily_asset_returns,
        metadata={
            "tickers_calculated": len(daily_asset_returns["Ticker"].unique()),
            "row_count": daily_asset_returns.shape[0],
            "preview": MetadataValue.md(daily_asset_returns.to_markdown()),
        }
    )
