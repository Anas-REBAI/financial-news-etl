import pytest
import pandas as pd
from etl.assets.prices import daily_asset_prices

def test_daily_asset_prices():
    """Test if daily_asset_prices returns a structured DataFrame."""
    df_prices = daily_asset_prices().value  # Extracting the DataFrame from Dagster Output
    
    assert not df_prices.empty, "❌ daily_asset_prices returned an empty DataFrame"
    
    # Required columns
    required_columns = {"Date", "Ticker", "Adj Close"}
    assert required_columns.issubset(df_prices.columns), f"❌ Missing columns: {required_columns - set(df_prices.columns)}"
