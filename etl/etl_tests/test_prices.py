import pytest
import pandas as pd
from etl.assets.prices import daily_asset_prices

def test_daily_asset_prices():
    """Test if daily_asset_prices returns a structured DataFrame."""

    # Execute the function and extract the DataFrame from the Dagster Output object
    df_prices = daily_asset_prices().value
    
    # Ensure the DataFrame is not empty
    assert not df_prices.empty, "❌ daily_asset_prices returned an empty DataFrame"
    
    # Define the required columns
    required_columns = {"Date", "Ticker", "Adj Close"}

    # Check that all required columns are present
    assert required_columns.issubset(df_prices.columns), f"❌ Missing columns: {required_columns - set(df_prices.columns)}"
