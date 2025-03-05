import pytest
import pandas as pd
from etl.assets.news import daily_asset_news

def test_daily_asset_news():
    """Test if daily_asset_news returns a non-empty DataFrame with valid columns."""

    # Execute the function and extract the DataFrame from the Output object
    df_news = daily_asset_news().value

    # Check that the DataFrame is not empty
    assert not df_news.empty, "❌ daily_asset_news returned an empty DataFrame"
    
    # Define the required column names
    required_columns = {"Date", "Ticker", "Title", "Source", "URL"}
    
    # Ensure the DataFrame contains all required columns
    assert required_columns.issubset(df_news.columns), f"❌ Missing columns: {required_columns - set(df_news.columns)}"
