import pytest
import pandas as pd
from etl.assets.news import daily_asset_news

def test_daily_asset_news():
    """Test if daily_asset_news returns a non-empty DataFrame with valid columns."""
    df_news = daily_asset_news().value

    assert not df_news.empty, "❌ daily_asset_news returned an empty DataFrame"
    
    required_columns = {"Date", "Ticker", "Title", "Source", "URL"}
    assert required_columns.issubset(df_news.columns), f"❌ Missing columns: {required_columns - set(df_news.columns)}"
