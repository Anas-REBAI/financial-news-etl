import pytest
import pandas as pd
import numpy as np
from etl.assets.returns import daily_asset_returns

def test_daily_asset_returns():
    """Test if daily_asset_returns correctly calculates simple and log returns."""
    # Sample data: Prices for 3 days for 2 tickers
    data = {
        "Date": ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-01", "2024-03-02", "2024-03-03"],
        "Ticker": ["AAPL", "AAPL", "AAPL", "TSLA", "TSLA", "TSLA"],
        "Adj Close": [150, 155, 160, 700, 710, 720]
    }
    df_prices = pd.DataFrame(data)
    df_prices["Date"] = pd.to_datetime(df_prices["Date"])

    df_returns = daily_asset_returns(df_prices).value  # Compute returns

    # Ensure required columns exist
    required_columns = {"Date", "Ticker", "Simple Return", "Log Return"}
    assert required_columns.issubset(df_returns.columns), f"❌ Missing columns: {required_columns - set(df_returns.columns)}"

    # ✅ Vérification des rendements simples
    expected_simple_returns = [(155 / 150 - 1) * 100, (160 / 155 - 1) * 100, (710 / 700 - 1) * 100, (720 / 710 - 1) * 100]   
    actual_simple_returns = df_returns["Simple Return"].round(6).tolist()

    for i, (actual, expected) in enumerate(zip(actual_simple_returns, expected_simple_returns)):
        assert actual == pytest.approx(expected, rel=1e-5), f"❌ Erreur Simple Return : Expected {expected:.6f}, got {actual:.6f}"

    # ✅ Vérification des rendements logarithmiques
    expected_log_returns = [round(np.log(155 / 150), 6), round(np.log(160 / 155), 6), round(np.log(710 / 700), 6), round(np.log(720 / 710), 6)]
    actual_log_returns = df_returns["Log Return"].round(6).tolist()

    for i, (actual, expected) in enumerate(zip(actual_log_returns, expected_log_returns)):
        assert actual == pytest.approx(expected, rel=1e-5), f"❌ Erreur Log Return : Expected {expected:.6f}, got {actual:.6f}"
