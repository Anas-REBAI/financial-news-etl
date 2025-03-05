import pytest
import pandas as pd
import numpy as np
from etl.assets.returns import daily_asset_returns

def test_daily_asset_returns():
    # Données de test (prix de 3 jours pour 2 tickers)
    data = {
        "Date": ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-01", "2024-03-02", "2024-03-03"],
        "Ticker": ["AAPL", "AAPL", "AAPL", "TSLA", "TSLA", "TSLA"],
        "Adj Close": [150, 155, 160, 700, 710, 720]
    }
    df_prices = pd.DataFrame(data)

    # Convertir Date en datetime
    df_prices["Date"] = pd.to_datetime(df_prices["Date"])

    df_returns = daily_asset_returns(df_prices).value  # Calculer les rendements

    # Vérifier que les colonnes existent
    required_columns = {"Date", "Ticker", "Simple Return", "Log Return"}
    assert required_columns.issubset(df_returns.columns), f"❌ Missing columns in daily_asset_returns: {required_columns - set(df_returns.columns)}"

    # Vérifier le calcul du rendement simple (convertir en pourcentage)
    expected_simple_returns = [(155 / 150 - 1) * 100, (160 / 155 - 1) * 100, (710 / 700 - 1) * 100, (720 / 710 - 1) * 100]
    assert all(a == b for a, b in zip(df_returns["Simple Return"].round(6).tolist(), expected_simple_returns)), "❌ Incorrect simple return calculation"

    # Vérifier le calcul du rendement logarithmique
    expected_log_returns = [round(np.log(155 / 150), 6), round(np.log(160 / 155), 6), round(np.log(710 / 700), 6), round(np.log(720 / 710), 6)]
    assert all(a == b for a, b in zip(df_returns["Log Return"].round(6).tolist(), expected_log_returns)), "❌ Incorrect log return calculation"

