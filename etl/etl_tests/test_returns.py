import pandas as pd
from etl.assets.returns import daily_asset_returns

# Test 2 : Vérifier que `daily_asset_returns` calcule bien les rendements
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

    # Vérifier le calcul du rendement simple
    expected_simple_returns = [(155 / 150 - 1), (160 / 155 - 1), (710 / 700 - 1), (720 / 710 - 1)]
    assert all(df_returns["Simple Return"].round(6).tolist() == expected_simple_returns), "❌ Incorrect simple return calculation"

    # Vérifier le calcul du rendement logarithmique
    import numpy as np
    expected_log_returns = [np.log(155 / 150), np.log(160 / 155), np.log(710 / 700), np.log(720 / 710)]
    assert all(df_returns["Log Return"].round(6).tolist() == expected_log_returns), "❌ Incorrect log return calculation"