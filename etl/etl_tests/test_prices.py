from etl.assets.prices import daily_asset_prices

# Test 1 : Vérifier que `daily_asset_prices` retourne bien un DataFrame structuré
def test_daily_asset_prices():
    df_prices = daily_asset_prices().value  # .value pour accéder au DataFrame de l'Output Dagster
    
    # Vérifier que ce n'est pas vide
    assert not df_prices.empty, "❌ daily_asset_prices returned an empty DataFrame"
    
    # Vérifier que les colonnes essentielles sont bien là
    required_columns = {"Date", "Ticker", "Adj Close"}
    assert required_columns.issubset(df_prices.columns), f"❌ Missing columns in daily_asset_prices: {required_columns - set(df_prices.columns)}"