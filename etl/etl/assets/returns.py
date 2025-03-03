from dagster import asset
import pandas as pd
import numpy as np

@asset
def daily_asset_returns(daily_asset_prices: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les rendements journaliers simples et logarithmiques pour chaque actif.
    """
    if daily_asset_prices.empty:
        print("⚠️ Erreur : Les données de prix sont vides. Retourne un DataFrame vide.")
        return pd.DataFrame()

    print("✅ Colonnes disponibles dans daily_asset_prices:", daily_asset_prices.columns)

    # Reformater le DataFrame en supprimant "Ticker"
    prices = daily_asset_prices.set_index("Date").drop(columns=["Ticker"], errors="ignore")

    # Vérifier qu'on a assez de données
    if len(prices) < 2:
        print("⚠️ Pas assez de données pour calculer les rendements.")
        return pd.DataFrame()

    # Vérification des données
    print("📊 Aperçu des données de prix :\n", prices.head())

    # Calcul du rendement simple
    simple_returns = prices.pct_change().reset_index()
    simple_returns["Return Type"] = "Simple"

    # Calcul du rendement logarithmique
    log_returns = np.log(prices / prices.shift(1)).reset_index()
    log_returns["Return Type"] = "Logarithmic"

    # Concaténer les deux types de rendement
    returns_df = pd.concat([simple_returns, log_returns])

    print(f"✅ Calcul des rendements terminé ({len(returns_df)} lignes pour {len(prices.columns)} tickers).")

    return returns_df
