import numpy as np
import pandas as pd
from dagster import asset, Output, MetadataValue

@asset
def daily_asset_returns(daily_asset_prices: pd.DataFrame) -> Output[pd.DataFrame]:
    """
    Calcule les rendements journaliers simples et logarithmiques pour chaque actif.
    """

    if daily_asset_prices.empty:
        print("⚠️ Erreur : Les données de prix sont vides. Retourne un DataFrame vide.")
        return Output(pd.DataFrame(), metadata={"status": "Données vides"})

    print("✅ Colonnes disponibles dans daily_asset_prices:", daily_asset_prices.columns)

    # Vérifier qu'on a plusieurs dates
    unique_dates = daily_asset_prices["Date"].nunique()
    print(f"📅 Nombre de dates uniques : {unique_dates}")

    if unique_dates < 2:
        print("❌ Pas assez de dates pour calculer les rendements. Essayez d'augmenter period dans yfinance.")
        return Output(pd.DataFrame(), metadata={"status": "Pas assez de dates"})

    # Conversion de Date en datetime
    daily_asset_prices["Date"] = pd.to_datetime(daily_asset_prices["Date"])

    # Trier les données
    daily_asset_prices = daily_asset_prices.sort_values(by=["Ticker", "Date"]).copy()

    # Calcul des rendements
    daily_asset_prices["Simple Return"] = daily_asset_prices.groupby("Ticker")["Adj Close"].pct_change()
    daily_asset_prices["Log Return"] = daily_asset_prices.groupby("Ticker")["Adj Close"].transform(lambda x: np.log(x / x.shift(1)))

    # Affichage avant suppression des NaN
    print("📊 Aperçu des rendements AVANT suppression des NaN :")
    print(daily_asset_prices[["Date", "Ticker", "Simple Return", "Log Return"]].head(10))

    # Supprimer les lignes NaN après le calcul
    daily_asset_returns = daily_asset_prices.dropna().reset_index(drop=True)

    print(f"✅ Calcul des rendements terminé ({len(daily_asset_returns)} lignes après suppression des NaN).")
    
    return Output(
        daily_asset_returns,
        metadata={
            "tickers_calculés": len(daily_asset_returns["Ticker"].unique()),
            "nombre_lignes": daily_asset_returns.shape[0],
            "aperçu": MetadataValue.md(daily_asset_returns.head().to_markdown()),  # Affichage top 5 dans Dagster UI
        }
    )