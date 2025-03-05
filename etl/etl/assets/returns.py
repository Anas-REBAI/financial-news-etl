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

    # Trier les données *dans le bon ordre chronologique*
    daily_asset_prices = daily_asset_prices.sort_values(by=["Ticker", "Date"], ascending=True).copy()

    # Calcul du rendement simple en POURCENTAGE
    daily_asset_prices["Simple Return"] = (daily_asset_prices.groupby("Ticker")["Adj Close"].pct_change()) * 100

    # Calcul du rendement logarithmique
    daily_asset_prices["Log Return"] = daily_asset_prices.groupby("Ticker")["Adj Close"].transform(lambda x: np.log(x / x.shift(1)))

    # Vérification des rendements avant suppression des NaN
    print("📊 Aperçu des rendements AVANT suppression des NaN :")
    print(daily_asset_prices[["Date", "Ticker", "Simple Return", "Log Return"]].head(10))

    # Supprimer les NaN (première ligne de chaque Ticker)
    daily_asset_returns = daily_asset_prices.dropna().reset_index(drop=True)

    print(f"✅ Calcul des rendements terminé ({len(daily_asset_returns)} lignes après suppression des NaN).")

    return Output(
        daily_asset_returns,
        metadata={
            "tickers_calculés": len(daily_asset_returns["Ticker"].unique()),
            "nombre_lignes": daily_asset_returns.shape[0],
            "aperçu": MetadataValue.md(daily_asset_returns.head().to_markdown()),  
        }
    )
