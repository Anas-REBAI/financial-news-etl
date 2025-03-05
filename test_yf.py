import pytest
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from etl.etl.assets.returns import daily_asset_returns
from etl.etl.assets.pdf_generation import generate_market_recap_pdf

# 📂 Définition d'un dossier temporaire pour les fichiers de sortie
TEMP_DIR = "tests_output"
os.makedirs(TEMP_DIR, exist_ok=True)

# ✅ 1️⃣ Test des calculs de rendements journaliers
def test_daily_asset_returns():
    # 📊 Jeu de données simulé
    test_data = pd.DataFrame({
        "Date": [datetime.today() - timedelta(days=i) for i in reversed(range(3))],  
        "Ticker": ["AAPL", "AAPL", "AAPL"],
        "Adj Close": [150.0, 155.0, 160.0]  
    })

    # ✅ Trier les données AVANT le calcul
    test_data = test_data.sort_values(by=["Ticker", "Date"], ascending=True).reset_index(drop=True)

    # 📌 Exécuter la fonction de calcul des rendements
    output = daily_asset_returns(test_data)
    print(output.value[["Date", "Ticker", "Adj Close", "Simple Return"]])

    # 📌 Vérification du calcul des rendements (déjà en %)
    expected_simple_return_1 = ((155 - 150) / 150) * 100  # Rendement 150 → 155
    expected_simple_return_2 = ((160 - 155) / 155) * 100  # Rendement 155 → 160

    assert round(output.value["Simple Return"].iloc[0], 2) == round(expected_simple_return_1, 2)
    assert round(output.value["Simple Return"].iloc[1], 2) == round(expected_simple_return_2, 2)

    print("✅ Test des rendements journaliers réussi.")

# ✅ 2️⃣ Test du chargement des données (extraction de prix et news)
def test_data_loading():
    # 📊 Données simulées pour tester le pipeline
    test_prices = pd.DataFrame({
        "Date": [datetime.today() - timedelta(days=1)],
        "Ticker": ["AAPL"],
        "Adj Close": [150.0]
    })

    test_news = pd.DataFrame({
        "Date": [datetime.today() - timedelta(days=1)],
        "Ticker": ["AAPL"],
        "Title": ["Apple stock rises after earnings report"],
        "Source": ["Bloomberg"],
        "URL": ["https://www.example.com"]
    })

    # 📌 Vérifier que les données sont bien chargées
    assert not test_prices.empty
    assert not test_news.empty
    assert "Adj Close" in test_prices.columns
    assert "Title" in test_news.columns
    print("✅ Test du chargement des données réussi.")

# ✅ 3️⃣ Test de la génération du PDF
def test_pdf_generation():
    # 📊 Données simulées
    test_prices = pd.DataFrame({
        "Date": [datetime.today() - timedelta(days=1)],
        "Ticker": ["AAPL"],
        "Adj Close": [150.0]
    })

    test_returns = pd.DataFrame({
        "Date": [datetime.today() - timedelta(days=1)],
        "Ticker": ["AAPL"],
        "Adj Close": [150.0],
        "Simple Return": [0.05],
        "Log Return": [np.log(1.05)]
    })

    test_news = pd.DataFrame({
        "Date": [datetime.today() - timedelta(days=1)],
        "Ticker": ["AAPL"],
        "Title": ["Apple stock rises after earnings report"],
        "Source": ["Bloomberg"],
        "URL": ["https://www.example.com"]
    })

    # 📌 Générer un PDF de test
    output = generate_market_recap_pdf(test_news, test_prices, test_returns)
    
    # 📌 Vérifier que le fichier PDF a bien été créé
    assert os.path.exists(output.value)
    print("✅ Test de la génération du PDF réussi.")

# 🔹 Exécuter les tests avec pytest si ce fichier est lancé directement
if __name__ == "_main_":
    pytest.main()