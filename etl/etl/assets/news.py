from dagster import asset
import requests
import pandas as pd
from etl.config.settings import TICKERS, NEWS_API_KEY, NEWS_API_URL

@asset
def daily_asset_news() -> pd.DataFrame:
    """
    Récupère les nouvelles du jour pour les actifs via l'API NewsAPI en regroupant les tickers par lot de 10.
    """
    all_news = []
    total_articles = 0  # Compteur

    # Découper les tickers en groupes de 10
    for i in range(0, len(TICKERS), 10):
        tickers_group = TICKERS[i:i+10]  # Prendre 10 tickers à la fois
        params = {
            "q": " OR ".join(tickers_group),  # Requête avec plusieurs tickers
            "apiKey": NEWS_API_KEY,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 50
        }
        response = requests.get(NEWS_API_URL, params=params)

        if response.status_code == 200:
            articles = response.json().get("articles", [])
            total_articles += len(articles)  # Ajouter au compteur
            for article in articles:
                all_news.append({
                    "tickers": ", ".join(tickers_group),  # Associer les tickers du groupe
                    "title": article["title"],
                    "source": article["source"]["name"],
                    "publishedAt": article["publishedAt"],
                    "url": article["url"]
                })
        else:
            print(f"❌ Erreur {response.status_code} pour les tickers {tickers_group}")

    print(f"✅ {total_articles} articles récupérés pour {len(TICKERS)} tickers")
    return pd.DataFrame(all_news)
