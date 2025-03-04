import requests
import pandas as pd
from dagster import asset, Output, MetadataValue
from datetime import datetime, timedelta
from etl.config.settings import NEWS_API_KEY, NEWS_API_URL, TICKERS, FINANCE_KEYWORDS, EXCLUDED_SOURCES

@asset
def daily_asset_news() -> Output[pd.DataFrame]:
    """
    Récupère les actualités financières récentes pour chaque actif à l'aide de NewsAPI.
    """
    if not NEWS_API_KEY:
        raise ValueError("❌ Clé API manquante ! Ajoutez votre clé NewsAPI.")

    articles_list = []
    today = datetime.today().strftime('%Y-%m-%d')
    from_date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')

    print(f"📅 Récupération des actualités entre {from_date} et {today}...")

    for ticker in TICKERS:
        query = f"{ticker} stock OR {ticker} stock price OR {ticker} earnings OR {ticker} stock market OR {ticker} stock news"
        url = f"{NEWS_API_URL}?q={query}&from={from_date}&to={today}&sortBy=publishedAt&apiKey={NEWS_API_KEY}"

        try:
            response = requests.get(url)
            data = response.json()

            if data["status"] == "ok":
                for article in data["articles"][:5]:  # On prend max 5 articles par ticker
                    title = article["title"].lower()
                    source = article["source"]["name"].lower()

                    # Vérification des mots-clés financiers et exclusion des sources non pertinentes
                    if any(keyword in title for keyword in FINANCE_KEYWORDS) and source not in EXCLUDED_SOURCES:
                        articles_list.append({
                            "Date": article["publishedAt"],
                            "Ticker": ticker,
                            "Title": article["title"],
                            "Source": article["source"]["name"],
                            "URL": article["url"]
                        })

            else:
                print(f"⚠️ Erreur API pour {ticker} : {data}")

        except Exception as e:
            print(f"❌ Erreur lors de la récupération des news pour {ticker} : {e}")

    # Convertir en DataFrame
    df_news = pd.DataFrame(articles_list)

    if df_news.empty:
        print("⚠️ Aucune actualité financière récupérée.")
        return Output(pd.DataFrame(), metadata={"status": "Aucune actualité pertinente trouvée"})

    # Trier les actualités par date
    df_news["Date"] = pd.to_datetime(df_news["Date"])
    df_news = df_news.sort_values(by="Date", ascending=False)

    print(f"✅ {len(df_news)} articles financiers récupérés.")

    return Output(
        df_news,
        metadata={
            "tickers_avec_news": df_news["Ticker"].nunique(),
            "articles_financiers_récupérés": df_news.shape[0],
            "aperçu": MetadataValue.md(df_news.head().to_markdown()),  # Afficher un aperçu des news dans Dagster UI
        }
    )
