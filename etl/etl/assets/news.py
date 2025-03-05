import requests
import pandas as pd
from dagster import asset, Output, MetadataValue
from datetime import datetime, timedelta
from etl.config.settings import NEWS_API_KEY, NEWS_API_URL, TICKERS, FINANCE_KEYWORDS, EXCLUDED_SOURCES

@asset
def daily_asset_news() -> Output[pd.DataFrame]:
    """
    Fetches recent financial news for each asset using NewsAPI.
    """

    # Check if the API key is set
    if not NEWS_API_KEY:
        raise ValueError("❌ Missing API key! Please set NEWS_API_KEY.")

    articles_list = []  # List to store the extracted news articles

    # Define the date range for news retrieval (last 7 days)
    today = datetime.today().strftime('%Y-%m-%d')
    from_date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')

    print(f"📅 Fetching news from {from_date} to {today}...")

    for ticker in TICKERS:
        # Construct a search query for each ticker
        query = f"{ticker} stock OR {ticker} stock price OR {ticker} earnings OR {ticker} stock market OR {ticker} stock news"
        url = f"{NEWS_API_URL}?q={query}&from={from_date}&to={today}&sortBy=publishedAt&apiKey={NEWS_API_KEY}"

        try:
            # Send a GET request to the NewsAPI
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for HTTP issues
            data = response.json()

            # Check if the API response is successful
            if data["status"] == "ok":
                for article in data["articles"][:5]:  # Limit to 5 articles per ticker
                    title = article["title"].lower()
                    source = article["source"]["name"].lower()

                    # Filter articles based on relevant keywords and exclude unwanted sources
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
            print(f"❌ Network error fetching news for {ticker}: {e}")

    # Convert collected articles to DataFrame
    df_news = pd.DataFrame(articles_list)

    # Check if any news articles were found
    if df_news.empty:
        print("⚠️ No relevant financial news found.")
        return Output(pd.DataFrame(), metadata={"status": "No relevant news found"})

    # Convert the date column to a datetime object and sort the articles by date (most recent first)
    df_news["Date"] = pd.to_datetime(df_news["Date"])
    df_news = df_news.sort_values(by="Date", ascending=False)

    print(f"✅ {len(df_news)} financial news articles retrieved.")

    return Output(
        df_news,
        metadata={
            "tickers_with_news": df_news["Ticker"].nunique(),
            "news_articles_retrieved": df_news.shape[0],
            "preview": MetadataValue.md(df_news.to_markdown()),
        }
    )
