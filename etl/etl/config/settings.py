import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# List of tickers
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "JPM", "V",
    "MA", "PYPL", "INTC", "CSCO", "IBM", "ORCL", "AMD", "ADBE", "CRM", "PFE",
    "MRNA", "JNJ", "UNH", "WMT", "COST", "TGT", "KO", "PEP", "MCD", "NKE",
    "XOM", "CVX", "BP", "GS", "MS", "BAC", "DIS", "CMCSA", "ABNB", "UBER",
    "SNAP", "SQ", "SHOP", "RIVN", "LYFT", "PLTR", "BABA", "TSM", "TDOC", "SPOT"
]

# Load API key for NewsAPI
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# NewsAPI base URL
NEWS_API_URL = "https://newsapi.org/v2/everything"

# Financial keywords for filtering news articles
FINANCE_KEYWORDS = ["stock market", "finance", "price", "earnings", "financial", "Nasdaq", "Dow", "S&P", "IPO", "revenue", "forecast", "dividend"]

# Excluded news sources
EXCLUDED_SOURCES = ["espn", "github.com", "toms hardware", "biztoc.com", "abc news"]

# Client secret configuration for Google Drive authentication
CLIENT_SECRETS_JSON = {
    "installed": {
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "project_id": os.getenv("GOOGLE_PROJECT_ID"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "redirect_uris": ["http://localhost"]
    }
}
