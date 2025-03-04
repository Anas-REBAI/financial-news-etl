import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# Configuration des secrets client pour Google Drive
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

# Charger les clés API pour NewsAPI
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# URL de l'API News
NEWS_API_URL = "https://newsapi.org/v2/everything"

# Liste des tickers à surveiller
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "JPM", "V",
    "MA", "PYPL", "INTC", "CSCO", "IBM", "ORCL", "AMD", "ADBE", "CRM", "PFE",
    "MRNA", "JNJ", "UNH", "WMT", "COST", "TGT", "KO", "PEP", "MCD", "NKE",
    "XOM", "CVX", "BP", "GS", "MS", "BAC", "DIS", "CMCSA", "ABNB", "UBER",
    "SNAP", "SQ", "SHOP", "RIVN", "LYFT", "PLTR", "BABA", "TSM", "TDOC", "SPOT"
]

# Mots-clés financiers pour filtrer les articles
FINANCE_KEYWORDS = ["stock market", "finance", "price", "earnings", "financial", "Nasdaq", "Dow", "S&P", "IPO", "revenue", "forecast", "dividend"]

# Sources non pertinentes
EXCLUDED_SOURCES = ["espn", "github.com", "toms hardware", "biztoc.com", "abc news"]
