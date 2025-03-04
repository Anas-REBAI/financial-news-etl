import json
import os
from dotenv import load_dotenv

# 🔄 Charger les variables d'environnement depuis .env
load_dotenv()

# 📌 Charger les credentials Google
CLIENT_SECRETS = json.loads(os.getenv("CLIENT_SECRETS_JSON", "{}"))

# 🔑 Charger les clés API pour NewsAPI
NEWS_API_KEY_1 = os.getenv("NEWS_API_KEY_1", "")
NEWS_API_KEY_2 = os.getenv("NEWS_API_KEY_2", "")

# 📡 URL de l'API News
NEWS_API_URL = "https://newsapi.org/v2/everything"

# 📊 Liste des tickers à surveiller
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "JPM", "V",
    "MA", "PYPL", "INTC", "CSCO", "IBM", "ORCL", "AMD", "ADBE", "CRM", "PFE",
    "MRNA", "JNJ", "UNH", "WMT", "COST", "TGT", "KO", "PEP", "MCD", "NKE",
    "XOM", "CVX", "BP", "GS", "MS", "BAC", "DIS", "CMCSA", "ABNB", "UBER",
    "SNAP", "SQ", "SHOP", "RIVN", "LYFT", "PLTR", "BABA", "TSM", "TDOC", "SPOT"
]

# 🔍 Mots-clés financiers pour filtrer les articles
FINANCE_KEYWORDS = ["stock market", "finance", "price", "earnings", "financial", "Nasdaq", "Dow", "S&P", "IPO", "revenue", "forecast", "dividend"]

# ❌ Sources non pertinentes
EXCLUDED_SOURCES = ["espn", "github.com", "toms hardware", "biztoc.com", "abc news"]
