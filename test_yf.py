import yfinance as yf

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "JPM", "V",
    "MA", "PYPL", "INTC", "CSCO", "IBM", "ORCL", "AMD", "ADBE", "CRM", "PFE",
    "MRNA", "JNJ", "UNH", "WMT", "COST", "TGT", "KO", "PEP", "MCD", "NKE",
    "XOM", "CVX", "BP", "GS", "MS", "BAC", "DIS", "CMCSA", "ABNB", "UBER",
    "SNAP", "SQ", "SHOP", "RIVN", "LYFT", "PLTR", "BABA", "TSM", "TDOC", "SPOT"
]

# Télécharger les données avec auto_adjust désactivé
data = yf.download(TICKERS, period="1d", interval="1d", auto_adjust=False)

# Afficher les données
print(data.head())

# Vérifier si la colonne "Adj Close" est bien présente
if "Adj Close" in data.columns:
    print(f"✅ Données valides pour {TICKERS}")
else:
    print(f"⚠️ Pas de 'Adj Close' pour {TICKERS}, vérifie les colonnes disponibles : {data.columns}")
