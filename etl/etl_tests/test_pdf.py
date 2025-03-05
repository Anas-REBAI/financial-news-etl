import pytest
import os
import pandas as pd
from etl.assets.pdf_generation import generate_market_recap_pdf

def test_generate_market_recap_pdf():
    """Test if generate_market_recap_pdf successfully creates a PDF file."""
    # Sample dummy data
    df_prices = pd.DataFrame({"Date": ["2024-03-01"], "Ticker": ["AAPL"], "Adj Close": [150]})
    df_returns = pd.DataFrame({"Date": ["2024-03-01"], "Ticker": ["AAPL"], "Simple Return": [0.05], "Log Return": [0.04879]})
    df_news = pd.DataFrame({"Date": ["2024-03-01"], "Ticker": ["AAPL"], "Title": ["Stock surges"], "Source": ["NewsAPI"], "URL": ["https://example.com"]})

    pdf_path = generate_market_recap_pdf(df_news, df_prices, df_returns).value

    assert os.path.exists(pdf_path), f"❌ PDF was not generated at {pdf_path}"
