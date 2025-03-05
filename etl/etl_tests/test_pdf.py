import pytest
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from etl.assets.pdf_generation import generate_market_recap_pdf

def test_generate_market_recap_pdf():
    """Test if generate_market_recap_pdf successfully creates a PDF file."""
    # Sample dummy data
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

    pdf_path = generate_market_recap_pdf(test_news, test_prices, test_returns).value

    assert os.path.exists(pdf_path), f"❌ PDF was not generated at {pdf_path}"
