from dagster import asset
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF

@asset
def daily_market_recap_pdf(daily_asset_prices: pd.DataFrame, 
                           daily_asset_returns: pd.DataFrame, 
                           daily_asset_news: pd.DataFrame) -> str:
    """
    Génère un PDF contenant le récapitulatif du marché avec les prix, rendements et news du jour.
    """
    pdf_filename = "daily_market_recap.pdf"

    # 📌 1. Création du PDF
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(200, 10, "Daily Market Recap", ln=True, align="C")
    pdf.ln(10)

    # 📌 2. Ajouter le tableau des prix et rendements
    pdf.set_font("Arial", "B", 12)
    pdf.cell(200, 10, "Prices & Returns", ln=True, align="L")
    pdf.ln(5)

    table_data = daily_asset_prices.iloc[:5]  # On prend 5 valeurs pour simplifier
    for index, row in table_data.iterrows():
        pdf.set_font("Arial", "", 10)
        pdf.cell(200, 10, f"{row['Date']} - {row.name}: {row.iloc[1]:.2f} USD", ln=True)

    pdf.ln(10)

    # 📌 3. Ajouter le graphique des top 5 rendements
    top_returns = daily_asset_returns.sort_values(by=daily_asset_returns.columns[1], ascending=False).iloc[:5]
    plt.figure(figsize=(6, 4))
    plt.bar(top_returns.iloc[:, 0], top_returns.iloc[:, 1], color="green")
    plt.xlabel("Asset")
    plt.ylabel("Daily Return")
    plt.title("Top 5 Returns of the Day")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("top_returns.png")
    plt.close()
    
    pdf.image("top_returns.png", x=10, y=pdf.get_y(), w=180)
    pdf.ln(50)

    # 📌 4. Ajouter les news du jour
    pdf.set_font("Arial", "B", 12)
    pdf.cell(200, 10, "Market News", ln=True, align="L")
    pdf.ln(5)

    news_data = daily_asset_news.iloc[:5]  # Prendre les 5 premières news
    for index, row in news_data.iterrows():
        pdf.set_font("Arial", "B", 10)
        pdf.multi_cell(0, 5, f"{row['title']} ({row['source']})")
        pdf.set_font("Arial", "", 9)
        pdf.multi_cell(0, 5, f"Published: {row['publishedAt']}")
        pdf.multi_cell(0, 5, f"Link: {row['url']}")
        pdf.ln(5)

    # 📌 5. Sauvegarder le PDF
    pdf.output(pdf_filename)
    print(f"✅ PDF généré : {pdf_filename}")
    
    return pdf_filename
