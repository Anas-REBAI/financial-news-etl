import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from dagster import asset, Output
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
from datetime import datetime, timedelta
import textwrap
from etl.config.settings import CLIENT_SECRETS_JSON
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Output directory for the PDF
OUTPUT_DIR = "output"
PDF_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "market_recap.pdf")
GRAPH_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "top_performers.png")

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

@asset
def generate_market_recap_pdf(daily_asset_news: pd.DataFrame, daily_asset_prices: pd.DataFrame, daily_asset_returns: pd.DataFrame) -> Output[str]:
    """
    Generates a structured PDF containing:
    1️⃣ A table of prices and returns for ALL assets.
    2️⃣ A chart of the top 5 best-performing assets.
    3️⃣ A full list of financial news (with handling for long lines).
    """

    # Data validation
    if daily_asset_news.empty or daily_asset_prices.empty or daily_asset_returns.empty:
        print("⚠️ No available data to generate the PDF.")
        return Output(None, metadata={"status": "No available data"})

    print("📄 Generating PDF report...")

    # Filter data for the previous day
    yesterday = datetime.today() - timedelta(days=1)
    daily_asset_news["Date"] = pd.to_datetime(daily_asset_news["Date"]).dt.tz_localize(None)
    daily_asset_returns["Date"] = pd.to_datetime(daily_asset_returns["Date"]).dt.tz_localize(None)

    daily_asset_news = daily_asset_news[daily_asset_news["Date"].dt.date == yesterday.date()]
    daily_asset_returns = daily_asset_returns[daily_asset_returns["Date"].dt.date == yesterday.date()]

    # Remove duplicates
    daily_asset_returns = daily_asset_returns.drop_duplicates(subset=["Ticker"], keep="last")

    # Create the PDF
    pdf_canvas = canvas.Canvas(PDF_OUTPUT_PATH, pagesize=letter)
    pdf_canvas.setTitle("Market Recap Report")

    # Page 1 - Prices and returns table (DISPLAYED IN 2 COLUMNS)
    pdf_canvas.setFont("Helvetica-Bold", 18)
    pdf_canvas.drawString(200, 750, "📊 Daily Market Recap")

    pdf_canvas.setFont("Helvetica-Bold", 14)
    pdf_canvas.drawString(50, 720, "📈 Daily Prices & Returns (All Assets):")

    # Split the table into 2 columns
    mid_index = len(daily_asset_returns) // 2
    left_data = [["Ticker", "Adj Close", "Simple Return (%)"]] + \
                daily_asset_returns.iloc[:mid_index][["Ticker", "Adj Close", "Simple Return"]].round(4).values.tolist()

    right_data = [["Ticker", "Adj Close", "Simple Return (%)"]] + \
                 daily_asset_returns.iloc[mid_index:][["Ticker", "Adj Close", "Simple Return"]].round(4).values.tolist()

    left_table = Table(left_data, colWidths=[80, 90, 90])
    right_table = Table(right_data, colWidths=[80, 90, 90])

    table_style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.darkblue),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
        ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
        ("GRID", (0, 0), (-1, -1), 1, colors.black)
    ])

    left_table.setStyle(table_style)
    right_table.setStyle(table_style)

    # Position the 2 table columns
    left_table.wrapOn(pdf_canvas, 250, 500)
    right_table.wrapOn(pdf_canvas, 250, 500)

    left_table.drawOn(pdf_canvas, 50, 200) 
    right_table.drawOn(pdf_canvas, 320, 200)

    pdf_canvas.showPage()  # New page for the chart

    # Page 2 - Top 5 performers chart
    pdf_canvas.setFont("Helvetica-Bold", 18)
    pdf_canvas.drawString(180, 750, "📈 Top 5 Performers of the Day")

    # *Top 5 performance chart*
    top_returns = daily_asset_returns.sort_values(by="Simple Return", ascending=False).head(5)

    if not top_returns.empty:
        plt.figure(figsize=(6, 4))
        plt.bar(top_returns["Ticker"], top_returns["Simple Return"], color="green")
        plt.xlabel("Ticker")
        plt.ylabel("Daily Return (%)")
        plt.title("Top 5 Performers of the Day")
        plt.savefig(GRAPH_OUTPUT_PATH)
        plt.close()

        # Add the chart to the PDF
        img = ImageReader(GRAPH_OUTPUT_PATH)
        pdf_canvas.drawImage(img, 100, 300, width=400, height=250)

    pdf_canvas.showPage()  # *New page for news*

    # *Page 3 - Financial news*
    pdf_canvas.setFont("Helvetica-Bold", 18)
    pdf_canvas.drawString(180, 750, "📰 Key News of the Day")

    pdf_canvas.setFont("Helvetica", 11)
    y_position = 720
    for index, row in daily_asset_news.iterrows():
        text = f"• {row['Title']} ({row['Source']})"
        wrapped_text = textwrap.wrap(text, width=80)  # *Automatic line wrapping*
        
        for line in wrapped_text:
            pdf_canvas.drawString(50, y_position, line)
            y_position -= 15  # *Vertical spacing for each line*

        y_position -= 10  # *Spacing between articles*

        if y_position < 50:  # *New page if too much text*
            pdf_canvas.showPage()
            pdf_canvas.setFont("Helvetica", 11)
            y_position = 750

    # *Footer*
    pdf_canvas.setFont("Helvetica", 10)
    pdf_canvas.drawString(50, 30, f"Generated on {datetime.today().strftime('%Y-%m-%d')} | © Market Data Inc.")

    # Finalize and save the PDF
    pdf_canvas.save()
    print(f"✅ PDF report successfully generated: {PDF_OUTPUT_PATH}")

    # Upload PDF to Google Drive
    try:
        # Create a temporary JSON file with secrets
        with open("client_secrets.json", "w") as f:
            json.dump(CLIENT_SECRETS_JSON, f)

        # Google Drive authentication
        gauth = GoogleAuth()
        gauth.LoadClientConfigFile("client_secrets.json")
        gauth.LocalWebserverAuth()
        drive = GoogleDrive(gauth)

        # Upload the PDF
        file_drive = drive.CreateFile({'title': os.path.basename(PDF_OUTPUT_PATH)})
        file_drive.SetContentFile(PDF_OUTPUT_PATH)
        file_drive.Upload()

        print(f"✅ PDF successfully uploaded to Google Drive: {file_drive['alternateLink']}")

        # Remove temporary JSON file
        os.remove("client_secrets.json")

        return Output(
            PDF_OUTPUT_PATH,
            metadata={
                "local_file_path": PDF_OUTPUT_PATH,
                "google_drive_link": file_drive['alternateLink'],
                "status": "✅ PDF generated and successfully uploaded"
            }
        )

    except Exception as e:
        print(f"❌ Error uploading to Google Drive: {e}")
        if os.path.exists("client_secrets.json"):
            os.remove("client_secrets.json")

        return Output(
            PDF_OUTPUT_PATH,
            metadata={
                "file_path": PDF_OUTPUT_PATH,
                "status": "❌ PDF generated but failed to upload to Google Drive"
            }
        )
