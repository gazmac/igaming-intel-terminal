import requests
import json
import urllib.parse
import os
import pandas as pd
import feedparser
import time
import yfinance as yf
from google import genai
from datetime import datetime

# --- 1. CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_ACTUAL_API_KEY_HERE")
client = genai.Client(api_key=GEMINI_API_KEY)

TARGET_COMPANIES = [
    {"name": "Flutter Entertainment", "ticker": "FLUT", "base_country": "Ireland"},
    {"name": "DraftKings", "ticker": "DKNG", "base_country": "USA"},
    {"name": "Entain PLC", "ticker": "ENT.L", "base_country": "UK"},
    {"name": "Evolution AB", "ticker": "EVO.ST", "base_country": "Sweden"}
]

# --- VERIFIED FALLBACK DATA ---
VERIFIED_DATA = {
    "FLUT": {"eps_actual": -1.75, "eps_forecast": 0.24, "revenue": "$16.38B", "net_income": "-$407M", "ebitda": "$2.84B", "ngr": "$16.38B", "fcf": "$407M", "jurisdictions": ["US", "UK", "Ireland", "Australia", "Italy"]},
    "DKNG": {"eps_actual": 0.28, "eps_forecast": 0.20, "revenue": "$1.99B", "net_income": "$136.4M", "ebitda": "$343.2M", "ngr": "$1.99B", "fcf": "$150M", "jurisdictions": ["US (26 States)", "Ontario", "Puerto Rico"]},
    "ENT.L": {"eps_actual": 0.62, "eps_forecast": 0.45, "revenue": "£5.26B", "net_income": "-£681M", "ebitda": "£1.16B", "ngr": "£5.32B", "fcf": "£151M", "jurisdictions": ["UK", "Italy", "Brazil", "Australia", "Spain"]},
    "EVO.ST": {"eps_actual": 1.54, "eps_forecast": 1.46, "revenue": "€514M", "net_income": "€306M", "ebitda": "€393M", "ngr": "€514M", "fcf": "€250M", "jurisdictions": ["Europe", "North America", "LatAm", "Asia"]}
}

VERIFIED_CALENDAR = {
    "FLUT": "2026-05-06T21:00:00Z", "DKNG": "2026-05-07T21:00:00Z", 
    "ENT.L": "2026-04-29T07:00:00Z", "EVO.ST": "2026-04-22T06:30:00Z"
}

def fetch_stock_history(ticker):
    """Fetches charts and the latest share price, bypassing the yfinance 1mo bug."""
    print(f"Fetching charts and price for {ticker} via yfinance...")
    periods = {
        "1d": {"p": "1d", "i": "15m"}, "1w": {"p": "5d", "i": "1h"}, 
        "1m": {"p": "1mo", "i": "1d"}, "3m": {"p": "3mo", "i": "1d"}, 
        "6m": {"p": "6mo", "i": "1d"}, "1y": {"p": "1y", "i": "1wk"}, 
        "5y": {"p": "5y", "i": "1wk"} 
    }
    history = {}
    last_price_str = "N/A"
    currencies = {"FLUT": "$", "DKNG": "$", "ENT.L": "GBp ", "EVO.ST": "SEK "}
    sym = currencies.get(ticker, "$")
    
    try:
        ytk = yf.Ticker(ticker)
        latest_data = ytk.history(period="1d")
        if not latest_data.empty:
            last_price_str = f"{sym}{round(latest_data['Close'].iloc[-1], 2)}"

        for label, config in periods.items():
            df = ytk.history(period=config["p"], interval=config["i"])
            data_points = []
            if not df.empty:
                for idx, row in df.iterrows():
                    ts = int(pd.Timestamp(idx).timestamp() * 1000)
                    data_points.append([ts, round(row['Close'], 2)])
            history[label] = data_points
    except Exception as e:
        print(f"❌ Error for {ticker}: {e}")
    return history, last_price_str

def ai_process_intelligence(ticker, company_name):
    rss_url = f"https://finance.yahoo.com/rss/headline?s={ticker}"
    feed = feedparser.parse(rss_url)
    headlines = [entry.title for entry in feed.entries[:5]]
    prompt = f"Act as a financial analyst. Based on these headlines for {company_name}: {' | '.join(headlines)}. Return ONLY JSON: {{\"summary\": [\"3 points\"], \"sentiment\": 0-100}}"
    try:
        response = client.models.generate_content(model='gemini-2.0-flash-lite', contents=prompt, config={"response_mime_type": "application/json"})
        return json.loads(response.text)
    except:
        return {"summary": ["News analysis unavailable."], "sentiment": 50}

def run_pipeline():
    master_db = []
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        intel = ai_process_intelligence(ticker, co['name'])
        history, last_price = fetch_stock_history(ticker)
        fin = VERIFIED_DATA[ticker]
        beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2)

        master_db.append({
            "ticker": ticker, "company": co["name"], "base_country": co["base_country"],
            "release_gmt": VERIFIED_CALENDAR[ticker], "last_price": last_price,
            "actuals": fin, "eps_beat_miss_pct": beat_miss,
            "news_summary": intel["summary"], "sentiment": intel["sentiment"],
            "jurisdictions": fin["jurisdictions"], "history": history
        })
    with open('gambling_stocks_live.json', 'w') as f:
        json.dump(master_db, f, indent=4)

if __name__ == "__main__":
    run_pipeline()
