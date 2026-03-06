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
    {"name": "Evolution AB", "ticker": "EVO.ST", "base_country": "Sweden"},
    {"name": "MGM Resorts", "ticker": "MGM", "base_country": "USA"},
    {"name": "Caesars Entertainment", "ticker": "CZR", "base_country": "USA"},
    {"name": "Penn Entertainment", "ticker": "PENN", "base_country": "USA"},
    {"name": "Las Vegas Sands", "ticker": "LVS", "base_country": "USA"},
    {"name": "Wynn Resorts", "ticker": "WYNN", "base_country": "USA"},
    {"name": "Evoke plc", "ticker": "EVOK.L", "base_country": "UK"},
    {"name": "Kindred Group", "ticker": "KIND-SDB.ST", "base_country": "Malta"},
    {"name": "Betsson AB", "ticker": "BETS-B.ST", "base_country": "Sweden"},
    {"name": "Playtech", "ticker": "PTEC.L", "base_country": "UK"},
    {"name": "Churchill Downs", "ticker": "CHDN", "base_country": "USA"},
    {"name": "Light & Wonder", "ticker": "LNW", "base_country": "USA"},
    {"name": "Aristocrat Leisure", "ticker": "ALL.AX", "base_country": "Australia"},
    {"name": "Super Group", "ticker": "SGHC", "base_country": "Guernsey"},
    {"name": "Rush Street Interactive", "ticker": "RSI", "base_country": "USA"},
    {"name": "Bragg Gaming Group", "ticker": "BRAG", "base_country": "Canada"},
    {"name": "Kambi Group", "ticker": "KAMBI.ST", "base_country": "Malta"}
]

# Map EU/Aussie tickers to US OTC equivalents to bypass Yahoo's GDPR blocks
OTC_MAP = {
    "ENT.L": "GMVHF",
    "EVO.ST": "EVVTY",
    "EVOK.L": "EIHDF",
    "KIND-SDB.ST": "KNDGF",
    "BETS-B.ST": "BTSBF",
    "PTEC.L": "PYTCF",
    "ALL.AX": "ARLUF",
    "KAMBI.ST": "KMBIF"
}

# --- VERIFIED FALLBACK DATA (For UI stability) ---
VERIFIED_DATA = {
    "FLUT": {"eps_actual": -1.75, "eps_forecast": 0.24, "revenue": "$16.38B", "net_income": "-$407M", "ebitda": "$2.84B", "ngr": "$16.38B", "fcf": "$407M", "jurisdictions": ["US", "UK", "Ireland", "Australia", "Italy"]},
    "DKNG": {"eps_actual": 0.28, "eps_forecast": 0.20, "revenue": "$1.99B", "net_income": "$136.4M", "ebitda": "$343.2M", "ngr": "$1.99B", "fcf": "$150M", "jurisdictions": ["US", "Ontario", "Puerto Rico"]},
    "ENT.L": {"eps_actual": 0.62, "eps_forecast": 0.45, "revenue": "£5.26B", "net_income": "-£681M", "ebitda": "£1.16B", "ngr": "£5.32B", "fcf": "£151M", "jurisdictions": ["UK", "Italy", "Brazil", "Australia", "Spain"]},
    "EVO.ST": {"eps_actual": 1.54, "eps_forecast": 1.46, "revenue": "€514M", "net_income": "€306M", "ebitda": "€393M", "ngr": "€514M", "fcf": "€250M", "jurisdictions": ["Europe", "North America", "LatAm", "Asia"]}
}

VERIFIED_CALENDAR = {
    "FLUT": "2026-05-06T21:00:00Z", "DKNG": "2026-05-07T21:00:00Z", "ENT.L": "2026-04-29T07:00:00Z", "EVO.ST": "2026-04-22T06:30:00Z"
}

def fetch_stock_history(ticker):
    """Fetches charts by pivoting to US OTC markets to bypass European blocks."""
    print(f"Fetching charts and price for {ticker}...")
    is_otc = ticker in OTC_MAP
    fetch_ticker = OTC_MAP.get(ticker, ticker)
    
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    
    history = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}
    last_price_str = "N/A"
    
    # Currency symbols: If we pivot to OTC, it's always USD ($)
    sym = "$" if is_otc else ("GBp " if ".L" in ticker else ("SEK " if ".ST" in ticker else "$"))
    
    try:
        ytk = yf.Ticker(fetch_ticker, session=session)
        df_1d = ytk.history(period="1d", interval="15m")
        if not df_1d.empty:
            last_price_str = f"{sym}{round(df_1d['Close'].iloc[-1], 2)}"
            history["1d"] = [[int(pd.Timestamp(idx).timestamp() * 1000), round(row['Close'], 2)] for idx, row in df_1d.iterrows()]

        df_5y = ytk.history(period="5y", interval="1d")
        if not df_5y.empty:
            df_5y.index = df_5y.index.tz_localize(None)
            def slice_data(days):
                cutoff = df_5y.index[-1] - pd.Timedelta(days=days)
                sliced = df_5y[df_5y.index >= cutoff]
                return [[int(pd.Timestamp(idx).timestamp() * 1000), round(row['Close'], 2)] for idx, row in sliced.iterrows()]
            
            history["1w"], history["1m"], history["3m"] = slice_data(7), slice_data(30), slice_data(90)
            history["6m"], history["1y"] = slice_data(180), slice_data(365)
            df_5y_weekly = df_5y.resample('W').last().dropna()
            history["5y"] = [[int(pd.Timestamp(idx).timestamp() * 1000), round(row['Close'], 2)] for idx, row in df_5y_weekly.iterrows()]
            
    except Exception as e:
        print(f"❌ Error fetching {fetch_ticker}: {e}")
        
    return history, last_price_str

def ai_process_intelligence(company_name):
    """Fetches Google News and forces clean JSON out of the AI."""
    print(f"Analyzing News for {company_name}...")
    query = urllib.parse.quote(f'"{company_name}" stock OR gambling')
    google_rss = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(google_rss)
    headlines = [entry.title for entry in feed.entries[:6]]
    
    if not headlines:
        return {"summary": ["No news found."], "sentiment": 50}

    prompt = f"Act as an iGaming financial analyst. Based on these headlines for {company_name}: {' | '.join(headlines)}. Return ONLY JSON: {{\"summary\": [\"Point 1\", \"Point 2\", \"Point 3\"], \"sentiment\": 75}}. No markdown."
    
    try:
        response = client.models.generate_content(
            model='gemini-2.0-flash-lite', 
            contents=prompt, 
            config={"response_mime_type": "application/json"}
        )
        # Force strip markdown ticks
        raw_text = response.text.strip().replace("```json", "").replace("```", "")
        return json.loads(raw_text)
    except Exception as e:
        print(f"❌ AI Error: {e}")
        return {"summary": ["Analysis unavailable."], "sentiment": 50}

def run_pipeline():
    master_db = []
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        intel = ai_process_intelligence(co['name'])
        history, last_price = fetch_stock_history(ticker)
        
        fin = VERIFIED_DATA.get(ticker, {"eps_actual": 0, "eps_forecast": 0, "revenue": "N/A", "net_income": "N/A", "ebitda": "N/A", "ngr": "N/A", "fcf": "N/A", "jurisdictions": []})
        beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2) if fin["eps_forecast"] != 0 else 0

        master_db.append({
            "ticker": ticker, "company": co["name"], "base_country": co["base_country"],
            "release_gmt": VERIFIED_CALENDAR.get(ticker, ""), "last_price": last_price,
            "actuals": fin, "eps_beat_miss_pct": beat_miss,
            "news_summary": intel["summary"], "sentiment": intel["sentiment"],
            "jurisdictions": fin["jurisdictions"], "history": history
        })
        time.sleep(4)

    with open('gambling_stocks_live.json', 'w') as f:
        json.dump(master_db, f, indent=4)
    print("✅ Pipeline Complete.")

if __name__ == "__main__":
    run_pipeline()
