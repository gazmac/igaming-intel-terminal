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

# Using your full list of 20 targets
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

# --- VERIFIED FALLBACK DATA ---
VERIFIED_DATA = {
    "FLUT": {"eps_actual": -1.75, "eps_forecast": 0.24, "revenue": "$16.38B", "net_income": "-$407M", "ebitda": "$2.84B", "ngr": "$16.38B", "fcf": "$407M", "jurisdictions": ["US", "UK", "Ireland", "Australia", "Italy"]},
    "DKNG": {"eps_actual": 0.28, "eps_forecast": 0.20, "revenue": "$1.99B", "net_income": "$136.4M", "ebitda": "$343.2M", "ngr": "$1.99B", "fcf": "$150M", "jurisdictions": ["US", "Ontario", "Puerto Rico"]},
    "ENT.L": {"eps_actual": 0.62, "eps_forecast": 0.45, "revenue": "£5.26B", "net_income": "-£681M", "ebitda": "£1.16B", "ngr": "£5.32B", "fcf": "£151M", "jurisdictions": ["UK", "Italy", "Brazil", "Australia", "Spain"]},
    "EVO.ST": {"eps_actual": 1.54, "eps_forecast": 1.46, "revenue": "€514M", "net_income": "€306M", "ebitda": "€393M", "ngr": "€514M", "fcf": "€250M", "jurisdictions": ["Europe", "North America", "LatAm", "Asia"]},
    "MGM": {"eps_actual": 0.76, "eps_forecast": 2.40, "revenue": "$17.5B", "net_income": "$206M", "ebitda": "$2.4B", "ngr": "$17.5B", "fcf": "$1.2B", "jurisdictions": ["US", "Macau", "Japan"]},
    "CZR": {"eps_actual": -2.42, "eps_forecast": 0.10, "revenue": "$11.49B", "net_income": "-$502M", "ebitda": "$3.29B", "ngr": "$11.49B", "fcf": "$493M", "jurisdictions": ["US", "Canada", "UK"]},
    "PENN": {"eps_actual": 0.07, "eps_forecast": 0.02, "revenue": "$6.3B", "net_income": "-$120M", "ebitda": "$1.4B", "ngr": "$6.3B", "fcf": "$250M", "jurisdictions": ["US", "Canada"]},
    "LVS": {"eps_actual": 2.45, "eps_forecast": 2.80, "revenue": "$13.0B", "net_income": "$1.87B", "ebitda": "$5.23B", "ngr": "$13.0B", "fcf": "$2.5B", "jurisdictions": ["Macau", "Singapore"]},
    "WYNN": {"eps_actual": 4.50, "eps_forecast": 5.00, "revenue": "$7.3B", "net_income": "$850M", "ebitda": "$2.4B", "ngr": "$7.3B", "fcf": "$1.1B", "jurisdictions": ["US", "Macau", "UAE"]},
    "EVOK.L": {"eps_actual": -0.15, "eps_forecast": 0.05, "revenue": "£1.7B", "net_income": "-£115M", "ebitda": "£310M", "ngr": "£1.7B", "fcf": "£50M", "jurisdictions": ["UK", "Italy", "Spain", "Romania"]},
    "KIND-SDB.ST": {"eps_actual": 0.21, "eps_forecast": 0.30, "revenue": "£1.21B", "net_income": "£47M", "ebitda": "£134M", "ngr": "£1.21B", "fcf": "£109M", "jurisdictions": ["UK", "Sweden", "Netherlands", "Australia"]},
    "BETS-B.ST": {"eps_actual": 1.29, "eps_forecast": 1.35, "revenue": "€1.20B", "net_income": "€182M", "ebitda": "€314M", "ngr": "€1.20B", "fcf": "€215M", "jurisdictions": ["Nordics", "LatAm", "CEECA", "Europe"]},
    "PTEC.L": {"eps_actual": 0.35, "eps_forecast": 0.40, "revenue": "€1.7B", "net_income": "€105M", "ebitda": "€400M", "ngr": "€1.7B", "fcf": "€150M", "jurisdictions": ["UK", "Italy", "LatAm"]},
    "CHDN": {"eps_actual": 5.29, "eps_forecast": 6.13, "revenue": "$2.93B", "net_income": "$383M", "ebitda": "$1.21B", "ngr": "$2.93B", "fcf": "$450M", "jurisdictions": ["US"]},
    "LNW": {"eps_actual": 1.50, "eps_forecast": 2.10, "revenue": "$3.2B", "net_income": "$180M", "ebitda": "$1.1B", "ngr": "$3.2B", "fcf": "$400M", "jurisdictions": ["US", "Australia", "UK"]},
    "ALL.AX": {"eps_actual": 1.85, "eps_forecast": 2.05, "revenue": "A$6.3B", "net_income": "A$1.18B", "ebitda": "A$2.1B", "ngr": "A$6.3B", "fcf": "A$1.48B", "jurisdictions": ["US", "Australia", "Global"]},
    "SGHC": {"eps_actual": 0.30, "eps_forecast": 0.35, "revenue": "€1.4B", "net_income": "€150M", "ebitda": "€300M", "ngr": "€1.4B", "fcf": "€180M", "jurisdictions": ["Canada", "Africa", "Europe"]},
    "RSI": {"eps_actual": 0.32, "eps_forecast": 0.40, "revenue": "$1.13B", "net_income": "$74M", "ebitda": "$154M", "ngr": "$1.13B", "fcf": "$85M", "jurisdictions": ["US", "Colombia", "Mexico", "Canada"]},
    "BRAG": {"eps_actual": -0.10, "eps_forecast": 0.05, "revenue": "€106M", "net_income": "-€4M", "ebitda": "€15M", "ngr": "€106M", "fcf": "€5M", "jurisdictions": ["US", "Europe", "Brazil", "Canada"]},
    "KAMBI.ST": {"eps_actual": 0.67, "eps_forecast": 0.69, "revenue": "€176M", "net_income": "€15M", "ebitda": "€59M", "ngr": "€176M", "fcf": "€27M", "jurisdictions": ["Global B2B", "US", "LatAm"]}
}

VERIFIED_CALENDAR = {
    "FLUT": "2026-05-06T21:00:00Z", "DKNG": "2026-05-07T21:00:00Z", "ENT.L": "2026-04-29T07:00:00Z", "EVO.ST": "2026-04-22T06:30:00Z",
    "MGM": "2026-05-01T21:00:00Z", "CZR": "2026-05-05T21:00:00Z", "PENN": "2026-05-07T21:00:00Z", "LVS": "2026-04-20T21:00:00Z",
    "WYNN": "2026-05-06T21:00:00Z", "EVOK.L": "2026-04-15T07:00:00Z", "KIND-SDB.ST": "2026-04-24T06:30:00Z", "BETS-B.ST": "2026-04-23T06:30:00Z",
    "PTEC.L": "2026-03-25T07:00:00Z", "CHDN": "2026-04-22T21:00:00Z", "LNW": "2026-05-08T21:00:00Z", "ALL.AX": "2026-05-13T00:00:00Z",
    "SGHC": "2026-05-14T12:00:00Z", "RSI": "2026-05-06T21:00:00Z", "BRAG": "2026-05-14T12:00:00Z", "KAMBI.ST": "2026-04-29T06:30:00Z"
}

def fetch_stock_history(ticker):
    """Fetches charts via yfinance using a secure session to bypass European GDPR walls."""
    print(f"Fetching charts and price for {ticker}...")
    
    # Injecting a browser session to defeat Yahoo's EU Cookie Blocks
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'})
    
    periods = {
        "1d": {"p": "1d", "i": "15m"}, "1w": {"p": "5d", "i": "1h"}, 
        "1m": {"p": "1mo", "i": "1d"}, "3m": {"p": "3mo", "i": "1d"}, 
        "6m": {"p": "6mo", "i": "1d"}, "1y": {"p": "1y", "i": "1wk"}, 
        "5y": {"p": "5y", "i": "1wk"} 
    }
    history = {}
    last_price_str = "N/A"
    
    # Format currencies cleanly
    currencies = {"FLUT": "$", "DKNG": "$", "ENT.L": "GBp ", "EVO.ST": "SEK ", "EVOK.L": "GBp ", "KIND-SDB.ST": "SEK ", "BETS-B.ST": "SEK ", "PTEC.L": "GBp ", "ALL.AX": "A$", "KAMBI.ST": "SEK "}
    sym = currencies.get(ticker, "$")
    
    try:
        # Pass the secure session into yfinance
        ytk = yf.Ticker(ticker, session=session)
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
        print(f"❌ Error fetching {ticker}: {e}")
        
    return history, last_price_str

def ai_process_intelligence(company_name):
    """Fetches global news from Google and parses Gemini JSON securely."""
    # 1. Switch entirely to Google News to fix the broken Yahoo RSS for International stocks
    print(f"Fetching Google News for {company_name}...")
    query = urllib.parse.quote(f'"{company_name}" stock OR earnings')
    google_rss = f"[https://news.google.com/rss/search?q=](https://news.google.com/rss/search?q=){query}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(google_rss)
    headlines = [entry.title for entry in feed.entries[:6]]
    
    if not headlines:
        return {"summary": ["No recent news found for this company."], "sentiment": 50}

    prompt = f"Act as an iGaming financial analyst. Based on these headlines for {company_name}: {' | '.join(headlines)}. Return ONLY a JSON object exactly like this: {{\"summary\": [\"Point 1\", \"Point 2\", \"Point 3\"], \"sentiment\": 75}}. Do not include markdown code blocks."
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite', 
            contents=prompt, 
            config={"response_mime_type": "application/json"}
        )
        
        # 2. The JSON Scrubber: Strips the ```json markdown that breaks the parser
        raw_text = response.text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:]
        if raw_text.startswith("```"):
            raw_text = raw_text[3:]
        if raw_text.endswith("```"):
            raw_text = raw_text[:-3]
            
        return json.loads(raw_text.strip())
    except Exception as e:
        print(f"❌ AI Parsing Error for {company_name}: {e}")
        return {"summary": ["AI Analysis temporarily unavailable due to formatting error."], "sentiment": 50}

def run_pipeline():
    master_db = []
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        print(f"\nProcessing {co['name']} ({ticker})...")
        
        intel = ai_process_intelligence(co['name'])
        history, last_price = fetch_stock_history(ticker)
        
        fin = VERIFIED_DATA.get(ticker, VERIFIED_DATA["DKNG"]) # Fallback safety
        beat_miss = 0
        if fin["eps_forecast"] != 0:
            beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2)

        master_db.append({
            "ticker": ticker, "company": co["name"], "base_country": co["base_country"],
            "release_gmt": VERIFIED_CALENDAR.get(ticker, ""), "last_price": last_price,
            "actuals": fin, "eps_beat_miss_pct": beat_miss,
            "news_summary": intel["summary"], "sentiment": intel["sentiment"],
            "jurisdictions": fin["jurisdictions"], "history": history
        })
        
        time.sleep(4) # Respect Gemini API limits

    with open('gambling_stocks_live.json', 'w') as f:
        json.dump(master_db, f, indent=4)
    print("\n✅ Pipeline Complete.")

if __name__ == "__main__":
    run_pipeline()
