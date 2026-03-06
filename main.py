import requests
import json
import urllib.parse
import os
import pandas as pd
import feedparser
import time
import yfinance as yf
import re
from google import genai
from datetime import datetime

# --- 1. CONFIGURATION ---
# Ensure your GitHub Secret is named GEMINI_API_KEY
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

# Map EU/Aussie tickers to US OTC equivalents to bypass Yahoo's GDPR cookie walls
OTC_MAP = {
    "ENT.L": "GMVHF", "EVO.ST": "EVVTY", "EVOK.L": "EIHDF", 
    "KIND-SDB.ST": "KNDGF", "BETS-B.ST": "BTSBF", "PTEC.L": "PYTCF", 
    "ALL.AX": "ARLUF", "KAMBI.ST": "KMBIF"
}

# Financial Fallbacks for UI Stability
VERIFIED_DATA = {
    "FLUT": {"eps_actual": -1.75, "eps_forecast": 0.24, "revenue": "$16.38B", "ebitda": "$2.84B", "jurisdictions": ["US", "UK", "Ireland"]},
    "DKNG": {"eps_actual": 0.28, "eps_forecast": 0.20, "revenue": "$1.99B", "ebitda": "$343.2M", "jurisdictions": ["US", "Canada"]},
    "ENT.L": {"eps_actual": 0.62, "eps_forecast": 0.45, "revenue": "£5.26B", "ebitda": "£1.16B", "jurisdictions": ["UK", "Italy", "Brazil"]},
    "EVO.ST": {"eps_actual": 1.54, "eps_forecast": 1.46, "revenue": "€514M", "ebitda": "€393M", "jurisdictions": ["Global Live Casino"]},
    "MGM": {"eps_actual": 0.76, "eps_forecast": 2.40, "revenue": "$17.5B", "ebitda": "$2.4B", "jurisdictions": ["US", "Macau"]},
    "CZR": {"eps_actual": -2.42, "eps_forecast": 0.10, "revenue": "$11.49B", "ebitda": "$3.29B", "jurisdictions": ["US", "Canada"]},
    "PENN": {"eps_actual": 0.07, "eps_forecast": 0.02, "revenue": "$6.3B", "ebitda": "$1.4B", "jurisdictions": ["US"]},
    "LVS": {"eps_actual": 2.45, "eps_forecast": 2.80, "revenue": "$13.0B", "ebitda": "$5.23B", "jurisdictions": ["Macau", "Singapore"]},
    "WYNN": {"eps_actual": 4.50, "eps_forecast": 5.00, "revenue": "$7.3B", "ebitda": "$2.4B", "jurisdictions": ["US", "Macau"]},
    "EVOK.L": {"eps_actual": -0.15, "eps_forecast": 0.05, "revenue": "£1.7B", "ebitda": "£310M", "jurisdictions": ["UK", "Spain"]},
    "KIND-SDB.ST": {"eps_actual": 0.21, "eps_forecast": 0.30, "revenue": "£1.21B", "ebitda": "£134M", "jurisdictions": ["Europe", "Australia"]},
    "BETS-B.ST": {"eps_actual": 1.29, "eps_forecast": 1.35, "revenue": "€1.20B", "ebitda": "€314M", "jurisdictions": ["Nordics", "LatAm"]},
    "PTEC.L": {"eps_actual": 0.35, "eps_forecast": 0.40, "revenue": "€1.7B", "ebitda": "€400M", "jurisdictions": ["UK", "Italy"]},
    "CHDN": {"eps_actual": 5.29, "eps_forecast": 6.13, "revenue": "$2.93B", "ebitda": "$1.21B", "jurisdictions": ["US"]},
    "LNW": {"eps_actual": 1.50, "eps_forecast": 2.10, "revenue": "$3.2B", "ebitda": "$1.1B", "jurisdictions": ["US", "Australia"]},
    "ALL.AX": {"eps_actual": 1.85, "eps_forecast": 2.05, "revenue": "A$6.3B", "ebitda": "A$2.1B", "jurisdictions": ["Global slots"]},
    "SGHC": {"eps_actual": 0.30, "eps_forecast": 0.35, "revenue": "€1.4B", "ebitda": "€300M", "jurisdictions": ["Canada", "Africa"]},
    "RSI": {"eps_actual": 0.32, "eps_forecast": 0.40, "revenue": "$1.13B", "ebitda": "$154M", "jurisdictions": ["US", "LatAm"]},
    "BRAG": {"eps_actual": -0.10, "eps_forecast": 0.05, "revenue": "€106M", "ebitda": "€15M", "jurisdictions": ["iGaming B2B"]},
    "KAMBI.ST": {"eps_actual": 0.67, "eps_forecast": 0.69, "revenue": "€176M", "ebitda": "€59M", "jurisdictions": ["B2B Sportsbook"]}
}

VERIFIED_CALENDAR = {
    "FLUT": "2026-05-06T21:00:00Z", "DKNG": "2026-05-07T21:00:00Z", "ENT.L": "2026-04-29T07:00:00Z", "EVO.ST": "2026-04-22T06:30:00Z"
}

# --- 2. CORE FUNCTIONS ---

def fetch_stock_history(ticker):
    """Fetches charts safely using native yfinance cookie handling."""
    print(f"  -> Fetching charts for {ticker}...")
    is_otc = ticker in OTC_MAP
    fetch_ticker = OTC_MAP.get(ticker, ticker)
    
    history = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}
    last_price_str = "N/A"
    sym = "$" if is_otc else ("GBp " if ".L" in ticker else ("SEK " if ".ST" in ticker else "$"))
    
    try:
        ytk = yf.Ticker(fetch_ticker)
        
        # 1. Fetch Intraday
        df_1d = ytk.history(period="1d", interval="15m")
        if not df_1d.empty:
            last_price_str = f"{sym}{round(df_1d['Close'].iloc[-1], 2)}"
            history["1d"] = [[int(pd.Timestamp(idx).timestamp() * 1000), round(row['Close'], 2)] for idx, row in df_1d.iterrows()]

        # 2. Fetch Historical and slice locally
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
        print(f"  ❌ yfinance failed for {ticker}: {e}")
        
    return history, last_price_str

def ai_process_intelligence(company_name):
    """Fetches news via stealth headers and strictly parses AI JSON."""
    print(f"  -> Analyzing News for {company_name}...")
    try:
        query = urllib.parse.quote(f'"{company_name}" stock OR gambling news')
        google_rss = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        
        # Stealth Headers to avoid 403 blocks
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
        rss_response = requests.get(google_rss, headers=headers, timeout=10)
        feed = feedparser.parse(rss_response.text)
        headlines = [entry.title for entry in feed.entries[:6]]
        
        if not headlines:
            return {"summary": ["No recent headlines found."], "sentiment": 50}

        prompt = f"Act as an iGaming analyst. Based on: {' | '.join(headlines)}. Return ONLY JSON: {{\"summary\": [\"Point 1\", \"Point 2\", \"Point 3\"], \"sentiment\": 75}}"
        
        response = client.models.generate_content(
            model='gemini-2.0-flash', 
            contents=prompt, 
            config={"response_mime_type": "application/json"}
        )
        
        # Use regex to find the JSON block in case AI adds extra text
        match = re.search(r'\{.*\}', response.text.strip(), re.DOTALL)
        if match:
            return json.loads(match.group(0))
        return {"summary": ["News analysis error."], "sentiment": 50}
        
    except Exception as e:
        print(f"  ❌ AI/News failed for {company_name}: {e}")
        return {"summary": ["News temporarily unavailable."], "sentiment": 50}

# --- 3. PIPELINE EXECUTION ---

def run_pipeline():
    master_db = []
    print(f"🚀 Starting Pipeline at {datetime.now()}...")
    
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        print(f"\nProcessing {co['name']}...")
        
        # Start with safe defaults
        fin = VERIFIED_DATA.get(ticker, {"eps_actual": 0, "eps_forecast": 0, "revenue": "N/A", "ebitda": "N/A", "jurisdictions": []})
        beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2) if fin.get("eps_forecast", 0) != 0 else 0
        
        # Wrap everything in a try/except so one failure doesn't kill the loop
        try:
            intel = ai_process_intelligence(co['name'])
            history, last_price = fetch_stock_history(ticker)
            
            master_db.append({
                "ticker": ticker,
                "company": co["name"],
                "base_country": co["base_country"],
                "release_gmt": VERIFIED_CALENDAR.get(ticker, ""),
                "last_price": last_price,
                "actuals": fin,
                "eps_beat_miss_pct": beat_miss,
                "news_summary": intel.get("summary", ["Analysis unavailable"]),
                "sentiment": intel.get("sentiment", 50),
                "jurisdictions": fin.get("jurisdictions", []),
                "history": history
            })
        except Exception as e:
            print(f"  ⚠️ Critical loop failure for {ticker}: {e}")
            
        # Standard rate-limit safety
        time.sleep(3)

    # Only write if we actually have data to prevent clearing the dashboard
    if master_db:
        with open('gambling_stocks_live.json', 'w') as f:
            json.dump(master_db, f, indent=4)
        print(f"\n✅ Pipeline Complete. Saved {len(master_db)} companies.")
    else:
        print("\n❌ Pipeline Error: No data collected.")

if __name__ == "__main__":
    run_pipeline()
