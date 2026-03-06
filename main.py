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

OTC_MAP = {
    "ENT.L": "GMVHF", "EVO.ST": "EVVTY", "EVOK.L": "EIHDF", 
    "KIND-SDB.ST": "KNDGF", "BETS-B.ST": "BTSBF", "PTEC.L": "PYTCF", 
    "ALL.AX": "ARLUF", "KAMBI.ST": "KMBIF"
}

# FULLY RESTORED & QUARTERIZED: Includes the exact "period" for dynamic HTML labeling
VERIFIED_DATA = {
    "FLUT": {"period": "Q4 25", "eps_actual": 1.74, "eps_forecast": 1.91, "revenue": "$4.74B", "net_income": "$10M", "ebitda": "$832M", "ngr": "$4.74B", "fcf": "-$1.43B", "jurisdictions": ["US", "UK", "Ireland", "Australia", "Italy"]},
    "DKNG": {"period": "Q4 25", "eps_actual": 0.25, "eps_forecast": 0.18, "revenue": "$1.99B", "net_income": "$136.4M", "ebitda": "$343M", "ngr": "$1.99B", "fcf": "$270M", "jurisdictions": ["US", "Ontario", "Puerto Rico"]},
    "ENT.L": {"period": "H2 25", "eps_actual": 0.62, "eps_forecast": 0.45, "revenue": "£3.2B", "net_income": "£150M", "ebitda": "£600M", "ngr": "£3.2B", "fcf": "£151M", "jurisdictions": ["UK", "Italy", "Brazil", "Australia"]},
    "EVO.ST": {"period": "Q4 25", "eps_actual": 1.54, "eps_forecast": 1.46, "revenue": "€514M", "net_income": "€306M", "ebitda": "€393M", "ngr": "€514M", "fcf": "€250M", "jurisdictions": ["Europe", "North America", "LatAm", "Asia"]},
    "MGM": {"period": "Q4 25", "eps_actual": 1.11, "eps_forecast": 0.56, "revenue": "$4.61B", "net_income": "$294M", "ebitda": "$635M", "ngr": "$4.61B", "fcf": "$300M", "jurisdictions": ["US", "Macau", "Japan"]},
    "CZR": {"period": "Q4 25", "eps_actual": -0.34, "eps_forecast": 0.10, "revenue": "$2.8B", "net_income": "-$72M", "ebitda": "$900M", "ngr": "$2.8B", "fcf": "$150M", "jurisdictions": ["US", "Canada"]},
    "PENN": {"period": "Q4 25", "eps_actual": 0.07, "eps_forecast": 0.02, "revenue": "$1.6B", "net_income": "$15M", "ebitda": "$350M", "ngr": "$1.6B", "fcf": "$80M", "jurisdictions": ["US", "Canada"]},
    "LVS": {"period": "Q4 25", "eps_actual": 0.65, "eps_forecast": 0.55, "revenue": "$2.9B", "net_income": "$450M", "ebitda": "$1.2B", "ngr": "$2.9B", "fcf": "$600M", "jurisdictions": ["Macau", "Singapore"]},
    "WYNN": {"period": "Q4 25", "eps_actual": 1.20, "eps_forecast": 1.05, "revenue": "$1.8B", "net_income": "$200M", "ebitda": "$600M", "ngr": "$1.8B", "fcf": "$300M", "jurisdictions": ["US", "Macau", "UAE"]},
    "EVOK.L": {"period": "H2 25", "eps_actual": -0.05, "eps_forecast": 0.01, "revenue": "£850M", "net_income": "-£40M", "ebitda": "£150M", "ngr": "£850M", "fcf": "£20M", "jurisdictions": ["UK", "Italy", "Spain"]},
    "KIND-SDB.ST": {"period": "Q4 25", "eps_actual": 0.15, "eps_forecast": 0.12, "revenue": "£310M", "net_income": "£35M", "ebitda": "£65M", "ngr": "£300M", "fcf": "£40M", "jurisdictions": ["UK", "Sweden", "Netherlands"]},
    "BETS-B.ST": {"period": "Q4 25", "eps_actual": 0.35, "eps_forecast": 0.32, "revenue": "€260M", "net_income": "€45M", "ebitda": "€75M", "ngr": "€260M", "fcf": "€50M", "jurisdictions": ["Nordics", "LatAm", "CEECA"]},
    "PTEC.L": {"period": "H2 25", "eps_actual": 0.18, "eps_forecast": 0.20, "revenue": "€850M", "net_income": "€55M", "ebitda": "€200M", "ngr": "€850M", "fcf": "€80M", "jurisdictions": ["UK", "Italy"]},
    "CHDN": {"period": "Q4 25", "eps_actual": 1.35, "eps_forecast": 1.20, "revenue": "$750M", "net_income": "$90M", "ebitda": "$300M", "ngr": "$750M", "fcf": "$120M", "jurisdictions": ["US"]},
    "LNW": {"period": "Q4 25", "eps_actual": 0.45, "eps_forecast": 0.50, "revenue": "$800M", "net_income": "$45M", "ebitda": "$280M", "ngr": "$800M", "fcf": "$100M", "jurisdictions": ["US", "Australia", "UK"]},
    "ALL.AX": {"period": "H2 25", "eps_actual": 0.95, "eps_forecast": 0.90, "revenue": "A$3.2B", "net_income": "A$600M", "ebitda": "A$1.1B", "ngr": "A$3.2B", "fcf": "A$750M", "jurisdictions": ["US", "Australia", "Global"]},
    "SGHC": {"period": "Q4 25", "eps_actual": 0.08, "eps_forecast": 0.10, "revenue": "€360M", "net_income": "€35M", "ebitda": "€75M", "ngr": "€360M", "fcf": "€45M", "jurisdictions": ["Canada", "Africa", "Europe"]},
    "RSI": {"period": "Q4 25", "eps_actual": 0.12, "eps_forecast": 0.08, "revenue": "$250M", "net_income": "$15M", "ebitda": "$40M", "ngr": "$250M", "fcf": "$20M", "jurisdictions": ["US", "Colombia", "Mexico"]},
    "BRAG": {"period": "Q4 25", "eps_actual": -0.02, "eps_forecast": 0.01, "revenue": "€28M", "net_income": "-€1M", "ebitda": "€4M", "ngr": "€28M", "fcf": "€1M", "jurisdictions": ["US", "Europe", "Canada"]},
    "KAMBI.ST": {"period": "Q4 25", "eps_actual": 0.18, "eps_forecast": 0.15, "revenue": "€45M", "net_income": "€5M", "ebitda": "€15M", "ngr": "€45M", "fcf": "€8M", "jurisdictions": ["Global B2B", "US", "LatAm"]}
}

VERIFIED_CALENDAR = {
    "FLUT": "2026-05-06T21:00:00Z", "DKNG": "2026-05-07T21:00:00Z", "ENT.L": "2026-04-29T07:00:00Z", "EVO.ST": "2026-04-22T06:30:00Z",
    "MGM": "2026-05-01T21:00:00Z", "CZR": "2026-05-05T21:00:00Z", "PENN": "2026-05-07T21:00:00Z", "LVS": "2026-04-20T21:00:00Z",
    "WYNN": "2026-05-06T21:00:00Z", "EVOK.L": "2026-04-15T07:00:00Z", "KIND-SDB.ST": "2026-04-24T06:30:00Z", "BETS-B.ST": "2026-04-23T06:30:00Z",
    "PTEC.L": "2026-03-25T07:00:00Z", "CHDN": "2026-04-22T21:00:00Z", "LNW": "2026-05-08T21:00:00Z", "ALL.AX": "2026-05-13T00:00:00Z",
    "SGHC": "2026-05-14T12:00:00Z", "RSI": "2026-05-06T21:00:00Z", "BRAG": "2026-05-14T12:00:00Z", "KAMBI.ST": "2026-04-29T06:30:00Z"
}

def fetch_stock_history(ticker):
    print(f"  -> Fetching charts for {ticker}...")
    is_otc = ticker in OTC_MAP
    fetch_ticker = OTC_MAP.get(ticker, ticker)
    
    history = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}
    last_price_str = "N/A"
    sym = "$" if is_otc else ("GBp " if ".L" in ticker else ("SEK " if ".ST" in ticker else "$"))
    
    try:
        ytk = yf.Ticker(fetch_ticker)
        
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
        print(f"  ❌ yfinance failed for {ticker}: {e}")
        
    return history, last_price_str

def ai_process_intelligence(company_name):
    print(f"  -> Analyzing News for {company_name}...")
    try:
        query = urllib.parse.quote(f'"{company_name}" stock OR gambling news')
        google_rss = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        
        req = urllib.request.Request(google_rss, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            feed = feedparser.parse(response.read())
            
        headlines = [entry.title for entry in feed.entries[:6]]
        if not headlines:
            return {"summary": ["No recent headlines found."], "sentiment": 50}

        prompt = f"Act as an iGaming analyst. Based on: {' | '.join(headlines)}. Return ONLY a raw JSON object like this: {{\"summary\": [\"Point 1\", \"Point 2\", \"Point 3\"], \"sentiment\": 75}}. Do not use code blocks."
        
        ai_resp = client.models.generate_content(
            model='gemini-1.5-flash', 
            contents=prompt
        )
        
        raw_text = ai_resp.text.strip()
        match = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
            
        return {"summary": ["News analysis parsing error."], "sentiment": 50}
        
    except Exception as e:
        print(f"  ❌ AI/News failed for {company_name}: {e}")
        return {"summary": ["News temporarily unavailable."], "sentiment": 50}

def run_pipeline():
    master_db = []
    print(f"🚀 Starting Pipeline...")
    
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        print(f"\nProcessing {co['name']}...")
        
        # Pulls the full structure, including the new 'period' key
        fin = VERIFIED_DATA.get(ticker, {
            "period": "N/A", "eps_actual": 0, "eps_forecast": 0, "revenue": "N/A", 
            "net_income": "N/A", "ebitda": "N/A", "ngr": "N/A", "fcf": "N/A", "jurisdictions": []
        })
        
        # Beat/Miss Math preserved
        beat_miss = 0
        if fin.get("eps_forecast", 0) != 0:
            beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2)
            
        try:
            intel = ai_process_intelligence(co['name'])
            history, last_price = fetch_stock_history(ticker)
        except Exception as e:
            print(f"  ⚠️ Critical loop failure for {ticker}: {e}")
            intel = {"summary": ["System Error."], "sentiment": 50}
            history, last_price = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}, "N/A"

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
        
        time.sleep(3)

    if master_db:
        with open('gambling_stocks_live.json', 'w') as f:
            json.dump(master_db, f, indent=4)
        print(f"\n✅ Pipeline Complete. Saved {len(master_db)} companies.")
    else:
        print("\n❌ Pipeline Error: No data collected.")

if __name__ == "__main__":
    run_pipeline()
