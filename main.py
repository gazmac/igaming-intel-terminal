import requests
import json
import urllib.parse
import urllib.request
import os
import pandas as pd
import feedparser
import time
import yfinance as yf
import re
import traceback
import sys
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
    {"name": "Sportradar", "ticker": "SRAD", "base_country": "Switzerland"},
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
    "BETS-B.ST": "BTSBF", "PTEC.L": "PYTCF", 
    "ALL.AX": "ARLUF", "KAMBI.ST": "KMBIF"
}

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
    "SRAD": {"period": "Q4 25", "eps_actual": 0.14, "eps_forecast": 0.10, "revenue": "$280M", "net_income": "$35M", "ebitda": "$55M", "ngr": "$280M", "fcf": "$40M", "jurisdictions": ["Global B2B", "US", "Europe"]},
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

# RESTORED: Fully detailed dictionary format
VERIFIED_CALENDAR = {
    "FLUT": {"date": "May 6, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "DKNG": {"date": "May 8, 2026", "report_time": "Pre-Market", "call_time": "8:30 AM EST"},
    "ENT.L": {"date": "Apr 16, 2026", "report_time": "7:00 AM BST", "call_time": "9:00 AM BST"},
    "EVO.ST": {"date": "Apr 22, 2026", "report_time": "7:30 AM CET", "call_time": "9:00 AM CET"},
    "MGM": {"date": "May 1, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "CZR": {"date": "Apr 28, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "PENN": {"date": "May 7, 2026", "report_time": "7:00 AM EST", "call_time": "8:00 AM EST"},
    "LVS": {"date": "Apr 22, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "WYNN": {"date": "May 6, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "EVOK.L": {"date": "Apr 15, 2026", "report_time": "7:00 AM BST", "call_time": "8:30 AM BST"},
    "SRAD": {"date": "May 12, 2026", "report_time": "Pre-Market", "call_time": "8:30 AM EST"},
    "BETS-B.ST": {"date": "Apr 24, 2026", "report_time": "7:30 AM CET", "call_time": "9:00 AM CET"},
    "PTEC.L": {"date": "Mar 25, 2026", "report_time": "7:00 AM GMT", "call_time": "9:00 AM GMT"},
    "CHDN": {"date": "Apr 22, 2026", "report_time": "Post-Market", "call_time": "9:00 AM EST (Next Day)"},
    "LNW": {"date": "May 8, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "ALL.AX": {"date": "May 13, 2026", "report_time": "8:00 AM AEST", "call_time": "10:30 AM AEST"},
    "SGHC": {"date": "May 14, 2026", "report_time": "Pre-Market", "call_time": "8:30 AM EST"},
    "RSI": {"date": "May 6, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "BRAG": {"date": "May 14, 2026", "report_time": "Pre-Market", "call_time": "8:30 AM EST"},
    "KAMBI.ST": {"date": "Apr 29, 2026", "report_time": "7:45 AM CET", "call_time": "10:45 AM CET"}
}

# --- 2. CORE FUNCTIONS ---

def get_native_price(ticker):
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=price"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=5)
        data = res.json()
        price_data = data['quoteSummary']['result'][0]['price']
        
        price = price_data['regularMarketPrice']['raw']
        currency = price_data['currency'] 
        
        if currency == "GBp": sym = "GBp "
        elif currency == "GBP": sym = "£"
        elif currency == "SEK": sym = "SEK "
        elif currency == "EUR": sym = "€"
        elif currency == "AUD": sym = "A$"
        elif currency == "CAD": sym = "C$"
        else: sym = "$"
        
        return f"{sym}{round(price, 2)}", price
    except Exception:
        try:
            ytk = yf.Ticker(ticker)
            price = ytk.fast_info['lastPrice']
            if ".L" in ticker: sym = "GBp "
            elif ".ST" in ticker: sym = "SEK "
            elif ".PA" in ticker or ".AS" in ticker: sym = "€"
            elif ".AX" in ticker: sym = "A$"
            else: sym = "$"
            return f"{sym}{round(price, 2)}", price
        except Exception:
            return "N/A", None

def fetch_stock_history(ticker, native_price_raw):
    print(f"  -> Fetching charts for {ticker}...")
    is_otc = ticker in OTC_MAP
    fetch_ticker = OTC_MAP.get(ticker, ticker)
    
    history = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}
    
    try:
        ytk = yf.Ticker(fetch_ticker)
        
        df_1d = ytk.history(period="1d", interval="15m")
        if not df_1d.empty:
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
            
        if is_otc and native_price_raw and history["1d"]:
            latest_otc = history["1d"][-1][1]
            if latest_otc > 0:
                ratio = native_price_raw / latest_otc
                for period in history:
                    history[period] = [[pt[0], round(pt[1] * ratio, 2)] for pt in history[period]]
                    
    except Exception as e:
        print(f"  ❌ yfinance failed for {ticker}: {e}")
        
    return history

def ai_process_intelligence(company_name, ticker):
    print(f"  -> Fetching Yahoo API News for {company_name}...")
    
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return {"summary": ["System Error: API key missing."], "sentiment": 50}
        
    try:
        client = genai.Client(api_key=api_key)
        clean_name = urllib.parse.quote(company_name)
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={clean_name}&newsCount=5"
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        res = requests.get(url, headers=headers, timeout=10)
        res_data = res.json()
        headlines = [item['title'] for item in res_data.get('news', [])]
        
        if not headlines:
            return {"summary": [f"No news headlines found recently for {company_name}."], "sentiment": 50}

        prompt = f"Act as an iGaming financial analyst. Review these headlines for {company_name}: {' | '.join(headlines)}. Return a valid JSON object with exactly two keys: 'summary' (a list of 3 string bullet points summarizing the news) and 'sentiment' (an integer from 0 to 100 representing market sentiment)."
        
        ai_resp = client.models.generate_content(
            model='gemini-2.5-flash', 
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        
        raw_text = ai_resp.text.strip()
        match = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if match:
            data = json.loads(match.group(0))
            if "summary" in data and "sentiment" in data:
                return data
                
        return {"summary": ["Failed to extract valid data from AI."], "sentiment": 50}
        
    except Exception as e:
        print(f"  ❌ AI/News failed for {company_name}: {e}")
        error_msg = str(e).replace('"', "'")[:60]
        return {"summary": [f"News Engine Error: {error_msg}"], "sentiment": 50}

# --- 3. PIPELINE EXECUTION ---

def run_pipeline():
    master_db = []
    print(f"🚀 Starting Pipeline...")
    
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        print(f"\nProcessing {co['name']}...")
        
        fin = VERIFIED_DATA.get(ticker, {
            "period": "N/A", "eps_actual": 0, "eps_forecast": 0, "revenue": "N/A", 
            "net_income": "N/A", "ebitda": "N/A", "ngr": "N/A", "fcf": "N/A", "jurisdictions": []
        })
        
        # RESTORED: Safely fetch the calendar dictionary object
        cal = VERIFIED_CALENDAR.get(ticker, {"date": "TBD", "report_time": "TBD", "call_time": "TBD"})
        
        beat_miss = 0
        if fin.get("eps_forecast", 0) != 0:
            beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2)
            
        try:
            intel = ai_process_intelligence(co['name'], ticker)
            last_price_str, native_
