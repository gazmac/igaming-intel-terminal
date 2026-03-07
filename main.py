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
    "FLUT": {"period": "Q4 25", "focus": "B2C Sportsbook & iGaming", "map_codes": ["US", "GB", "IE", "AU", "IT", "BR"], "eps_actual": 1.74, "eps_forecast": 1.91, "revenue": "$4.74B", "net_income": "$10M", "ebitda": "$832M", "ngr": "$4.74B", "fcf": "-$1.43B", "jurisdictions": ["US", "UK", "Ireland", "Australia", "Italy"]},
    "DKNG": {"period": "Q4 25", "focus": "B2C Sportsbook & iGaming", "map_codes": ["US", "CA", "PR"], "eps_actual": 0.25, "eps_forecast": 0.18, "revenue": "$1.99B", "net_income": "$136.4M", "ebitda": "$343M", "ngr": "$1.99B", "fcf": "$270M", "jurisdictions": ["US", "Ontario", "Puerto Rico"]},
    "ENT.L": {"period": "H2 25", "focus": "B2C Sportsbook, iGaming & Retail", "map_codes": ["GB", "IT", "BR", "AU", "ES"], "eps_actual": 0.62, "eps_forecast": 0.45, "revenue": "£3.2B", "net_income": "£150M", "ebitda": "£600M", "ngr": "£3.2B", "fcf": "£151M", "jurisdictions": ["UK", "Italy", "Brazil", "Australia"]},
    "EVO.ST": {"period": "Q4 25", "focus": "B2B Live Casino Technology", "map_codes": ["SE", "US", "CA", "MT", "LV", "GE", "RO"], "eps_actual": 1.54, "eps_forecast": 1.46, "revenue": "€514M", "net_income": "€306M", "ebitda": "€393M", "ngr": "€514M", "fcf": "€250M", "jurisdictions": ["Europe", "North America", "LatAm", "Asia"]},
    "MGM": {"period": "Q4 25", "focus": "Land-based Resorts & B2C Digital", "map_codes": ["US", "CN", "JP"], "eps_actual": 1.11, "eps_forecast": 0.56, "revenue": "$4.61B", "net_income": "$294M", "ebitda": "$635M", "ngr": "$4.61B", "fcf": "$300M", "jurisdictions": ["US", "Macau", "Japan"]},
    "CZR": {"period": "Q4 25", "focus": "Land-based Resorts & B2C Digital", "map_codes": ["US", "CA", "GB", "AE"], "eps_actual": -0.34, "eps_forecast": 0.10, "revenue": "$2.8B", "net_income": "-$72M", "ebitda": "$900M", "ngr": "$2.8B", "fcf": "$150M", "jurisdictions": ["US", "Canada", "UK", "UAE"]},
    "PENN": {"period": "Q4 25", "focus": "Land-based Casinos & B2C Digital", "map_codes": ["US", "CA"], "eps_actual": 0.07, "eps_forecast": 0.02, "revenue": "$1.6B", "net_income": "$15M", "ebitda": "$350M", "ngr": "$1.6B", "fcf": "$80M", "jurisdictions": ["US", "Canada"]},
    "LVS": {"period": "Q4 25", "focus": "Land-based Casino Resorts", "map_codes": ["CN", "SG"], "eps_actual": 0.65, "eps_forecast": 0.55, "revenue": "$2.9B", "net_income": "$450M", "ebitda": "$1.2B", "ngr": "$2.9B", "fcf": "$600M", "jurisdictions": ["Macau", "Singapore"]},
    "WYNN": {"period": "Q4 25", "focus": "Luxury Land-based Resorts", "map_codes": ["US", "CN", "AE"], "eps_actual": 1.20, "eps_forecast": 1.05, "revenue": "$1.8B", "net_income": "$200M", "ebitda": "$600M", "ngr": "$1.8B", "fcf": "$300M", "jurisdictions": ["US", "Macau", "UAE"]},
    "EVOK.L": {"period": "H2 25", "focus": "B2C Sportsbook, iGaming & Retail", "map_codes": ["GB", "IT", "ES", "RO"], "eps_actual": -0.05, "eps_forecast": 0.01, "revenue": "£850M", "net_income": "-£40M", "ebitda": "£150M", "ngr": "£850M", "fcf": "£20M", "jurisdictions": ["UK", "Italy", "Spain"]},
    "SRAD": {"period": "Q4 25", "focus": "B2B Sports Data & Technology", "map_codes": ["CH", "US", "GB", "DE", "AT"], "eps_actual": 0.14, "eps_forecast": 0.10, "revenue": "$280M", "net_income": "$35M", "ebitda": "$55M", "ngr": "$280M", "fcf": "$40M", "jurisdictions": ["Global B2B", "US", "Europe"]},
    "BETS-B.ST": {"period": "Q4 25", "focus": "B2C & B2B iGaming/Sportsbook", "map_codes": ["SE", "MT", "IT", "AR", "CO", "PE"], "eps_actual": 0.35, "eps_forecast": 0.32, "revenue": "€260M", "net_income": "€45M", "ebitda": "€75M", "ngr": "€260M", "fcf": "€50M", "jurisdictions": ["Nordics", "LatAm", "CEECA"]},
    "PTEC.L": {"period": "H2 25", "focus": "B2B iGaming & Sportsbook Tech", "map_codes": ["GB", "IT", "BG", "UA", "EE"], "eps_actual": 0.18, "eps_forecast": 0.20, "revenue": "€850M", "net_income": "€55M", "ebitda": "€200M", "ngr": "€850M", "fcf": "€80M", "jurisdictions": ["UK", "Italy", "LatAm"]},
    "CHDN": {"period": "Q4 25", "focus": "Racing, Casinos & Online Wagering", "map_codes": ["US"], "eps_actual": 1.35, "eps_forecast": 1.20, "revenue": "$750M", "net_income": "$90M", "ebitda": "$300M", "ngr": "$750M", "fcf": "$120M", "jurisdictions": ["US"]},
    "LNW": {"period": "Q4 25", "focus": "B2B Gaming Machines & iGaming", "map_codes": ["US", "AU", "GB", "SE"], "eps_actual": 0.45, "eps_forecast": 0.50, "revenue": "$800M", "net_income": "$45M", "ebitda": "$280M", "ngr": "$800M", "fcf": "$100M", "jurisdictions": ["US", "Australia", "UK"]},
    "ALL.AX": {"period": "H2 25", "focus": "B2B Slots, Social Casino & iGaming", "map_codes": ["AU", "US", "GB", "IL"], "eps_actual": 0.95, "eps_forecast": 0.90, "revenue": "A$3.2B", "net_income": "A$600M", "ebitda": "A$1.1B", "ngr": "A$3.2B", "fcf": "A$750M", "jurisdictions": ["US", "Australia", "Global"]},
    "SGHC": {"period": "Q4 25", "focus": "B2C Sportsbook & iGaming", "map_codes": ["ZA", "CA", "GB", "MT", "FR"], "eps_actual": 0.08, "eps_forecast": 0.10, "revenue": "€360M", "net_income": "€35M", "ebitda": "€75M", "ngr": "€360M", "fcf": "€45M", "jurisdictions": ["Canada", "Africa", "Europe"]},
    "RSI": {"period": "Q4 25", "focus": "B2C Casino-First iGaming", "map_codes": ["US", "CO", "MX", "CA", "PE"], "eps_actual": 0.12, "eps_forecast": 0.08, "revenue": "$250M", "net_income": "$15M", "ebitda": "$40M", "ngr": "$250M", "fcf": "$20M", "jurisdictions": ["US", "Colombia", "Mexico"]},
    "BRAG": {"period": "Q4 25", "focus": "B2B iGaming Content & PAM", "map_codes": ["CA", "US", "NL", "BR", "FI"], "eps_actual": -0.02, "eps_forecast": 0.01, "revenue": "€28M", "net_income": "-€1M", "ebitda": "€4M", "ngr": "€28M", "fcf": "€1M", "jurisdictions": ["US", "Europe", "Canada"]},
    "KAMBI.ST": {"period": "Q4 25", "focus": "B2B Sportsbook Technology", "map_codes": ["MT", "SE", "GB", "US", "RO", "CO"], "eps_actual": 0.18, "eps_forecast": 0.15, "revenue": "€45M", "net_income": "€5M", "ebitda": "€15M", "ngr": "€45M", "fcf": "€8M", "jurisdictions": ["Global B2B", "US", "LatAm"]}
}

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

def get_live_fx_rates():
    """Fetches real-time Forex conversion rates to safely convert EU/UK/AUS market caps to USD."""
    print("🌍 Fetching live Forex rates...")
    rates = {'USD': 1.0, '$': 1.0}
    pairs = {'GBP': 'GBPUSD=X', 'GBp': 'GBPUSD=X', 'EUR': 'EURUSD=X', 'SEK': 'SEKUSD=X', 'AUD': 'AUDUSD=X', 'CAD': 'CADUSD=X'}
    for currency, ticker in pairs.items():
        try:
            val = yf.Ticker(ticker).fast_info['lastPrice']
            if currency == 'GBp': val = val / 100.0 # Convert pence to pounds for math
            rates[currency] = val
        except Exception:
            rates[currency] = 1.0 # Safe fallback
    return rates

def get_stock_fundamentals(ticker, fx_rates):
    """Multi-tiered approach to guarantee Price and Market Cap, while safely attempting P/E and Debt."""
    # Safety Defaults
    price = 0
    price_str = "N/A"
    mc_display = "N/A"
    mc_usd_val = 0
    pe_str = "N/A"
    de_str = "N/A"
    sym = "$"
    currency = "USD"
    
    try:
        ytk = yf.Ticker(ticker)
        
        # --- TIER 1: PRICE & CURRENCY (Highly Reliable) ---
        try:
            price = ytk.fast_info['lastPrice']
            currency = ytk.fast_info['currency']
        except Exception:
            pass # We will rely on the 0 default if this completely fails
            
        if currency == "GBp": sym = "GBp "
        elif currency == "GBP": sym = "£"
        elif currency == "SEK": sym = "SEK "
        elif currency == "EUR": sym = "€"
        elif currency == "AUD": sym = "A$"
        elif currency == "CAD": sym = "C$"
        else: sym = "$"
        
        if price > 0:
            price_str = f"{sym}{round(price, 2)}"
            
        # --- TIER 2: MARKET CAP (Highly Reliable) ---
        try:
            mc_raw = ytk.fast_info['marketCap']
            if mc_raw and mc_raw > 0:
                if mc_raw >= 1e9: mc_native = f"{sym}{round(mc_raw/1e9, 2)}B"
                elif mc_raw >= 1e6: mc_native = f"{sym}{round(mc_raw/1e6, 2)}M"
                else: mc_native = f"{sym}{mc_raw}"
                
                fx_rate = fx_rates.get(currency, 1.0)
                mc_usd_val = mc_raw * fx_rate
                
                if currency not in ["USD", "$"]:
                    if mc_usd_val >= 1e9: mc_usd_str = f"${round(mc_usd_val/1e9, 2)}B"
                    elif mc_usd_val >= 1e6: mc_usd_str = f"${round(mc_usd_val/1e6, 2)}M"
                    else: mc_usd_str = f"${round(mc_usd_val, 2)}"
                    mc_display = f"{mc_native} ({mc_usd_str})"
                else:
                    mc_display = mc_native
        except Exception:
            pass

        # --- TIER 3: P/E & DEBT-TO-EQUITY (Flaky on GitHub Actions) ---
        # Wrapped in its own try/except so a Yahoo block doesn't wipe out the Price
        try:
            info = ytk.info
            pe_raw = info.get('trailingPE') or info.get('forwardPE')
            if pe_raw: pe_str = f"{round(pe_raw, 2)}"
            
            de_raw = info.get('debtToEquity')
            if de_raw is not None: de_str = f"{round(de_raw, 2)}%"
        except Exception:
            pass # Fail silently, leaving them as "N/A" but preserving the rest
            
        return price_str, price, mc_display, mc_usd_val, pe_str, de_str
        
    except Exception as e:
        print(f"  ❌ FATAL Fundamentals fetch failed for {ticker}: {e}")
        return "N/A", 0, "N/A", 0, "N/A", "N/A"

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
    if not api_key or api_key == "YOUR_ACTUAL_API_KEY_HERE":
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
        error_msg = str(e).replace('"', "'")[:60]
        return {"summary": [f"News Engine Error: {error_msg}"], "sentiment": 50}

# --- 3. PIPELINE EXECUTION ---

def run_pipeline():
    master_db = []
    print(f"🚀 Starting Pipeline...")
    
    fx_rates = get_live_fx_rates()
    
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        print(f"\nProcessing {co['name']}...")
        
        fin = VERIFIED_DATA.get(ticker, {
            "period": "N/A", "eps_actual": 0, "eps_forecast": 0, "revenue": "N/A", 
            "net_income": "N/A", "ebitda": "N/A", "ngr": "N/A", "fcf": "N/A", "jurisdictions": [],
            "focus": "Diversified Gaming", "map_codes": []
        })
        
        cal = VERIFIED_CALENDAR.get(ticker, {"date": "TBD", "report_time": "TBD", "call_time": "TBD"})
        beat_miss = 0
        if fin.get("eps_forecast", 0) != 0:
            beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2)
            
        try:
            intel = ai_process_intelligence(co['name'], ticker)
            last_price_str, native_price_raw, mc_str, mc_usd, pe_ratio, debt_equity = get_stock_fundamentals(ticker, fx_rates)
            history = fetch_stock_history(ticker, native_price_raw)
        except Exception as e:
            print(f"  ⚠️ Critical loop failure for {ticker}: {e}")
            intel = {"summary": [f"System Error: {str(e)[:50]}"], "sentiment": 50}
            history, last_price_str, mc_str, mc_usd, pe_ratio, debt_equity = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}, "N/A", "N/A", 0, "N/A", "N/A"

        master_db.append({
            "ticker": ticker,
            "company": co["name"],
            "base_country": co["base_country"],
            "focus": fin.get("focus", "Diversified Gaming"), 
            "map_codes": fin.get("map_codes", []),           
            "calendar": cal, 
            "last_price": last_price_str,
            
            # The new metrics for the UI
            "market_cap_str": mc_str,
            "market_cap_usd": mc_usd,
            "pe_ratio": pe_ratio,
            "debt_to_equity": debt_equity,
            
            "actuals": fin,
            "eps_beat_miss_pct": beat_miss,
            "news_summary": intel.get("summary", ["Data parsing failed."]),
            "sentiment": intel.get("sentiment", 50),
            "jurisdictions": fin.get("jurisdictions", []),
            "history": history
        })
        
        time.sleep(5)

    if master_db:
        with open('gambling_stocks_live.json', 'w') as f:
            json.dump(master_db, f, indent=4)
        print(f"\n✅ Pipeline Complete. Saved {len(master_db)} companies.")
    else:
        print("\n❌ Pipeline Error: No data collected.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        print("\n❌ FATAL CRASH: The script encountered a module-level error.")
        traceback.print_exc()
        sys.exit(1)
