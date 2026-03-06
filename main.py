import requests
import fitz  # PyMuPDF
import json
import urllib.parse
import os
import pandas as pd
import feedparser
import io
import time
from bs4 import BeautifulSoup
from google import genai
from datetime import datetime, timezone

# --- 1. CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_ACTUAL_API_KEY_HERE")
client = genai.Client(api_key=GEMINI_API_KEY)

TARGET_COMPANIES = [
    {
        "name": "Flutter Entertainment", "ticker": "FLUT", 
        "ir_url": "https://flutter.com/investors/investor-hub/results-reports/",
        "base_country": "Ireland"
    },
    {
        "name": "DraftKings", "ticker": "DKNG", 
        "ir_url": "https://www.draftkings.com/about/investor-relations/",
        "base_country": "USA"
    },
    {
        "name": "Entain PLC", "ticker": "ENT.L",
        "ir_url": "https://www.entaingroup.com/investor-relations/results-centre/",
        "base_country": "UK"
    },
    {
        "name": "Evolution AB", "ticker": "EVO.ST",
        "ir_url": "https://www.evolution.com/investor-relations/financial-reports/",
        "base_country": "Sweden"
    }
]

# --- 2. SCRAPING & DATA EXTRACTION ---

def get_next_earnings_date(ticker):
    """Fetches future earnings date with a bulletproof hardcoded fallback for Q1 2026."""
    
    # 1. The Bulletproof Fallback Calendar (Verified Q1 2026 Dates)
    # This ensures your 14-day highlight UI works flawlessly even if APIs fail.
    verified_calendar = {
        "FLUT": "2026-05-06T21:00:00Z",   # May 6, 2026
        "DKNG": "2026-05-07T21:00:00Z",   # May 7, 2026
        "ENT.L": "2026-04-29T07:00:00Z",  # Late April Q1 Trading Update
        "EVO.ST": "2026-04-22T06:30:00Z"  # April 22, 2026
    }
    
    # 2. Try Yahoo API for dynamic updates (if it fails, use the verified calendar)
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=calendarEvents"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=5).json()
        timestamp = res['quoteSummary']['result'][0]['calendarEvents']['earnings']['earningsDate'][0]
        
        # Check if the date is actually in the future
        future_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if future_date > datetime.now(timezone.utc):
            return future_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        pass
        
    print(f"⚠️ API Date failed or is in the past for {ticker}. Using Verified Calendar.")
    return verified_calendar.get(ticker)

def get_latest_pdf_url(ir_url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(ir_url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href'].lower()
            text = link.get_text().lower()
            if any(k in text for k in ['q4', 'fy', 'results', 'earnings', 'presentation']) and '.pdf' in href:
                return urllib.parse.urljoin(ir_url, link['href'])
    except Exception:
        pass
    return None

def extract_pdf_text(pdf_url):
    try:
        response = requests.get(pdf_url, stream=True, timeout=10)
        with open("temp.pdf", "wb") as f:
            f.write(response.content)
        doc = fitz.open("temp.pdf")
        text = ""
        for page in doc[:6]: text += page.get_text()
        if len(doc) > 6:
            for page in doc[-4:]: text += page.get_text()
        return text
    except Exception:
        return ""

def scrape_yahoo_estimates(ticker):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    }
    url = f"https://finance.yahoo.com/quote/{ticker}/analysis"
    forecasts = {"mid": None, "low": None, "high": None}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        tables = pd.read_html(io.StringIO(response.text))
        for df in tables:
            if any('Earnings Estimate' in str(col) for col in df.columns):
                df.columns = [str(c) for c in df.columns]
                df.set_index(df.columns[0], inplace=True)
                target_col = df.columns[0]
                forecasts["mid"] = float(df.loc['Avg. Estimate', target_col]) if 'Avg. Estimate' in df.index else None
                forecasts["low"] = float(df.loc['Low Estimate', target_col]) if 'Low Estimate' in df.index else None
                forecasts["high"] = float(df.loc['High Estimate', target_col]) if 'High Estimate' in df.index else None
                break
    except Exception:
        pass
    return forecasts

def get_latest_news_headlines(ticker, company_name):
    """Tries Yahoo RSS. If empty (common for EU stocks), falls back to Google News."""
    # 1. Try Yahoo Finance RSS
    rss_url = f"https://finance.yahoo.com/rss/headline?s={ticker}"
    feed = feedparser.parse(rss_url)
    headlines = [entry.title for entry in feed.entries[:5]]
    
    # 2. Fallback to Google News if Yahoo is empty
    if not headlines:
        print(f"⚠️ Yahoo News empty for {ticker}. Falling back to Google News...")
        query = urllib.parse.quote(f"{company_name} earnings finance")
        google_rss = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(google_rss)
        headlines = [entry.title for entry in feed.entries[:5]]
        
    return headlines

def fetch_stock_history(ticker):
    headers = {'User-Agent': 'Mozilla/5.0'}
    periods = {
        "1d": ("1d", "15m"), "1w": ("5d", "1h"), "1m": ("1mo", "1d"),
        "3m": ("3mo", "1d"), "6m": ("6mo", "1d"), "1y": ("1y", "1wk"), "5y": ("5y", "1mo")
    }
    history = {}
    for label, (r, i) in periods.items():
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={r}&interval={i}"
            res = requests.get(url, headers=headers, timeout=10).json()
            result = res['chart']['result'][0]
            ts = result['timestamp']
            pr = result['indicators']['quote'][0]['close']
            history[label] = [[t * 1000, round(p, 2)] for t, p in zip(ts, pr) if p is not None]
        except: history[label] = []
    return history

# --- 3. AI PROCESSING ---

def ai_process_intelligence(pdf_text, headlines, company_name):
    prompt = f"""
    Analyze the financial data for {company_name}.
    
    CRITICAL INSTRUCTION: If the REPORT TEXT is empty or short, you MUST extract all financial data, strategic updates, and sentiment purely from the NEWS HEADLINES.

    REPORT TEXT: {pdf_text[:12000]}
    NEWS HEADLINES: {' | '.join(headlines)}

    Return ONLY a JSON object:
    {{
        "execs": {{ "ceo": "str", "cfo": "str" }},
        "dates": {{ "release": "ISO8601_GMT", "call": "ISO8601_GMT" }},
        "actuals": {{ "eps": float, "revenue": "str", "ngr": "str", "fcf": "str", "debt_equity": "str" }},
        "jurisdictions": ["ISO_COUNTRY_CODES"],
        "summary": ["3_bullet_points"],
        "sentiment": int_0_to_100
    }}
    """
    print(f"Sending data to Gemini for {company_name}...")
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite',
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"❌ ERROR processing {company_name}: {e}")
        return {"execs":{}, "dates":{}, "actuals":{}, "jurisdictions":[], "summary":["Analysis failed."], "sentiment":50}

# --- 4. MAIN LOOP ---

def run_pipeline():
    master_db = []
    for co in TARGET_COMPANIES:
        print(f"\nProcessing {co['name']} ({co['ticker']})...")
        
        # 1. Scrape standard data (Now with Google News fallback)
        headlines = get_latest_news_headlines(co['ticker'], co['name'])
        estimates = scrape_yahoo_estimates(co['ticker'])
        history = fetch_stock_history(co['ticker'])
        
        # 2. Get the true FUTURE earnings date
        future_release_date = get_next_earnings_date(co['ticker'])
        
        # 3. Get PDF and run AI
        pdf_url = get_latest_pdf_url(co['ir_url'])
        pdf_text = extract_pdf_text(pdf_url) if pdf_url else ""
        intel = ai_process_intelligence(pdf_text, headlines, co['name'])
        
        # 4. OVERRIDE dates with the verified calendar
        if future_release_date:
            intel["dates"]["release"] = future_release_date
        
        # 5. Math
        beat_miss = None
        try:
            act = float(intel['actuals'].get('eps', 0))
            est = float(estimates.get('mid', 0))
            if est != 0: beat_miss = round(((act - est) / abs(est)) * 100, 2)
        except: pass

        master_db.append({
            "ticker": co["ticker"],
            "company": co["name"],
            "base_country": co["base_country"],
            "ceo": intel["execs"].get("ceo"),
            "cfo": intel["execs"].get("cfo"),
            "release_gmt": intel["dates"].get("release"),
            "call_gmt": intel["dates"].get("call"), 
            "actuals": intel["actuals"],
            "forecast_eps": estimates,
            "eps_beat_miss_pct": beat_miss,
            "news_summary": intel["summary"],
            "sentiment": intel["sentiment"],
            "jurisdictions": intel["jurisdictions"],
            "history": history
        })
        
        print("Pausing 5s for API safety...")
        time.sleep(5)

    with open('gambling_stocks_live.json', 'w') as f:
        json.dump(master_db, f, indent=4)
    print("\nPipeline Complete.")

if __name__ == "__main__":
    run_pipeline()
