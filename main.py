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
from datetime import datetime

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

def get_latest_pdf_url(ir_url):
    """Scrapes IR page to find the most recent earnings PDF link."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(ir_url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href'].lower()
            text = link.get_text().lower()
            if any(k in text for k in ['q4', 'fy', 'results', 'earnings', 'presentation']) and '.pdf' in href:
                return urllib.parse.urljoin(ir_url, link['href'])
        return None
    except Exception as e:
        print(f"Error scraping IR page {ir_url}: {e}")
        return None

def extract_pdf_text(pdf_url):
    """Bifocal extraction: Scans the start (narrative) and end (tables) of the PDF."""
    try:
        response = requests.get(pdf_url, stream=True, timeout=20)
        with open("temp.pdf", "wb") as f:
            f.write(response.content)
        doc = fitz.open("temp.pdf")
        text = ""
        # Get first 6 pages (Executive summary)
        for page in doc[:6]:
            text += page.get_text()
        # Get last 4 pages (Financial tables)
        if len(doc) > 6:
            for page in doc[-4:]:
                text += page.get_text()
        return text
    except Exception as e:
        print(f"Error reading PDF: {e}")
        return ""

def scrape_yahoo_estimates(ticker):
    """Stealth scraper for Yahoo Finance Analysis tab."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    url = f"https://finance.yahoo.com/quote/{ticker}/analysis"
    forecasts = {"mid": None, "low": None, "high": None}
    try:
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=15)
        
        if "Earnings Estimate" not in response.text:
            print(f"⚠️ Yahoo blocked {ticker} or table missing.")
            return forecasts

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
    except Exception as e:
        print(f"Error scraping Yahoo estimates for {ticker}: {e}")
    return forecasts

def get_latest_news_headlines(ticker):
    """Fetches top 5 headlines from Yahoo Finance RSS."""
    rss_url = f"https://finance.yahoo.com/rss/headline?s={ticker}"
    feed = feedparser.parse(rss_url)
    return [entry.title for entry in feed.entries[:5]]

def fetch_stock_history(ticker):
    """Fetches price data for ranges: 1d, 1w, 1m, 3m, 6m, 1y, 5y."""
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
    """Unified AI call with hints for improved extraction accuracy."""
    prompt = f"""
    Analyze the financial data for {company_name}.
    
    HINTS: 
    - Look for CFO name in the board or financial review sections.
    - Financials (Revenue/NGR) might be in GBP, EUR, or USD; use the reported currency.
    - If the PDF text is empty, prioritize news headlines for sentiment and summary.

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
    print(f"Sending data to Gemini for {company_name} (Text size: {len(pdf_text)})...")
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
        print(f"\nProcessing {co['name']}...")
        
        headlines = get_latest_news_headlines(co['ticker'])
        estimates = scrape_yahoo_estimates(co['ticker'])
        history = fetch_stock_history(co['ticker'])
        
        pdf_url = get_latest_pdf_url(co['ir_url'])
        pdf_text = extract_pdf_text(pdf_url) if pdf_url else ""
        
        intel = ai_process_intelligence(pdf_text, headlines, co['name'])
        
        # Calculate Beat/Miss %
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
        
        # Rate-limiting pause
        print("Pausing 5s for API safety...")
        time.sleep(5)

    with open('gambling_stocks_live.json', 'w') as f:
        json.dump(master_db, f, indent=4)
    print("\nPipeline Complete.")

if __name__ == "__main__":
    run_pipeline()
