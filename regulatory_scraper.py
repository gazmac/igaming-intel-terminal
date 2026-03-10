import json
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random
import os
import io
import re
from bs4 import BeautifulSoup
import PyPDF2
from google import genai

# --- 1. CONFIGURATION ---
STATES = {
    "NY": {"name": "New York", "base_handle": 1200, "tax_rate": 0.51},
    "NJ": {"name": "New Jersey", "base_handle": 950, "tax_rate": 0.1425},
    "PA": {"name": "Pennsylvania", "base_handle": 700, "tax_rate": 0.36},
    "MI": {"name": "Michigan", "base_handle": 450, "tax_rate": 0.084},
    "OH": {"name": "Ohio", "base_handle": 600, "tax_rate": 0.20},
    "IL": {"name": "Illinois", "base_handle": 850, "tax_rate": 0.15}
}

os.makedirs("data_drops", exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}

def get_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key: return None
    return genai.Client(api_key=api_key)

def process_ai_extraction(text_data, state_name):
    client = get_gemini_client()
    if not client: return None
    
    prompt = f"""You are a forensic financial analyst. Review this raw regulatory gaming data for {state_name}.
    Extract the total Statewide Handle and Gross Gaming Revenue (GGR) for the MOST RECENT month available.
    Also, extract the Handle and GGR for EACH SPECIFIC OPERATOR (e.g., FanDuel, DraftKings, BetMGM, Caesars, Rush Street, etc.) for that SAME month.
    Convert all monetary values to Millions (e.g., 1,500,000,000 becomes 1500.0).

    Return STRICTLY a JSON object in this exact format (no markdown blocks, no conversational text):
    {{
        "month": "Month Year",
        "statewide": {{"handle": 1500.0, "ggr": 150.0}},
        "operators": [
            {{"name": "FanDuel", "handle": 600.0, "ggr": 75.0}},
            {{"name": "DraftKings", "handle": 500.0, "ggr": 50.0}}
        ]
    }}
    Data: {text_data[:35000]}"""
    
    try:
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        raw_output = response.text.strip()
        print(f"\n--- AI RAW OUTPUT FOR {state_name} ---")
        print(raw_output[:500] + "... [truncated]") 
        
        clean_json = re.sub(r'^```json\s*', '', raw_output)
        clean_json = re.sub(r'^```\s*', '', clean_json)
        clean_json = re.sub(r'```$', '', clean_json).strip()
        
        return json.loads(clean_json)
    except Exception as e:
        print(f"❌ AI Extraction Error for {state_name}: {e}")
        return None

# --- 2. NY AGENT (LIVE WEB EXCEL) ---
def scrape_ny():
    print("\nChecking New York (Live API)...")
    try:
        url = "[https://gaming.ny.gov/revenue-reports](https://gaming.ny.gov/revenue-reports)"
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        
        link = None
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            text = a.text.lower()
            # Look for "statewide" and either "excel" OR the actual .xlsx file extension
            if 'statewide' in text and ('excel' in text or href.endswith('.xlsx') or href.endswith('.xls')):
                link = a['href'] if a['href'].startswith('http') else "[https://gaming.ny.gov](https://gaming.ny.gov)" + a['href']
                break
        
        if not link:
            print("  -> Could not find the NY Excel link on the webpage. Structure may have changed.")
            return None

        print(f"  -> Found NY link: {link}")
        xl_res = requests.get(link, headers=HEADERS)
        df = pd.read_excel(io.BytesIO(xl_res.content))
        raw_text = df.head(150).to_csv(index=False) 
        
        return process_ai_extraction(raw_text, "New York")
    except Exception as e:
        print(f"❌ NY Error: {e}")
        return None

# --- 3. DROP ZONE AGENT (LOCAL FILES) ---
def process_local_drop(state_code):
    print(f"\nChecking Data Drops for {state_code}...")
    base_path = f"data_drops/{state_code}_revenue"
    raw_text = ""
    
    try:
        if os.path.exists(base_path + ".pdf"):
            print(f"  -> Found PDF for {state_code}. Reading...")
            reader = PyPDF2.PdfReader(base_path + ".pdf")
            for page in reader.pages[:4]: raw_text += page.extract_text()
        elif os.path.exists(base_path + ".xlsx"):
            print(f"  -> Found Excel for {state_code}. Reading...")
            df = pd.read_excel(base_path + ".xlsx")
            raw_text = df.head(200).to_csv(index=False)
        elif os.path.exists(base_path + ".csv"):
            print(f"  -> Found CSV for {state_code}. Reading...")
            df = pd.read_csv(base_path + ".csv")
            raw_text = df.head(200).to_csv(index=False)
        else:
            print(f"  -> No local file found for {state_code} in data_drops/")
            return None
            
        return process_ai_extraction(raw_text, STATES[state_code]["name"])
    except Exception as e:
        print(f"❌ Error reading local file for {state_code}: {e}")
        return None

# --- 4. FALLBACK GENERATOR (CHARTS & DUMMY DATA) ---
def generate_dummy_history(config):
    data = []
    curr = datetime.now()
    for i in range(12): 
        m = (curr - relativedelta(months=i)).strftime("%b %Y")
        h = config['base_handle'] * random.uniform(0.9, 1.1)
        g = h * random.uniform(0.07, 0.11)
        data.append({"month": m, "handle": round(h,1), "hold": round((g/h)*100,1), "ggr": round(g,1), "ngr": round(g*(1-config['tax_rate']),1)})
    return data

def generate_dummy_operators(config):
    return [
        {"name": "FanDuel", "handle": config['base_handle']*0.4, "ggr": config['base_handle']*0.4*0.11},
        {"name": "DraftKings", "handle": config['base_handle']*0.35, "ggr": config['base_handle']*0.35*0.09},
        {"name": "BetMGM", "handle": config['base_handle']*0.15, "ggr": config['base_handle']*0.15*0.08},
        {"name": "Caesars", "handle": config['base_handle']*0.10, "ggr": config['base_handle']*0.10*0.07}
    ]

# --- 5. PIPELINE EXECUTION ---
def run():
    results = {}
    
    for code, config in STATES.items():
        ai_data = scrape_ny() if code == "NY" else process_local_drop(code)
        
        hist = generate_dummy_history(config) 
        
        if ai_data and isinstance(ai_data, dict) and "operators" in ai_data:
            print(f"  ✅ AI Data verified and integrated for {code}")
            hist[0] = {
                "month": ai_data.get("month", "Recent"),
                "handle": round(ai_data["statewide"]["handle"], 1),
                "ggr": round(ai_data["statewide"]["ggr"], 1),
                "hold": round((ai_data["statewide"]["ggr"] / ai_data["statewide"]["handle"]) * 100, 1) if ai_data["statewide"]["handle"] > 0 else 0,
                "ngr": round(ai_data["statewide"]["ggr"] * (1 - config["tax_rate"]), 1)
            }
            
            operators = []
            for op in ai_data["operators"]:
                h, g = op.get("handle", 0), op.get("ggr", 0)
                operators.append({
                    "name": op.get("name", "Unknown"),
                    "handle": round(h, 1),
                    "ggr": round(g, 1),
                    "hold": round((g / h) * 100, 1) if h > 0 else 0,
                    "ngr": round(g * (1 - config["tax_rate"]), 1)
                })
            
            operators = sorted(operators, key=lambda x: x["handle"], reverse=True)
            
            results[code] = {
                "source": "AI Extracted Data" if code == "NY" else "AI Extracted (Local Drop)", 
                "history": hist, 
                "operators": operators,
                "latest_month": ai_data.get("month", "Recent")
            }
        else:
            print(f"  ⚠️ Falling back to Dummy Data for {code}")
            results[code] = {
                "source": "Simulated Dummy Data", 
                "history": hist, 
                "operators": generate_dummy_operators(config),
                "latest_month": hist[0]["month"]
            }

    with open('regulatory_data.json', 'w') as f:
        json.dump(results, f, indent=4)

if __name__ == "__main__":
    run()
