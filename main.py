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
from google import genai
import warnings
warnings.filterwarnings('ignore')

# --- 1. STATE CONFIGURATIONS ---
STATES = {
    "NY": {"name": "New York", "base_handle": 1200, "tax_rate": 0.51},
    "PA": {"name": "Pennsylvania", "base_handle": 650, "tax_rate": 0.36},
    "NJ": {"name": "New Jersey", "base_handle": 900, "tax_rate": 0.1425},
    "MI": {"name": "Michigan", "base_handle": 400, "tax_rate": 0.084},
    "OH": {"name": "Ohio", "base_handle": 600, "tax_rate": 0.20},
    "IL": {"name": "Illinois", "base_handle": 850, "tax_rate": 0.15}
}

# --- 2. AI AGENT EXTRACTOR (NEW YORK) ---
def scrape_ny_agent():
    """Hunts down the latest NYSGC Excel file and uses Gemini to extract structured data."""
    print("    -> [AI AGENT] Hunting for NYSGC Excel Report...")
    url = "https://gaming.ny.gov/revenue-reports"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        req = requests.get(url, headers=headers, timeout=10, verify=False)
        soup = BeautifulSoup(req.text, 'html.parser')
        
        excel_link = None
        for a in soup.find_all('a', href=True):
            if 'Statewide' in a.text and 'Excel' in a.text:
                excel_link = a['href']
                if not excel_link.startswith('http'):
                    excel_link = "https://gaming.ny.gov" + excel_link
                break
                
        if not excel_link:
            raise Exception("Could not find the NYSGC Statewide Excel link on the page.")
            
        print(f"    -> [AI AGENT] Found file: {excel_link}. Downloading and formatting...")
        
        xl_req = requests.get(excel_link, headers=headers, verify=False)
        df = pd.read_excel(io.BytesIO(xl_req.content), sheet_name=0)
        raw_csv = df.head(100).to_csv(index=False)
        
        print("    -> [AI AGENT] Data formatted. Handing over to Gemini Brain...")
        
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise Exception("GEMINI_API_KEY missing.")
            
        client = genai.Client(api_key=api_key)
        
        prompt = f"""You are a forensic financial data analyst. Below is the raw CSV text extracted from the New York State Gaming Commission's weekly mobile sports wagering report.
Your task: Aggregate the weekly data into MONTHLY totals.
Extract the Statewide Total Handle and Gross Gaming Revenue (GGR) for the 12 most recent completed months available in the data.
Calculate Net Gaming Revenue (NGR) as: GGR * 0.49 (because NY tax is 51%). Calculate Hold % as (GGR / Handle) * 100.
Convert all monetary values to Millions (e.g. 1,500,000,000 becomes 1500.0).

Return the data STRICTLY as a JSON array of objects ordered from newest month to oldest. Do NOT wrap it in markdown blockquotes. Start immediately with [ and end with ].
Example format:
[
  {{"month": "Dec 2025", "handle": 1850.5, "hold": 9.2, "ggr": 178.2, "ngr": 87.3}},
  {{"month": "Nov 2025", "handle": 1800.1, "hold": 8.5, "ggr": 168.0, "ngr": 82.3}}
]

Raw CSV Data:
{raw_csv[:25000]}""" 
        
        ai_resp = client.models.generate_content(
            model='gemini-2.5-flash', 
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        
        raw_text = ai_resp.text.strip()
        raw_text = re.sub(r'^```json\s*', '', raw_text)
        raw_text = re.sub(r'^```\s*', '', raw_text)
        raw_text = re.sub(r'\s*```$', '', raw_text)
        
        data = json.loads(raw_text)
        
        if isinstance(data, list) and len(data) > 0:
            print("    -> [AI AGENT] Success! NY Data structured.")
            return data
            
        raise Exception("Invalid JSON structure returned by Gemini.")
        
    except Exception as e:
        print(f"    ❌ NY AI Agent Failed: {e}")
        return None

# --- 3. DUMMY DATA GENERATOR (NON-AGENT STATES) ---
def generate_t12m_baseline(state_code, config):
    data = []
    current_date = datetime.now() - relativedelta(months=1)
    
    for i in range(12):
        month_str = current_date.strftime("%b %Y")
        is_nfl_season = current_date.month in [9, 10, 11, 12]
        seasonal_multiplier = 1.3 if is_nfl_season else 0.9
        
        handle = config["base_handle"] * seasonal_multiplier * random.uniform(0.9, 1.1)
        hold_pct = random.uniform(7.5, 11.5) 
        ggr = handle * (hold_pct / 100)
        ngr = ggr * (1 - config["tax_rate"]) * 0.90
        
        data.insert(0, {
            "month": month_str,
            "handle": round(handle, 1),
            "hold": round(hold_pct, 1),
            "ggr": round(ggr, 1),
            "ngr": round(ngr, 1)
        })
        current_date -= relativedelta(months=1)
        
    return data

# --- 4. EXECUTION PIPELINE ---
def run_scraper():
    print("🛡️ Starting Regulatory Data Pipeline...")
    master_regulatory_data = {}
    
    for state_code, config in STATES.items():
        print(f"Processing {config['name']} ({state_code})...")
        
        if state_code == "NY":
            ny_data = scrape_ny_agent()
            if ny_data:
                master_regulatory_data[state_code] = {"source": "AI Extracted Data", "data": ny_data}
            else:
                master_regulatory_data[state_code] = {"source": "Simulated Dummy Data", "data": generate_t12m_baseline(state_code, config)}
                
        elif state_code == "PA":
            print("    -> [INFO] PGCB uses Imperva Anti-Bot Firewall. Bypassing cloud scraping and engaging dummy proxy...")
            master_regulatory_data[state_code] = {"source": "Simulated Dummy Data", "data": generate_t12m_baseline(state_code, config)}
                
        else:
            master_regulatory_data[state_code] = {"source": "Simulated Dummy Data", "data": generate_t12m_baseline(state_code, config)}

    with open('regulatory_data.json', 'w') as f:
        json.dump(master_regulatory_data, f, indent=4)
        
    print(f"✅ Regulatory Pipeline Complete.")

if __name__ == "__main__":
    run_scraper()
