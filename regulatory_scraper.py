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

# --- 1. STATE CONFIGURATIONS ---
STATES = {
    "NY": {"name": "New York", "base_handle": 1200, "tax_rate": 0.51},
    "PA": {"name": "Pennsylvania", "base_handle": 650, "tax_rate": 0.36},
    "NJ": {"name": "New Jersey", "base_handle": 900, "tax_rate": 0.1425},
    "MI": {"name": "Michigan", "base_handle": 400, "tax_rate": 0.084},
    "OH": {"name": "Ohio", "base_handle": 600, "tax_rate": 0.20},
    "IL": {"name": "Illinois", "base_handle": 850, "tax_rate": 0.15}
}

# --- 2. LIVE GOVERNMENT API (NEW YORK) ---
def scrape_live_ny_data():
    """Pulls live, real-time aggregate data from the NYSGC Open Data API."""
    print("    -> Connecting to NYSGC Open Data API...")
    url = "https://data.ny.gov/resource/wbg7-vjc8.json?$limit=5000"
    
    try:
        req = requests.get(url, timeout=10)
        req.raise_for_status()
        df = pd.DataFrame(req.json())
        
        df['week_ending_date'] = pd.to_datetime(df['week_ending_date'])
        df['mobile_sports_wagering_handle'] = pd.to_numeric(df['mobile_sports_wagering_handle'], errors='coerce').fillna(0)
        df['mobile_sports_wagering_ggr'] = pd.to_numeric(df['mobile_sports_wagering_ggr'], errors='coerce').fillna(0)
        
        df['month_year'] = df['week_ending_date'].dt.to_period('M')
        monthly = df.groupby('month_year').sum(numeric_only=True).reset_index()
        monthly = monthly.sort_values('month_year', ascending=False).head(12)
        
        data = []
        for _, row in monthly.iterrows():
            handle = row['mobile_sports_wagering_handle'] / 1e6 
            ggr = row['mobile_sports_wagering_ggr'] / 1e6
            hold = (ggr / handle * 100) if handle > 0 else 0
            ngr = ggr * (1 - STATES["NY"]["tax_rate"]) * 0.90 
            
            data.append({
                "month": row['month_year'].strftime('%b %Y'),
                "handle": round(handle, 1),
                "hold": round(hold, 1),
                "ggr": round(ggr, 1),
                "ngr": round(ngr, 1)
            })
            
        return data[::-1]
    except Exception as e:
        print(f"    ❌ NY API Failed: {e}")
        return None

# --- 3. AI AGENT EXTRACTOR (PENNSYLVANIA) ---
def scrape_pa_agent():
    """Hunts down the latest PGCB Excel files and uses Gemini to extract structured data."""
    print("    -> [AI AGENT] Initiating Pennsylvania Hunt...")
    base_url = "https://gamingcontrolboard.pa.gov/news-and-transparency/revenue"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        # Step 1: The Hunter (Find the files)
        req = requests.get(base_url, headers=headers, timeout=10)
        soup = BeautifulSoup(req.text, 'html.parser')
        
        fy_links = []
        for a in soup.find_all('a', href=True):
            if "sports-wagering-fy" in a['href']:
                full_link = a['href'] if a['href'].startswith('http') else "https://gamingcontrolboard.pa.gov" + a['href']
                if full_link not in fy_links:
                    fy_links.append(full_link)
                    
        # Grab the top 2 (Current FY and Previous FY to ensure we have 12 trailing months)
        fy_links = fy_links[:2]
        all_csv_data = ""
        
        for link in fy_links:
            pg_req = requests.get(link, headers=headers)
            pg_soup = BeautifulSoup(pg_req.text, 'html.parser')
            for a in pg_soup.find_all('a', href=True):
                if '.xlsx' in a['href']:
                    xl_link = a['href'] if a['href'].startswith('http') else "https://gamingcontrolboard.pa.gov" + a['href']
                    
                    # Step 2: The Formatter (Download and strip to text)
                    xl_data = requests.get(xl_link, headers=headers).content
                    df = pd.read_excel(io.BytesIO(xl_data), sheet_name=0)
                    all_csv_data += df.to_csv(index=False) + "\n\n"
                    break
        
        if not all_csv_data:
            raise Exception("Could not locate Excel files on PA site.")
            
        print("    -> [AI AGENT] Data extracted. Feeding to Gemini for semantic parsing...")
        
        # Step 3: The Brain (Gemini Extraction)
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise Exception("GEMINI_API_KEY missing.")
            
        client = genai.Client(api_key=api_key)
        
        prompt = f"""You are a forensic data analyst. Below is the raw CSV text extracted from the Pennsylvania Gaming Control Board's monthly sports wagering Excel reports.
Your task: Find the "Statewide Total" row for each month. 
Extract the Total Handle, Gross Revenue (GGR), and State Tax for the trailing 12 most recent months available in the data. All numbers should be converted to Millions (e.g. 500,000,000 becomes 500.0).
Note: Calculate Net Gaming Revenue (NGR) as: GGR - State Tax. Calculate Hold % as (GGR / Handle) * 100.

Return the data STRICTLY as a JSON array of objects. Do NOT wrap it in markdown blockquotes. Start immediately with [ and end with ].
Example format:
[
  {{"month": "Feb 2026", "handle": 850.5, "hold": 9.2, "ggr": 78.2, "ngr": 50.1}},
  {{"month": "Jan 2026", "handle": 800.1, "hold": 8.5, "ggr": 68.0, "ngr": 43.5}}
]

Raw CSV Data:
{all_csv_data[:35000]}""" 
        
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
            print("    -> [AI AGENT] Success! PA Data structured.")
            data.sort(key=lambda x: datetime.strptime(x['month'], "%b %Y"))
            return data
            
        raise Exception("Invalid JSON structure returned by Gemini.")
        
    except Exception as e:
        print(f"    ❌ PA AI Agent Failed: {e}")
        return None

# --- 4. ALGORITHMIC PROXY (NON-AGENT STATES) ---
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

# --- 5. EXECUTION PIPELINE ---
def run_scraper():
    print("🛡️ Starting Regulatory Data Pipeline...")
    master_regulatory_data = {}
    
    for state_code, config in STATES.items():
        print(f"Processing {config['name']} ({state_code})...")
        
        if state_code == "NY":
            live_data = scrape_live_ny_data()
            if live_data:
                master_regulatory_data[state_code] = {"source": "Live Government API", "data": live_data}
            else:
                master_regulatory_data[state_code] = {"source": "Algorithmic Proxy", "data": generate_t12m_baseline(state_code, config)}
                
        elif state_code == "PA":
            pa_data = scrape_pa_agent()
            if pa_data:
                master_regulatory_data[state_code] = {"source": "AI Extracted Data", "data": pa_data}
            else:
                master_regulatory_data[state_code] = {"source": "Algorithmic Proxy", "data": generate_t12m_baseline(state_code, config)}
                
        else:
            master_regulatory_data[state_code] = {"source": "Algorithmic Proxy", "data": generate_t12m_baseline(state_code, config)}

    with open('regulatory_data.json', 'w') as f:
        json.dump(master_regulatory_data, f, indent=4)
        
    print(f"✅ Regulatory Pipeline Complete.")

if __name__ == "__main__":
    run_scraper()
