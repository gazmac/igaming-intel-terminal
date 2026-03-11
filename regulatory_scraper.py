import json
import pandas as pd
from datetime import datetime
import os
import re
from google import genai
import warnings
warnings.filterwarnings('ignore')

TARGET_TICKERS = [
    "FLUT", "DKNG", "ENT.L", "MGM", "CZR", "PENN", "RSI", "BALY", 
    "SGHC", "EVOK.L", "BETS-B.ST", "CHDN", "BYD", "RRR", "GDEN", "MCRI",
    "WYNN", "LVS", "FLL", "CNTY", "INSE", "GENI"
]

def get_ai_brand_mapping(entity_brand_pairs):
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    if not client: return {}
    
    prompt = f"""You are an expert in US iGaming. Map "Entity | Brand" strings to parent tickers: {TARGET_TICKERS}.
    
    CRITICAL RULES:
    1. PRIORITIZE BRAND OVER ENTITY: If the Brand is 'DraftKings' or 'BetMGM', map to DKNG or MGM even if the Entity is 'Hollywood Casino'.
    2. PENN MAPPING: Map 'ESPN BET', 'theScore', or any Penn-operated Hollywood/Ameristar BRAND to 'PENN'. 
    3. EXCLUDE FROM PENN: 'Hollywood | DraftKings' -> DKNG, 'Hollywood | BetMGM' -> MGM, 'Hollywood | Barstool' -> PRIVATE, 'Hollywood | PointsBet' -> PRIVATE.
    4. FLUTTER: 'FanDuel' or 'MotorCity' -> FLUT.
    5. CHURCHILL: 'TwinSpires', 'Derby City', 'Ellis Park', 'Turfway' -> CHDN.
    
    Pairs to map: {entity_brand_pairs}
    Return ONLY a JSON dictionary."""
    
    try:
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        clean_json = re.sub(r'^```json\s*|```$', '', response.text.strip())
        return json.loads(clean_json)
    except: return {}

def process_excel_file(file_path):
    xls = pd.ExcelFile(file_path)
    dfs = [pd.read_excel(xls, s).assign(Vertical=s.upper()) for s in xls.sheet_names if s.upper() in ['CASINO', 'SPORTS']]
    df = pd.concat(dfs, ignore_index=True)
    
    df.rename(columns={'Period': 'Date', 'Revenue - Taxable': 'Taxable_Rev', 'Tax - State': 'State_Tax'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date', 'Brand'])
    
    # 12-Month Window
    max_date = df['Date'].max()
    df = df[df['Date'] > (max_date - pd.DateOffset(months=12))]
    df['Month_Str'] = df['Date'].dt.strftime('%b %Y')
    
    for col in ['Handle', 'Revenue', 'Taxable_Rev', 'State_Tax']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    df['Net_Rev'] = df['Taxable_Rev'] - df['State_Tax']
    df['Entity_Brand'] = df['Entity'].astype(str) + " | " + df['Brand'].astype(str)
    
    brand_to_ticker = get_ai_brand_mapping(df['Entity_Brand'].unique().tolist())
    df['Ticker'] = df['Entity_Brand'].map(lambda x: brand_to_ticker.get(x, 'PRIVATE'))
    df = df[df['Ticker'].isin(TARGET_TICKERS)]

    master_db = {}
    all_months = sorted(df['Month_Str'].unique(), key=lambda x: datetime.strptime(x, '%b %Y'))

    for ticker, t_df in df.groupby('Ticker'):
        master_db[ticker] = {"summary": {"casino_gross": 0, "casino_net": 0, "sports_gross": 0, "sports_net": 0}, "Casino": {}, "Sports": {}}
        
        # Aggregate Totals
        for v in ["Casino", "Sports"]:
            v_data = t_df[t_df['Vertical'] == v.upper()]
            master_db[ticker]["summary"][f"{v.lower()}_gross"] = round(v_data['Revenue'].sum() / 1e6, 2)
            master_db[ticker]["summary"][f"{v.lower()}_net"] = round(v_data['Net_Rev'].sum() / 1e6, 2)

        for vertical, v_df in t_df.groupby('Vertical'):
            vert_key = "Casino" if vertical == "CASINO" else "Sports"
            for state, s_df in v_df.groupby('State'):
                brand_totals = []
                for brand, b_df in s_df.groupby('Brand'):
                    brand_totals.append({
                        "brand": brand, "handle_12m": round(b_df['Handle'].sum() / 1e6, 2),
                        "revenue_12m": round(b_df['Revenue'].sum() / 1e6, 2), "net_rev_12m": round(b_df['Net_Rev'].sum() / 1e6, 2)
                    })
                
                trend_df = s_df.groupby(['Date', 'Month_Str']).sum(numeric_only=True).reset_index()
                state_max_actual = trend_df[trend_df['Revenue'] > 0]['Date'].max()
                
                state_trend = []
                for m in all_months:
                    row = trend_df[trend_df['Month_Str'] == m]
                    m_date = datetime.strptime(m, '%b %Y')
                    if not row.empty and (state_max_actual and m_date <= state_max_actual):
                        state_trend.append({"month": m, "handle": round(row['Handle'].iloc[0]/1e6, 2), "revenue": round(row['Revenue'].iloc[0]/1e6, 2), "net_rev": round(row['Net_Rev'].iloc[0]/1e6, 2)})
                    else:
                        # FIX: Use None for future/unreported months to prevent chart collapse
                        state_trend.append({"month": m, "handle": None, "revenue": None, "net_rev": None})
                
                master_db[ticker][vert_key][state] = {"brands": sorted(brand_totals, key=lambda x: x['revenue_12m'], reverse=True), "trend": state_trend}
                
    return master_db

def run():
    db = process_excel_file("data_drops/Sports_Casino_Data_ByBrand_US_States.xlsx")
    with open('regulatory_data.json', 'w') as f: json.dump(db or {}, f, indent=4)

if __name__ == "__main__": run()
