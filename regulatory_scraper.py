import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import re
from google import genai
import warnings
warnings.filterwarnings('ignore')

# Comprehensive list of all targeted public gaming tickers
TARGET_TICKERS = [
    "FLUT", "DKNG", "ENT.L", "MGM", "CZR", "PENN", "RSI", "BALY", 
    "SGHC", "EVOK.L", "BETS-B.ST", "CHDN", "BYD", "RRR", "GDEN", "MCRI",
    "WYNN", "LVS", "FLL", "CNTY", "INSE", "GENI"
]

def get_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key: return None
    return genai.Client(api_key=api_key)

def get_ai_brand_mapping(entity_brand_pairs):
    client = get_gemini_client()
    if not client or not entity_brand_pairs:
        return {}
    
    prompt = f"""You are an expert in the US iGaming and Casino industry. 
    I am providing a list of "Entity | Brand" pairs extracted from state regulatory data. 
    'Entity' is the local license holder/casino, and 'Brand' is the consumer sportsbook/iCasino.
    
    Map each exact "Entity | Brand" string to its ultimate publicly traded parent company ticker.
    Use ONLY these tickers: {TARGET_TICKERS}. If the operator is private, tribal, or not owned by these, map it to "PRIVATE".
    
    CRITICAL MAPPINGS:
    - 'WSOP' or 'World Series of Poker' -> 'CZR'
    - 'WynnBet' or 'Wynn' -> 'WYNN'
    - 'FanDuel', 'Betfair', 'Paddy Power' -> 'FLUT'
    - 'DraftKings', 'Golden Nugget' -> 'DKNG'
    - 'BetMGM', 'Borgata', 'PartyPoker' -> 'MGM'
    - 'Caesars', 'Tropicana', 'Harrahs', 'William Hill' -> 'CZR'
    - 'Barstool', 'ESPN Bet', 'theScore', 'Hollywood' -> 'PENN'
    - 'BetRivers', 'Rush Street', 'PlaySugarHouse' -> 'RSI'
    - 'Bally' -> 'BALY'
    - 'Boyd' -> 'BYD'
    
    Pairs to map: {entity_brand_pairs}
    
    Return STRICTLY a valid JSON dictionary where keys are the exact "Entity | Brand" strings provided, and values are the corresponding tickers. Do NOT wrap in markdown blockquotes."""
    
    try:
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        clean_json = re.sub(r'^```json\s*', '', response.text.strip())
        clean_json = re.sub(r'```$', '', clean_json).strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"⚠️ AI Mapping Error: {e}")
        return {}

def process_excel_file(file_path):
    print(f"📊 Processing local file: {file_path}")
    xls = pd.ExcelFile(file_path)
    
    dfs = []
    for sheet in xls.sheet_names:
        if sheet.upper() in ['CASINO', 'SPORTS']:
            temp_df = pd.read_excel(xls, sheet)
            temp_df['Vertical'] = sheet.upper()
            dfs.append(temp_df)
            
    if not dfs:
        print("⚠️ Neither CASINO nor SPORTS sheets found.")
        return None
        
    df = pd.concat(dfs, ignore_index=True)
    
    # THE FIX: Explicit renaming. No fuzzy searching to prevent duplicate columns!
    df.rename(columns={
        'Period': 'Date',
        'Revenue - Taxable': 'Taxable_Rev',
        'Tax - State': 'State_Tax'
    }, inplace=True)
    
    # Ensure required columns are present
    required_cols = ['State', 'Date', 'Entity', 'Brand', 'Vertical', 'Handle', 'Revenue']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"⚠️ Missing columns in Excel. Missing: {missing}")
        return None
        
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date', 'Brand', 'Entity'])
    
    max_date = df['Date'].max()
    cutoff_date = max_date - pd.DateOffset(months=12)
    df = df[df['Date'] > cutoff_date]
    df['Month_Str'] = df['Date'].dt.strftime('%b %Y')
    
    for col in ['Handle', 'Revenue', 'Taxable_Rev', 'State_Tax']:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    df['Net_Rev'] = df['Taxable_Rev'] - df['State_Tax']
    df['Vertical'] = df['Vertical'].astype(str).str.strip().str.title()
    
    # CREATING THE PROXY CONTEXT: Combine Entity and Brand
    df['Entity_Brand'] = df['Entity'].astype(str) + " | " + df['Brand'].astype(str)

    unique_pairs = df['Entity_Brand'].unique().tolist()
    print(f"🧠 Asking AI to map {len(unique_pairs)} unique Entity | Brand pairs...")
    
    brand_to_ticker = get_ai_brand_mapping(unique_pairs)
    
    df['Ticker'] = df['Entity_Brand'].map(lambda x: brand_to_ticker.get(x, 'PRIVATE'))
    df = df[df['Ticker'].isin(TARGET_TICKERS)]

    master_db = {}
    all_months = sorted(df['Month_Str'].unique(), key=lambda x: datetime.strptime(x, '%b %Y'))

    for ticker, t_df in df.groupby('Ticker'):
        master_db[ticker] = {
            "summary": {"casino_gross": 0, "casino_net": 0, "sports_gross": 0, "sports_net": 0},
            "Casino": {}, 
            "Sports": {}
        }
        
        # Aggregate National Totals
        for vert in ["Casino", "Sports"]:
            v_data = t_df[t_df['Vertical'] == vert]
            if not v_data.empty:
                gross = v_data['Revenue'].sum() / 1e6
                net = v_data['Net_Rev'].sum() / 1e6
                if vert == "Casino":
                    master_db[ticker]["summary"]["casino_gross"] = round(gross, 2)
                    master_db[ticker]["summary"]["casino_net"] = round(net, 2)
                else:
                    master_db[ticker]["summary"]["sports_gross"] = round(gross, 2)
                    master_db[ticker]["summary"]["sports_net"] = round(net, 2)

        # Break out by State
        for vertical, v_df in t_df.groupby('Vertical'):
            for state, s_df in v_df.groupby('State'):
                
                brand_totals = []
                # Group by consumer Brand for the UI table
                for brand, b_df in s_df.groupby('Brand'):
                    brand_totals.append({
                        "brand": brand,
                        "handle_12m": round(b_df['Handle'].sum() / 1e6, 2),
                        "revenue_12m": round(b_df['Revenue'].sum() / 1e6, 2),
                        "taxable_12m": round(b_df['Taxable_Rev'].sum() / 1e6, 2),
                        "net_rev_12m": round(b_df['Net_Rev'].sum() / 1e6, 2)
                    })
                
                trend_df = s_df.groupby(['Date', 'Month_Str']).sum(numeric_only=True).reset_index()
                
                state_trend = []
                for m in all_months:
                    row = trend_df[trend_df['Month_Str'] == m]
                    if not row.empty:
                        state_trend.append({
                            "month": m,
                            "handle": round(row['Handle'].iloc[0] / 1e6, 2),
                            "revenue": round(row['Revenue'].iloc[0] / 1e6, 2),
                            "net_rev": round(row['Net_Rev'].iloc[0] / 1e6, 2)
                        })
                    else:
                        state_trend.append({"month": m, "handle": 0.0, "revenue": 0.0, "net_rev": 0.0})
                
                master_db[ticker][vertical][state] = {
                    "brands": sorted(brand_totals, key=lambda x: x['revenue_12m'], reverse=True),
                    "trend": state_trend
                }
                
    return master_db

def run():
    print("🛡️ Starting ETL Pipeline...")
    file_path = "data_drops/Sports_Casino_Data_ByBrand_US_States.xlsx"
    db = process_excel_file(file_path) if os.path.exists(file_path) else {}
    with open('regulatory_data.json', 'w') as f:
        json.dump(db or {}, f, indent=4)
    print("✅ Pipeline Complete.")

if __name__ == "__main__":
    run()
