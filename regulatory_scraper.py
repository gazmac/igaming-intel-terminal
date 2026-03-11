import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
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

def get_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key: return None
    return genai.Client(api_key=api_key)

def get_ai_brand_mapping(entity_brand_pairs):
    client = get_gemini_client()
    if not client or not entity_brand_pairs:
        return {}
    
    prompt = f"""You are an expert in the US iGaming and Casino industry. 
    Map each exact "Entity | Brand" string to its ultimate publicly traded parent company ticker.
    Use ONLY these tickers: {TARGET_TICKERS}. If the operator is private, map it to "PRIVATE".
    
    CRITICAL GOLD-STANDARD MAPPINGS:
    - RULE: If Brand is 'DraftKings' but Entity is 'Hollywood', map to 'DKNG'. Brand supersedes Entity.
    - 'WSOP' or 'World Series of Poker' -> 'CZR'
    - 'WynnBet', 'Wynn', 'Encore' -> 'WYNN'
    - 'FanDuel', 'Betfair', 'Paddy Power', 'MotorCity' -> 'FLUT'
    - 'DraftKings', 'Golden Nugget' -> 'DKNG'
    - 'BetMGM', 'Borgata', 'PartyPoker' -> 'MGM'
    - 'Caesars', 'Tropicana', 'Harrahs', 'William Hill' -> 'CZR'
    - 'Barstool', 'ESPN Bet', 'theScore', 'Hollywood', 'Ameristar' -> 'PENN'
    - 'BetRivers', 'Rush Street', 'PlaySugarHouse', 'Rivers' -> 'RSI'
    - 'TwinSpires', 'Derby City', 'Turfway', 'Ellis Park', 'Oak Grove', 'Terre Haute', 'Oxford', 'Fair Grounds', 'Rosies', 'The Rose' -> 'CHDN'
    - 'Bally' -> 'BALY'
    - 'Boyd', 'Treasure Chest', 'Delta Downs' -> 'BYD'
    
    Pairs to map: {entity_brand_pairs}
    
    Return STRICTLY a valid JSON dictionary where keys are the exact "Entity | Brand" strings provided, and values are the corresponding tickers. Do NOT wrap in markdown."""
    
    try:
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        clean_json = re.sub(r'^```json\s*', '', response.text.strip())
        clean_json = re.sub(r'```$', '', clean_json).strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"⚠️ AI Mapping Error: {e}")
        return {}

def safe_val(val):
    """Guarantees no NaN values ever leak into the JSON output."""
    if pd.isna(val): return 0.0
    return round(float(val), 2)

def process_excel_file(file_path):
    print(f"📊 Processing local file: {file_path}")
    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"⚠️ Could not load Excel file: {e}")
        return None
    
    dfs = []
    for sheet in xls.sheet_names:
        if sheet.upper() in ['CASINO', 'SPORTS']:
            temp_df = pd.read_excel(xls, sheet)
            temp_df['Vertical'] = sheet.title()
            dfs.append(temp_df)
            
    if not dfs: return None
        
    df = pd.concat(dfs, ignore_index=True)
    
    # Strip whitespace from column names to prevent matching errors
    df.columns = [str(c).strip() for c in df.columns]
    
    # Safely map columns without causing duplicates
    rename_dict = {}
    for col in df.columns:
        c = col.lower()
        if c in ['period', 'date', 'month']: rename_dict[col] = 'Date'
        elif c in ['revenue - taxable', 'taxable rev']: rename_dict[col] = 'Taxable_Rev'
        elif c in ['tax - state', 'state tax']: rename_dict[col] = 'State_Tax'
    df.rename(columns=rename_dict, inplace=True)
    
    # Graceful Fallbacks for missing Identifiers (Prevents Data Wipes)
    if 'Entity' not in df.columns: df['Entity'] = 'Unknown'
    if 'Brand' not in df.columns:
        if 'Operator' in df.columns: df.rename(columns={'Operator': 'Brand'}, inplace=True)
        elif 'Licensee' in df.columns: df.rename(columns={'Licensee': 'Brand'}, inplace=True)
        else: df['Brand'] = 'Unknown'
            
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Only drop rows if they don't have a valid Date. 
    df = df.dropna(subset=['Date'])
    
    # Fill empty text cells
    df['Entity'] = df['Entity'].fillna('Unknown')
    df['Brand'] = df['Brand'].fillna('Unknown')
    
    # Filter for trailing 12 months
    max_date = df['Date'].max()
    if pd.isna(max_date): return None
    
    cutoff_date = max_date - pd.DateOffset(months=12)
    df = df[df['Date'] > cutoff_date]
    df['Month_Str'] = df['Date'].dt.strftime('%b %Y')
    
    # Fill empty numeric cells
    for col in ['Handle', 'Revenue', 'Taxable_Rev', 'State_Tax']:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    df['Net_Rev'] = df['Taxable_Rev'] - df['State_Tax']
    
    # Create Context Strings for Gemini
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
        
        # Calculate Top-Level Summaries Safely
        for vert in ["Casino", "Sports"]:
            v_data = t_df[t_df['Vertical'] == vert]
            if not v_data.empty:
                master_db[ticker]["summary"][f"{vert.lower()}_gross"] = safe_val(v_data['Revenue'].sum() / 1e6)
                master_db[ticker]["summary"][f"{vert.lower()}_net"] = safe_val(v_data['Net_Rev'].sum() / 1e6)

        # Process States and Brands
        for vertical, v_df in t_df.groupby('Vertical'):
            for state, s_df in v_df.groupby('State'):
                
                brand_totals = []
                for brand, b_df in s_df.groupby('Brand'):
                    brand_totals.append({
                        "brand": brand,
                        "handle_12m": safe_val(b_df['Handle'].sum() / 1e6),
                        "revenue_12m": safe_val(b_df['Revenue'].sum() / 1e6),
                        "taxable_12m": safe_val(b_df['Taxable_Rev'].sum() / 1e6),
                        "net_rev_12m": safe_val(b_df['Net_Rev'].sum() / 1e6)
                    })
                
                trend_df = s_df.groupby(['Date', 'Month_Str']).sum(numeric_only=True).reset_index()
                
                # Determine where actual reporting stops
                actual_data = trend_df[trend_df['Revenue'] > 0]
                state_max_date = actual_data['Date'].max() if not actual_data.empty else pd.NaT
                
                state_trend = []
                for m in all_months:
                    row = trend_df[trend_df['Month_Str'] == m]
                    m_date = datetime.strptime(m, '%b %Y')
                    
                    if not row.empty:
                        state_trend.append({
                            "month": m,
                            "handle": safe_val(row['Handle'].iloc[0] / 1e6),
                            "revenue": safe_val(row['Revenue'].iloc[0] / 1e6),
                            "net_rev": safe_val(row['Net_Rev'].iloc[0] / 1e6)
                        })
                    else:
                        # Fixes the 0.00 drop: applies 'null' to future/unreported dates
                        if pd.notna(state_max_date) and m_date > state_max_date:
                            state_trend.append({"month": m, "handle": None, "revenue": None, "net_rev": None})
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
