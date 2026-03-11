import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random
import os
import re
from google import genai
import warnings
warnings.filterwarnings('ignore')

# The known tickers we want Gemini to map brands to
TARGET_TICKERS = [
    "FLUT", "DKNG", "ENT.L", "MGM", "CZR", "PENN", "RSI", "BALY", 
    "SGHC", "EVOK.L", "BETS-B.ST", "CHDN", "BYD", "RRR", "GDEN", "MCRI"
]

def get_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key: return None
    return genai.Client(api_key=api_key)

def get_ai_brand_mapping(brands):
    """Asks Gemini to map a list of casino/sportsbook brands to their parent stock tickers."""
    client = get_gemini_client()
    if not client or not brands:
        return {}
    
    prompt = f"""You are an expert in the US iGaming and Casino industry. 
    Map the following list of brands to their ultimate publicly traded parent company ticker.
    Use ONLY these tickers: {TARGET_TICKERS}. If a brand is private or not owned by these, map it to "PRIVATE".
    (e.g., 'FanDuel' -> 'FLUT', 'DraftKings' -> 'DKNG', 'BetMGM' -> 'MGM', 'Borgata' -> 'MGM', 'Caesars' -> 'CZR', 'Tropicana' -> 'CZR', 'Barstool' -> 'PENN', 'ESPN Bet' -> 'PENN', 'BetRivers' -> 'RSI').
    
    Brands to map: {brands}
    
    Return STRICTLY a valid JSON dictionary where keys are the exact brand names and values are the tickers. Do NOT wrap in markdown blockquotes."""
    
    try:
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        clean_json = re.sub(r'^```json\s*', '', response.text.strip())
        clean_json = re.sub(r'```$', '', clean_json).strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"⚠️ AI Mapping Error: {e}")
        return {}

def generate_dummy_data_for_ui():
    """Generates structural dummy data so the UI doesn't crash before the user uploads the Excel file."""
    dummy_db = {}
    curr = datetime.now()
    months = [(curr - relativedelta(months=i)).strftime("%b %Y") for i in range(12)][::-1]
    
    for ticker in ["DKNG", "FLUT", "MGM", "CZR", "PENN", "RSI"]:
        dummy_db[ticker] = {"Casino": {}, "Sports": {}}
        for vertical in ["Casino", "Sports"]:
            for state in ["NJ", "PA", "MI"]:
                brand_name = f"{ticker} {vertical} Brand"
                history = []
                for m in months:
                    h = random.uniform(50, 200)
                    rev = h * random.uniform(0.07, 0.12)
                    taxable = rev * 0.9
                    tax = taxable * 0.15
                    history.append({
                        "month": m, "brand": brand_name,
                        "handle": round(h, 2), "revenue": round(rev, 2),
                        "taxable_rev": round(taxable, 2), "state_tax": round(tax, 2),
                        "net_rev": round(taxable - tax, 2)
                    })
                
                brand_totals = [{
                    "brand": brand_name,
                    "handle_12m": round(sum(x['handle'] for x in history), 2),
                    "revenue_12m": round(sum(x['revenue'] for x in history), 2),
                    "taxable_12m": round(sum(x['taxable_rev'] for x in history), 2),
                    "net_rev_12m": round(sum(x['net_rev'] for x in history), 2)
                }]
                
                state_trend = []
                for m in months:
                    m_data = [x for x in history if x['month'] == m]
                    state_trend.append({
                        "month": m,
                        "handle": round(sum(x['handle'] for x in m_data), 2),
                        "revenue": round(sum(x['revenue'] for x in m_data), 2),
                        "net_rev": round(sum(x['net_rev'] for x in m_data), 2)
                    })
                    
                dummy_db[ticker][vertical][state] = {
                    "brands": brand_totals,
                    "trend": state_trend
                }
    return dummy_db

def process_excel_file(file_path):
    print(f"📊 Processing local file: {file_path}")
    xls = pd.ExcelFile(file_path)
    
    dfs = []
    # Read the specific tabs we want
    for sheet in xls.sheet_names:
        if sheet.upper() in ['CASINO', 'SPORTS']:
            print(f"  -> Loading sheet: {sheet}")
            dfs.append(pd.read_excel(xls, sheet))
            
    if not dfs:
        print("⚠️ Neither CASINO nor SPORTS sheets found. Falling back to dummy data.")
        return None
        
    df = pd.concat(dfs, ignore_index=True)
    
    # Map the user's exact columns to the engine's internal names
    df.rename(columns={
        'Period': 'Date',
        'Revenue - Taxable': 'Taxable_Rev',
        'Tax - State': 'State_Tax'
    }, inplace=True)
    
    required = ['State', 'Brand', 'Date', 'Revenue', 'Vertical']
    missing = [r for r in required if r not in df.columns]
    if missing:
        print(f"⚠️ Missing columns in Excel. Missing: {missing}")
        return None

    # Standardize Data Types
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date', 'Brand'])
    
    # Filter for the Last 12 Months
    max_date = df['Date'].max()
    cutoff_date = max_date - pd.DateOffset(months=12)
    df = df[df['Date'] > cutoff_date]
    df['Month_Str'] = df['Date'].dt.strftime('%b %Y')
    
    # Fill missing numeric cols with 0
    for col in ['Handle', 'Revenue', 'Taxable_Rev', 'State_Tax']:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # Calculate Net Revenue
    df['Net_Rev'] = df['Taxable_Rev'] - df['State_Tax']
    
    # Clean up the Vertical column text (e.g. "Casino", "Sports")
    df['Vertical'] = df['Vertical'].astype(str).str.strip().str.title()

    # Ask Gemini to map brands
    unique_brands = df['Brand'].unique().tolist()
    print(f"🧠 Asking AI to map {len(unique_brands)} unique brands to parent tickers...")
    brand_to_ticker = get_ai_brand_mapping(unique_brands)
    
    df['Ticker'] = df['Brand'].map(lambda x: brand_to_ticker.get(x, 'PRIVATE'))
    df = df[df['Ticker'].isin(TARGET_TICKERS)] # Drop private/unmapped

    master_db = {}
    for ticker, t_df in df.groupby('Ticker'):
        master_db[ticker] = {"Casino": {}, "Sports": {}}
        
        for vertical, v_df in t_df.groupby('Vertical'):
            # Ensure the vertical key exists in our db structure
            if vertical not in master_db[ticker]:
                master_db[ticker][vertical] = {}
                
            for state, s_df in v_df.groupby('State'):
                
                brand_totals = []
                for brand, b_df in s_df.groupby('Brand'):
                    brand_totals.append({
                        "brand": brand,
                        "handle_12m": round(b_df['Handle'].sum() / 1e6, 2), # Convert to Millions
                        "revenue_12m": round(b_df['Revenue'].sum() / 1e6, 2),
                        "taxable_12m": round(b_df['Taxable_Rev'].sum() / 1e6, 2),
                        "net_rev_12m": round(b_df['Net_Rev'].sum() / 1e6, 2)
                    })
                
                trend_df = s_df.groupby(['Date', 'Month_Str']).sum(numeric_only=True).reset_index().sort_values('Date')
                state_trend = []
                for _, row in trend_df.iterrows():
                    state_trend.append({
                        "month": row['Month_Str'],
                        "handle": round(row['Handle'] / 1e6, 2),
                        "revenue": round(row['Revenue'] / 1e6, 2),
                        "net_rev": round(row['Net_Rev'] / 1e6, 2)
                    })
                
                master_db[ticker][vertical][state] = {
                    "brands": sorted(brand_totals, key=lambda x: x['revenue_12m'], reverse=True),
                    "trend": state_trend
                }
                
    return master_db

def run():
    print("🛡️ Starting AI Data Pipeline...")
    file_path = "data_drops/Sports_Casino_Data_ByBrand_US_States.xlsx"
    
    if os.path.exists(file_path):
        db = process_excel_file(file_path)
        if not db: 
            print("⚠️ Data processing failed. Generating UI placeholder data...")
            db = generate_dummy_data_for_ui()
    else:
        print(f"⚠️ Excel file not found at {file_path}")
        db = generate_dummy_data_for_ui()
        
    with open('regulatory_data.json', 'w') as f:
        json.dump(db, f, indent=4)
        
    print(f"✅ Regulatory Pipeline Complete.")

if __name__ == "__main__":
    run()
