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

# The Dictionary now acts strictly as a Safety Net
VERIFIED_DATA = {
    "FLUT": {"rev_label": "NGR", "revenue_fy": "$14.05B (FY '24)", "revenue_interim": "$3.79B (Q4 '24)", "focus": "B2C Sportsbook & iGaming", "map_codes": ["US", "GB", "IE", "AU", "IT", "BR"], "eps_actual": 1.74, "eps_forecast": 1.91, "net_income": "$162M", "ebitda": "$2.36B", "fcf": "$941M", "jurisdictions": ["US", "UK", "Ireland", "Australia", "Italy"]},
    "DKNG": {"rev_label": "REV", "revenue_fy": "$4.77B (FY '24)", "revenue_interim": "$1.39B (Q4 '24)", "focus": "B2C Sportsbook & iGaming", "map_codes": ["US", "CA", "PR"], "eps_actual": 0.25, "eps_forecast": 0.18, "net_income": "-$507M", "ebitda": "$181M", "fcf": "$270M", "jurisdictions": ["US", "Ontario", "Puerto Rico"]},
    "ENT.L": {"rev_label": "NGR", "revenue_fy": "£5.33B (FY '25)", "revenue_interim": "£2.70B (H2 '25)", "focus": "B2C Sportsbook, iGaming & Retail", "map_codes": ["GB", "IT", "BR", "AU", "ES"], "eps_actual": 0.62, "eps_forecast": 0.55, "net_income": "-£681M", "ebitda": "£1.16B", "fcf": "£151M", "jurisdictions": ["UK", "Italy", "Brazil", "Australia"]},
    "EVO.ST": {"rev_label": "REV", "revenue_fy": "€2.21B (FY '24)", "revenue_interim": "€625M (Q4 '24)", "focus": "B2B Live Casino Technology", "map_codes": ["SE", "US", "CA", "MT", "LV", "GE", "RO"], "eps_actual": 1.54, "eps_forecast": 1.46, "net_income": "€1.24B", "ebitda": "€1.56B", "fcf": "€250M", "jurisdictions": ["Europe", "North America", "LatAm", "Asia"]},
    "MGM": {"rev_label": "REV", "revenue_fy": "$17.2B (FY '24)", "revenue_interim": "$4.3B (Q4 '24)", "focus": "Land-based Resorts & B2C Digital", "map_codes": ["US", "CN", "JP"], "eps_actual": -1.10, "eps_forecast": 0.56, "net_income": "$157M", "ebitda": "$528M", "fcf": "$300M", "jurisdictions": ["US", "Macau", "Japan"]},
    "CZR": {"rev_label": "REV", "revenue_fy": "$11.4B (FY '24)", "revenue_interim": "$2.8B (Q4 '24)", "focus": "Land-based Resorts & B2C Digital", "map_codes": ["US", "CA", "GB", "AE"], "eps_actual": -0.34, "eps_forecast": 0.10, "net_income": "-$72M", "ebitda": "$900M", "fcf": "$150M", "jurisdictions": ["US", "Canada", "UK", "UAE"]},
    "PENN": {"rev_label": "REV", "revenue_fy": "$6.3B (FY '24)", "revenue_interim": "$1.6B (Q4 '24)", "focus": "Land-based Casinos & B2C Digital", "map_codes": ["US", "CA"], "eps_actual": 0.07, "eps_forecast": 0.02, "net_income": "$15M", "ebitda": "$350M", "fcf": "$80M", "jurisdictions": ["US", "Canada"]},
    "LVS": {"rev_label": "REV", "revenue_fy": "$11.5B (FY '24)", "revenue_interim": "$2.9B (Q4 '24)", "focus": "Land-based Casino Resorts", "map_codes": ["CN", "SG"], "eps_actual": 0.65, "eps_forecast": 0.55, "net_income": "$450M", "ebitda": "$1.2B", "fcf": "$600M", "jurisdictions": ["Macau", "Singapore"]},
    "WYNN": {"rev_label": "REV", "revenue_fy": "$7.2B (FY '24)", "revenue_interim": "$1.8B (Q4 '24)", "focus": "Luxury Land-based Resorts", "map_codes": ["US", "CN", "AE"], "eps_actual": 1.20, "eps_forecast": 1.05, "net_income": "$200M", "ebitda": "$600M", "fcf": "$300M", "jurisdictions": ["US", "Macau", "UAE"]},
    "EVOK.L": {"rev_label": "NGR", "revenue_fy": "£1.75B (FY '24)", "revenue_interim": "£850M (H2 '24)", "focus": "B2C Sportsbook, iGaming & Retail", "map_codes": ["GB", "IT", "ES", "RO"], "eps_actual": -0.05, "eps_forecast": 0.01, "net_income": "-£191M", "ebitda": "£312M", "fcf": "£20M", "jurisdictions": ["UK", "Italy", "Spain"]},
    "SRAD": {"rev_label": "REV", "revenue_fy": "$980M (FY '24)", "revenue_interim": "$280M (Q4 '24)", "focus": "B2B Sports Data & Technology", "map_codes": ["CH", "US", "GB", "DE", "AT"], "eps_actual": 0.14, "eps_forecast": 0.10, "net_income": "$35M", "ebitda": "$55M", "fcf": "$40M", "jurisdictions": ["Global B2B", "US", "Europe"]},
    "BETS-B.ST": {"rev_label": "REV", "revenue_fy": "€1.0B (FY '24)", "revenue_interim": "€260M (Q4 '24)", "focus": "B2C & B2B iGaming/Sportsbook", "map_codes": ["SE", "MT", "IT", "AR", "CO", "PE"], "eps_actual": 0.35, "eps_forecast": 0.32, "net_income": "€45M", "ebitda": "€75M", "fcf": "€50M", "jurisdictions": ["Nordics", "LatAm", "CEECA"]},
    "PTEC.L": {"rev_label": "REV", "revenue_fy": "€1.7B (FY '24)", "revenue_interim": "€850M (H2 '24)", "focus": "B2B iGaming & Sportsbook Tech", "map_codes": ["GB", "IT", "BG", "UA", "EE"], "eps_actual": 0.18, "eps_forecast": 0.20, "net_income": "€55M", "ebitda": "€200M", "fcf": "€80M", "jurisdictions": ["UK", "Italy", "LatAm"]},
    "CHDN": {"rev_label": "REV", "revenue_fy": "$2.8B (FY '24)", "revenue_interim": "$750M (Q4 '24)", "focus": "Racing, Casinos & Online Wagering", "map_codes": ["US"], "eps_actual": 1.35, "eps_forecast": 1.20, "net_income": "$90M", "ebitda": "$300M", "fcf": "$120M", "jurisdictions": ["US"]},
    "LNW": {"rev_label": "REV", "revenue_fy": "$3.1B (FY '24)", "revenue_interim": "$800M (Q4 '24)", "focus": "B2B Gaming Machines & iGaming", "map_codes": ["US", "AU", "GB", "SE"], "eps_actual": 0.45, "eps_forecast": 0.50, "net_income": "$45M", "ebitda": "$280M", "fcf": "$100M", "jurisdictions": ["US", "Australia", "UK"]},
    "ALL.AX": {"rev_label": "REV", "revenue_fy": "A$6.4B (FY '24)", "revenue_interim": "A$3.2B (H2 '24)", "focus": "B2B Slots, Social Casino & iGaming", "map_codes": ["AU", "US", "GB", "IL"], "eps_actual": 0.95, "eps_forecast": 0.90, "net_income": "A$600M", "ebitda": "A$1.1B", "fcf": "A$750M", "jurisdictions": ["US", "Australia", "Global"]},
    "SGHC": {"rev_label": "NGR", "revenue_fy": "€1.4B (FY '24)", "revenue_interim": "€360M (Q4 '24)", "focus": "B2C Sportsbook & iGaming", "map_codes": ["ZA", "CA", "GB", "MT", "FR"], "eps_actual": 0.08, "eps_forecast": 0.10, "net_income": "€35M", "ebitda": "€75M", "fcf": "€45M", "jurisdictions": ["Canada", "Africa", "Europe"]},
    "RSI": {"rev_label": "REV", "revenue_fy": "$950M (FY '24)", "revenue_interim": "$250M (Q4 '24)", "focus": "B2C Casino-First iGaming", "map_codes": ["US", "CO", "MX", "CA", "PE"], "eps_actual": 0.12, "eps_forecast": 0.08, "net_income": "$15M", "ebitda": "$40M", "fcf": "$20M", "jurisdictions": ["US", "Colombia", "Mexico"]},
    "BRAG": {"rev_label": "REV", "revenue_fy": "€105M (FY '24)", "revenue_interim": "€28M (Q4 '24)", "focus": "B2B iGaming Content & PAM", "map_codes": ["CA", "US", "NL", "BR", "FI"], "eps_actual": -0.02, "eps_forecast": 0.01, "net_income": "-€1M", "ebitda": "€4M", "fcf": "€1M", "jurisdictions": ["US", "Europe", "Canada"]},
    "KAMBI.ST": {"rev_label": "REV", "revenue_fy": "€180M (FY '24)", "revenue_interim": "€45M (Q4 '24)", "focus": "B2B Sportsbook Technology", "map_codes": ["MT", "SE", "GB", "US", "RO", "CO"], "eps_actual": 0.18, "eps_forecast": 0.15, "net_income": "€5M", "ebitda": "€15M", "fcf": "€8M", "jurisdictions": ["Global B2B", "US", "LatAm"]}
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
    print("🌍 Fetching live Forex rates...")
    rates = {'USD': 1.0, '$': 1.0}
    pairs = {'GBP': 'GBPUSD=X', 'GBp': 'GBPUSD=X', 'EUR': 'EURUSD=X', 'SEK': 'SEKUSD=X', 'AUD': 'AUDUSD=X', 'CAD': 'CADUSD=X'}
    for currency, ticker in pairs.items():
        try:
            val = yf.Ticker(ticker).fast_info['lastPrice']
            if currency == 'GBp': val = val / 100.0 
            rates[currency] = val
        except Exception:
            rates[currency] = 1.0 
    return rates

def format_money(raw_val, sym):
    """Helper to cleanly format large financial numbers"""
    if pd.isna(raw_val): return "N/A"
    is_neg = raw_val < 0
    abs_val = abs(raw_val)
    if abs_val >= 1e9: res = f"{sym}{round(abs_val/1e9, 2)}B"
    elif abs_val >= 1e6: res = f"{sym}{round(abs_val/1e6, 2)}M"
    else: res = f"{sym}{abs_val}"
    return f"-{res}" if is_neg else res

def get_stock_fundamentals(ticker, fx_rates):
    """TOTAL AUTOMATION ENGINE: Hunts for Revenue, EBITDA, Net Income, FCF, and EPS."""
    price, mc_usd_val = 0, 0
    price_str, mc_display, pe_str, de_str = "N/A", "N/A", "N/A", "N/A"
    fy_rev_str, interim_rev_str = "N/A", "N/A"
    dyn_net_inc, dyn_ebitda, dyn_fcf = "N/A", "N/A", "N/A"
    dyn_eps_act, dyn_eps_est = None, None
    sym, currency = "$", "USD"
    
    try:
        ytk = yf.Ticker(ticker)
        
        # --- TIER 1 & 2: PRICE & MARKET CAP ---
        try:
            price = ytk.fast_info['lastPrice']
            currency = ytk.fast_info['currency']
        except Exception: pass 
            
        if currency == "GBp": sym = "GBp "
        elif currency == "GBP": sym = "£"
        elif currency == "SEK": sym = "SEK "
        elif currency == "EUR": sym = "€"
        elif currency == "AUD": sym = "A$"
        elif currency == "CAD": sym = "C$"
        else: sym = "$"
        
        if price > 0: price_str = f"{sym}{round(price, 2)}"
            
        try:
            mc_raw = ytk.fast_info['marketCap']
            if mc_raw and mc_raw > 0:
                mc_native = format_money(mc_raw, sym)
                fx_rate = fx_rates.get(currency, 1.0)
                mc_usd_val = mc_raw * fx_rate
                
                if currency not in ["USD", "$"]:
                    mc_usd_str = format_money(mc_usd_val, "$")
                    mc_display = f"{mc_native} ({mc_usd_str})"
                else:
                    mc_display = mc_native
        except Exception: pass

        # --- TIER 3: P/E & DEBT-TO-EQUITY ---
        try:
            info = ytk.info
            pe_raw = info.get('trailingPE') or info.get('forwardPE')
            if pe_raw: pe_str = f"{round(pe_raw, 2)}"
            else:
                eps = info.get('trailingEps')
                if eps is not None:
                    if eps <= 0: pe_str = "Neg EPS"
                    elif price > 0: pe_str = f"{round(price / eps, 2)}" 

            de_raw = info.get('debtToEquity')
            if de_raw is not None: de_str = f"{round(de_raw, 2)}%"
            else:
                total_debt = info.get('totalDebt')
                total_equity = info.get('totalStockholderEquity') 
                if total_debt is not None and total_equity is not None:
                    if total_equity <= 0: de_str = "Neg Equity" 
                    else: de_str = f"{round((total_debt / total_equity) * 100, 2)}%" 
                elif total_debt == 0: de_str = "0.00%"
        except Exception: pass 

        # --- TIER 4: THE FORENSIC ACCOUNTANT (Revenue, EBITDA, Net Income) ---
        try:
            income_annual = ytk.income_stmt
            if not income_annual.empty:
                # 1. FY Revenue
                raw_rev_fy = None
                if 'Total Revenue' in income_annual.index: raw_rev_fy = income_annual.loc['Total Revenue'].iloc[0]
                elif 'Operating Revenue' in income_annual.index: raw_rev_fy = income_annual.loc['Operating Revenue'].iloc[0]
                if pd.notna(raw_rev_fy):
                    fy_year = pd.to_datetime(income_annual.columns[0]).year
                    fy_rev_str = f"{format_money(raw_rev_fy, sym)} (FY '{str(fy_year)[-2:]})"
                
                # 2. Net Income
                raw_ni = None
                for key in ['Net Income Common Stockholders', 'Net Income', 'Net Income From Continuing Operations']:
                    if key in income_annual.index:
                        raw_ni = income_annual.loc[key].iloc[0]
                        break
                if pd.notna(raw_ni): dyn_net_inc = format_money(raw_ni, sym)

                # 3. EBITDA
                raw_ebitda = None
                for key in ['Normalized EBITDA', 'EBITDA']:
                    if key in income_annual.index:
                        raw_ebitda = income_annual.loc[key].iloc[0]
                        break
                if pd.notna(raw_ebitda): dyn_ebitda = format_money(raw_ebitda, sym)

        except Exception: pass

        # --- TIER 5: CASH FLOW (FCF) ---
        try:
            cf = ytk.cashflow
            if not cf.empty:
                raw_fcf = None
                if 'Free Cash Flow' in cf.index:
                    raw_fcf = cf.loc['Free Cash Flow'].iloc[0]
                elif 'Operating Cash Flow' in cf.index and 'Capital Expenditure' in cf.index:
                    # Calculate FCF manually if Yahoo hid it
                    raw_fcf = cf.loc['Operating Cash Flow'].iloc[0] + cf.loc['Capital Expenditure'].iloc[0]
                if pd.notna(raw_fcf): dyn_fcf = format_money(raw_fcf, sym)
        except Exception: pass

        # --- TIER 6: INTERIM REVENUE & EPS BEAT/MISS ---
        try:
            income_quarterly = ytk.quarterly_income_stmt
            if not income_quarterly.empty:
                raw_rev_q = None
                if 'Total Revenue' in income_quarterly.index: raw_rev_q = income_quarterly.loc['Total Revenue'].iloc[0]
                elif 'Operating Revenue' in income_quarterly.index: raw_rev_q = income_quarterly.loc['Operating Revenue'].iloc[0]
                    
                if pd.notna(raw_rev_q) and raw_rev_q > 0:
                    q_date = income_quarterly.columns[0]
                    q_month = pd.to_datetime(q_date).month
                    q_year = pd.to_datetime(q_date).year
                    q_label = "Q4"
                    if q_month <= 3: q_label = "Q1"
                    elif q_month <= 6: q_label = "Q2"
                    elif q_month <= 9: q_label = "Q3"
                    
                    try:
                        if len(income_quarterly.columns) > 1:
                            days_diff = (pd.to_datetime(income_quarterly.columns[0]) - pd.to_datetime(income_quarterly.columns[1])).days
                            if days_diff > 120: q_label = "H1" if q_month <= 6 else "H2"
                    except Exception: pass
                    
                    interim_rev_str = f"{format_money(raw_rev_q, sym)} ({q_label} '{str(q_year)[-2:]})"
        except Exception: pass
        
        try:
            ed = ytk.earnings_dates
            if ed is not None and not ed.empty:
                past_ed = ed[ed['Reported EPS'].notna()]
                if not past_ed.empty:
                    dyn_eps_act = past_ed['Reported EPS'].iloc[0]
                    dyn_eps_est = past_ed['Estimate EPS'].iloc[0]
        except Exception: pass
            
        return price_str, price, mc_display, mc_usd_val, pe_str, de_str, fy_rev_str, interim_rev_str, dyn_net_inc, dyn_ebitda, dyn_fcf, dyn_eps_act, dyn_eps_est
        
    except Exception as e:
        print(f"  ❌ FATAL Fundamentals fetch failed for {ticker}: {e}")
        return "N/A", 0, "N/A", 0, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", None, None

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
        
        # Base fallback from verified data
        fin = VERIFIED_DATA.get(ticker, {
            "eps_actual": 0, "eps_forecast": 0, "net_income": "N/A", "ebitda": "N/A", "fcf": "N/A", "jurisdictions": [],
            "focus": "Diversified Gaming", "map_codes": [], "rev_label": "REV", "revenue_fy": "N/A", "revenue_interim": "N/A"
        })
        
        cal = VERIFIED_CALENDAR.get(ticker, {"date": "TBD", "report_time": "TBD", "call_time": "TBD"})
            
        try:
            intel = ai_process_intelligence(co['name'], ticker)
            
            # The Total Automation Engine
            last_price_str, native_price_raw, mc_str, mc_usd, pe_ratio, debt_equity, dyn_fy_rev, dyn_int_rev, dyn_net_inc, dyn_ebitda, dyn_fcf, dyn_eps_act, dyn_eps_est = get_stock_fundamentals(ticker, fx_rates)
            
            # SMART MERGE: Prioritize dynamic API data, fallback to VERIFIED_DATA if missing
            fin["revenue_fy"] = dyn_fy_rev if dyn_fy_rev != "N/A" else fin.get("revenue_fy", "N/A")
            fin["revenue_interim"] = dyn_int_rev if dyn_int_rev != "N/A" else fin.get("revenue_interim", "N/A")
            fin["net_income"] = dyn_net_inc if dyn_net_inc != "N/A" else fin.get("net_income", "N/A")
            fin["ebitda"] = dyn_ebitda if dyn_ebitda != "N/A" else fin.get("ebitda", "N/A")
            fin["fcf"] = dyn_fcf if dyn_fcf != "N/A" else fin.get("fcf", "N/A")
            
            # EPS Fallback Logic
            beat_miss = 0
            if dyn_eps_act is not None and dyn_eps_est is not None and dyn_eps_est != 0:
                fin["eps_actual"] = round(dyn_eps_act, 2)
                fin["eps_forecast"] = round(dyn_eps_est, 2)
                beat_miss = round(((dyn_eps_act - dyn_eps_est) / abs(dyn_eps_est)) * 100, 2)
            else:
                if fin.get("eps_forecast", 0) != 0:
                    beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2)

            history = fetch_stock_history(ticker, native_price_raw)
            
        except Exception as e:
            print(f"  ⚠️ Critical loop failure for {ticker}: {e}")
            intel = {"summary": [f"System Error: {str(e)[:50]}"], "sentiment": 50}
            history, last_price_str, mc_str, mc_usd, pe_ratio, debt_equity = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}, "N/A", "N/A", 0, "N/A", "N/A"
            beat_miss = 0

        master_db.append({
            "ticker": ticker,
            "company": co["name"],
            "base_country": co["base_country"],
            "focus": fin.get("focus", "Diversified Gaming"), 
            "map_codes": fin.get("map_codes", []),           
            "calendar": cal, 
            "last_price": last_price_str,
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
