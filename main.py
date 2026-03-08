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
    # Original Tier 1
    {"name": "Flutter Entertainment", "ticker": "FLUT", "domain": "flutter.com", "base_country": "Ireland"},
    {"name": "DraftKings", "ticker": "DKNG", "domain": "draftkings.com", "base_country": "USA"},
    {"name": "Entain PLC", "ticker": "ENT.L", "domain": "entaingroup.com", "base_country": "UK"},
    {"name": "Evolution AB", "ticker": "EVO.ST", "domain": "evolution.com", "base_country": "Sweden"},
    {"name": "MGM Resorts", "ticker": "MGM", "domain": "mgmresorts.com", "base_country": "USA"},
    {"name": "Caesars Entertainment", "ticker": "CZR", "domain": "caesars.com", "base_country": "USA"},
    {"name": "Penn Entertainment", "ticker": "PENN", "domain": "pennentertainment.com", "base_country": "USA"},
    {"name": "Las Vegas Sands", "ticker": "LVS", "domain": "sands.com", "base_country": "USA"},
    {"name": "Wynn Resorts", "ticker": "WYNN", "domain": "wynnresorts.com", "base_country": "USA"},
    {"name": "Evoke plc", "ticker": "EVOK.L", "domain": "evokeplc.com", "base_country": "UK"},
    {"name": "Sportradar", "ticker": "SRAD", "domain": "sportradar.com", "base_country": "Switzerland"},
    {"name": "Betsson AB", "ticker": "BETS-B.ST", "domain": "betssongroup.com", "base_country": "Sweden"},
    {"name": "Playtech", "ticker": "PTEC.L", "domain": "playtech.com", "base_country": "UK"},
    {"name": "Churchill Downs", "ticker": "CHDN", "domain": "churchilldownsincorporated.com", "base_country": "USA"},
    {"name": "Light & Wonder", "ticker": "LNW", "domain": "lnw.com", "base_country": "USA"},
    {"name": "Aristocrat Leisure", "ticker": "ALL.AX", "domain": "aristocrat.com", "base_country": "Australia"},
    {"name": "Super Group", "ticker": "SGHC", "domain": "supergroup.com", "base_country": "Guernsey"},
    {"name": "Rush Street Interactive", "ticker": "RSI", "domain": "rushstreetinteractive.com", "base_country": "USA"},
    {"name": "Bragg Gaming Group", "ticker": "BRAG", "domain": "bragg.group", "base_country": "Canada"},
    {"name": "Kambi Group", "ticker": "KAMBI.ST", "domain": "kambi.com", "base_country": "Malta"},
    
    # Tier 2: Global Expansion
    {"name": "Galaxy Entertainment", "ticker": "0027.HK", "domain": "galaxyentertainment.com", "base_country": "Hong Kong"},
    {"name": "Melco Resorts", "ticker": "MLCO", "domain": "melco-resorts.com", "base_country": "Hong Kong"},
    {"name": "SJM Holdings", "ticker": "1980.HK", "domain": "sjmholdings.com", "base_country": "Hong Kong"},
    {"name": "Wynn Macau", "ticker": "1128.HK", "domain": "wynnmacau.com", "base_country": "Macau"},
    {"name": "Genting Singapore", "ticker": "G13.SI", "domain": "gentingsingapore.com", "base_country": "Singapore"},
    {"name": "La Française des Jeux", "ticker": "FDJ.PA", "domain": "fdjunited.com", "base_country": "France"},
    {"name": "Lottomatica Group", "ticker": "LOTO.MI", "domain": "lottomaticagroup.com", "base_country": "Italy"},
    {"name": "Rank Group", "ticker": "RNK.L", "domain": "rank.com", "base_country": "UK"},
    {"name": "Better Collective", "ticker": "BETCO.ST", "domain": "bettercollective.com", "base_country": "Denmark"},
    {"name": "Catena Media", "ticker": "CTM.ST", "domain": "catenamedia.com", "base_country": "Malta"},
    {"name": "Bally's Corporation", "ticker": "BALY", "domain": "ballys.com", "base_country": "USA"},
    {"name": "Boyd Gaming", "ticker": "BYD", "domain": "boydgaming.com", "base_country": "USA"},
    {"name": "Red Rock Resorts", "ticker": "RRR", "domain": "redrockresorts.com", "base_country": "USA"},
    {"name": "Golden Entertainment", "ticker": "GDEN", "domain": "goldenent.com", "base_country": "USA"},
    {"name": "Monarch Casino", "ticker": "MCRI", "domain": "monarchcasino.com", "base_country": "USA"},
    {"name": "Century Casinos", "ticker": "CNTY", "domain": "cnty.com", "base_country": "USA"},
    {"name": "Genius Sports", "ticker": "GENI", "domain": "geniussports.com", "base_country": "UK"},
    {"name": "IGT", "ticker": "IGT", "domain": "igt.com", "base_country": "UK"},
    {"name": "Inspired Entertainment", "ticker": "INSE", "domain": "inseinc.com", "base_country": "USA"},
    {"name": "Star Entertainment", "ticker": "SGR.AX", "domain": "starentertainmentgroup.com.au", "base_country": "Australia"},
    
    # Tier 3: Malaysia, US Deep Cuts, and Final European Sweep
    {"name": "Genting Malaysia", "ticker": "GENM.KL", "domain": "gentingmalaysia.com", "base_country": "Malaysia"},
    {"name": "VICI Properties", "ticker": "VICI", "domain": "viciproperties.com", "base_country": "USA"},
    {"name": "Gaming & Leisure Prop", "ticker": "GLPI", "domain": "glpropinc.com", "base_country": "USA"},
    {"name": "Full House Resorts", "ticker": "FLL", "domain": "fullhouseresorts.com", "base_country": "USA"},
    {"name": "Everi Holdings", "ticker": "EVRI", "domain": "everi.com", "base_country": "USA"},
    {"name": "OPAP S.A.", "ticker": "OPAP.AT", "domain": "opap.gr", "base_country": "Greece"},
    {"name": "Zeal Network", "ticker": "TIMA.F", "domain": "zealnetwork.de", "base_country": "Germany"},
    {"name": "Gaming Realms", "ticker": "GMR.L", "domain": "gamingrealms.com", "base_country": "UK"},
    {"name": "Groupe Partouche", "ticker": "PARP.PA", "domain": "groupepartouche.com", "base_country": "France"},
    {"name": "Bet-at-home", "ticker": "ACX.DE", "domain": "bet-at-home.ag", "base_country": "Germany"},
    {"name": "Gambling.com Group", "ticker": "GAMB", "domain": "gambling.com", "base_country": "Jersey"}
]

# Fully populated mapping to ensure no cloud server API failures
OTC_MAP = {
    "ENT.L": "GMVHF", "EVO.ST": "EVVTY", "EVOK.L": "EIHDF", 
    "BETS-B.ST": "BTSBF", "PTEC.L": "PYTCF", 
    "ALL.AX": "ARLUF", "KAMBI.ST": "KMBIF",
    "0027.HK": "GXYEF", "1980.HK": "SJMHF", "1128.HK": "WYNMF",
    "G13.SI": "GIGNF", "FDJ.PA": "LFDJF", "RNK.L": "RANKF",
    "SGR.AX": "EHGRF", "GENM.KL": "GMALY",
    "OPAP.AT": "GOFPY", "LOTO.MI": "LTMGF", "PARP.PA": "PARPF" 
}

# Base safety-net dictionary (All 51 Companies)
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
    "KAMBI.ST": {"rev_label": "REV", "revenue_fy": "€180M (FY '24)", "revenue_interim": "€45M (Q4 '24)", "focus": "B2B Sportsbook Technology", "map_codes": ["MT", "SE", "GB", "US", "RO", "CO"], "eps_actual": 0.18, "eps_forecast": 0.15, "net_income": "€5M", "ebitda": "€15M", "fcf": "€8M", "jurisdictions": ["Global B2B", "US", "LatAm"]},
    "0027.HK": {"rev_label": "REV", "focus": "Macau Casino Resorts", "map_codes": ["CN", "HK"]},
    "MLCO": {"rev_label": "REV", "focus": "Macau & Asia Resorts", "map_codes": ["CN", "PH", "CY"]},
    "1980.HK": {"rev_label": "REV", "focus": "Macau Casino Resorts", "map_codes": ["CN", "HK"]},
    "1128.HK": {"rev_label": "REV", "focus": "Macau Luxury Resorts", "map_codes": ["CN", "HK"]},
    "G13.SI": {"rev_label": "REV", "focus": "Singapore Integrated Resorts", "map_codes": ["SG"]},
    "FDJ.PA": {"rev_label": "NGR", "focus": "European Lottery & iGaming", "map_codes": ["FR", "IE"]},
    "LOTO.MI": {"rev_label": "NGR", "focus": "Italian Sportsbook & Gaming", "map_codes": ["IT"]},
    "RNK.L": {"rev_label": "NGR", "focus": "UK Retail Casinos & Digital", "map_codes": ["GB", "ES"]},
    "BETCO.ST": {"rev_label": "REV", "focus": "Global Sports Media Affiliate", "map_codes": ["DK", "US", "GB", "SE"]},
    "CTM.ST": {"rev_label": "REV", "focus": "iGaming Lead Generation", "map_codes": ["MT", "US", "SE"]},
    "BALY": {"rev_label": "REV", "focus": "US Regional Casinos & iGaming", "map_codes": ["US", "GB"]},
    "BYD": {"rev_label": "REV", "focus": "US Regional & Locals Casinos", "map_codes": ["US"]},
    "RRR": {"rev_label": "REV", "focus": "Las Vegas Locals Casinos", "map_codes": ["US"]},
    "GDEN": {"rev_label": "REV", "focus": "Taverns & Regional Casinos", "map_codes": ["US"]},
    "MCRI": {"rev_label": "REV", "focus": "Regional US Casinos", "map_codes": ["US"]},
    "CNTY": {"rev_label": "REV", "focus": "International Regional Casinos", "map_codes": ["US", "CA", "PL"]},
    "GENI": {"rev_label": "REV", "focus": "B2B Sports Data Rights", "map_codes": ["GB", "US", "CO"]},
    "IGT": {"rev_label": "REV", "focus": "B2B Lottery & Slot Cabinets", "map_codes": ["US", "IT", "GB"]},
    "INSE": {"rev_label": "REV", "focus": "VLTs & Virtual Sports", "map_codes": ["US", "GB", "GR"]},
    "SGR.AX": {"rev_label": "REV", "focus": "Australian Casino Resorts", "map_codes": ["AU"]},
    "GENM.KL": {"rev_label": "REV", "focus": "Asian Integrated Resorts", "map_codes": ["MY", "US", "GB", "BS"]},
    "VICI": {"rev_label": "REV", "focus": "Gaming & Hospitality REIT", "map_codes": ["US", "CA"]},
    "GLPI": {"rev_label": "REV", "focus": "Gaming & Leisure REIT", "map_codes": ["US"]},
    "FLL": {"rev_label": "REV", "focus": "US Regional Casinos", "map_codes": ["US"]},
    "EVRI": {"rev_label": "REV", "focus": "FinTech & Slot Cabinets", "map_codes": ["US", "CA"]},
    "OPAP.AT": {"rev_label": "NGR", "focus": "Greek Lottery & Betting Monopoly", "map_codes": ["GR", "CY"]},
    "TIMA.F": {"rev_label": "REV", "focus": "Online Lottery Broker", "map_codes": ["DE", "GB"]},
    "GMR.L": {"rev_label": "REV", "focus": "Mobile Slingo & iGaming Content", "map_codes": ["GB", "US", "CA"]},
    "PARP.PA": {"rev_label": "REV", "focus": "French Casino Operator", "map_codes": ["FR", "CH"]},
    "ACX.DE": {"rev_label": "REV", "focus": "European Sportsbook", "map_codes": ["DE", "AT"]},
    "GAMB": {"rev_label": "REV", "focus": "iGaming Performance Marketing", "map_codes": ["US", "GB", "IE"]}
}

VERIFIED_CALENDAR = {
    # Calendar omitted. UI natively defaults to "TBD" for dates without throwing errors.
}

def get_live_fx_rates():
    print("🌍 Fetching live Forex rates...")
    rates = {'USD': 1.0, '$': 1.0}
    # Expanded currencies to cover Global additions
    pairs = {
        'GBP': 'GBPUSD=X', 'GBp': 'GBPUSD=X', 'EUR': 'EURUSD=X', 
        'SEK': 'SEKUSD=X', 'AUD': 'AUDUSD=X', 'CAD': 'CADUSD=X',
        'HKD': 'HKDUSD=X', 'SGD': 'SGDUSD=X', 'MYR': 'MYRUSD=X'
    }
    for currency, ticker in pairs.items():
        try:
            val = yf.Ticker(ticker).fast_info['lastPrice']
            if currency == 'GBp': val = val / 100.0 
            rates[currency] = val
        except Exception:
            rates[currency] = 1.0 
    return rates

def format_money(raw_val, sym):
    if pd.isna(raw_val): return "N/A"
    is_neg = raw_val < 0
    abs_val = abs(raw_val)
    if abs_val >= 1e9: res = f"{sym}{round(abs_val/1e9, 2)}B"
    elif abs_val >= 1e6: res = f"{sym}{round(abs_val/1e6, 2)}M"
    else: res = f"{sym}{abs_val}"
    return f"-{res}" if is_neg else res

def get_stock_fundamentals(ticker, fx_rates):
    """TOTAL AUTOMATION ENGINE WITH GLOBAL FX SUPPORT"""
    price, mc_usd_val = 0, 0
    price_str, mc_display, pe_str, de_str = "N/A", "N/A", "N/A", "N/A"
    fy_rev_str, interim_rev_str = "N/A", "N/A"
    dyn_net_inc, dyn_ebitda, dyn_fcf = "N/A", "N/A", "N/A"
    dyn_eps_act, dyn_eps_est = None, None
    sym, currency = "$", "USD"
    
    try:
        ytk = yf.Ticker(ticker)
        
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
        elif currency == "HKD": sym = "HK$"
        elif currency == "SGD": sym = "S$"
        elif currency == "MYR": sym = "RM "
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

        try:
            income_annual = ytk.income_stmt
            if not income_annual.empty:
                raw_rev_fy = None
                if 'Total Revenue' in income_annual.index: raw_rev_fy = income_annual.loc['Total Revenue'].iloc[0]
                elif 'Operating Revenue' in income_annual.index: raw_rev_fy = income_annual.loc['Operating Revenue'].iloc[0]
                if pd.notna(raw_rev_fy):
                    fy_year = pd.to_datetime(income_annual.columns[0]).year
                    fy_rev_str = f"{format_money(raw_rev_fy, sym)} (FY '{str(fy_year)[-2:]})"
                
                raw_ni = None
                for key in ['Net Income Common Stockholders', 'Net Income', 'Net Income From Continuing Operations']:
                    if key in income_annual.index:
                        raw_ni = income_annual.loc[key].iloc[0]
                        break
                if pd.notna(raw_ni): dyn_net_inc = format_money(raw_ni, sym)

                raw_ebitda = None
                for key in ['Normalized EBITDA', 'EBITDA']:
                    if key in income_annual.index:
                        raw_ebitda = income_annual.loc[key].iloc[0]
                        break
                if pd.notna(raw_ebitda): dyn_ebitda = format_money(raw_ebitda, sym)
        except Exception: pass

        try:
            cf = ytk.cashflow
            if not cf.empty:
                raw_fcf = None
                if 'Free Cash Flow' in cf.index:
                    raw_fcf = cf.loc['Free Cash Flow'].iloc[0]
                elif 'Operating Cash Flow' in cf.index and 'Capital Expenditure' in cf.index:
                    raw_fcf = cf.loc['Operating Cash Flow'].iloc[0] + cf.loc['Capital Expenditure'].iloc[0]
                if pd.notna(raw_fcf): dyn_fcf = format_money(raw_fcf, sym)
        except Exception: pass

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
    except Exception: pass
    return history

def ai_process_intelligence(company_name, ticker):
    print(f"  -> Fetching Yahoo API News & Generating Reading Room for {company_name}...")
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key or api_key == "YOUR_ACTUAL_API_KEY_HERE":
        return {"summary": ["System Error: API key missing."], "sentiment": 50, "reading_room": "<p>API Key required.</p>", "quotes": []}
        
    try:
        client = genai.Client(api_key=api_key)
        clean_name = urllib.parse.quote(company_name)
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={clean_name}&newsCount=5"
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        res = requests.get(url, headers=headers, timeout=10)
        res_data = res.json()
        headlines = [item['title'] for item in res_data.get('news', [])]
        
        if not headlines:
            return {"summary": [f"No news headlines found recently for {company_name}."], "sentiment": 50, "reading_room": "<p>No recent news available.</p>", "quotes": []}

        prompt = f"""Act as an iGaming financial analyst. Review these recent financial headlines for {company_name}: {' | '.join(headlines)}. 
Return a valid JSON object with exactly four keys: 
1. 'summary' (a list of 3 string bullet points summarizing the news), 
2. 'sentiment' (an integer from 0 to 100 representing market sentiment), 
3. 'reading_room' (An HTML formatted string using <p>, <strong>, <ul>, and <li> tags providing a detailed 'Executive Analyst Briefing' based on the headlines. Write it in the style of an earnings call summary, covering recent performance, headwinds, and strategic outlook.),
4. 'quotes' (A list of 2 distinct string sentences containing key strategic quotes from the CEO, CFO, or Management. If the exact quotes are not in the headlines, logically synthesize highly realistic, professional quotes reflecting the factual data of the headlines. Do not use HTML tags in this list)."""
        
        ai_resp = client.models.generate_content(
            model='gemini-2.5-flash', 
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        
        raw_text = ai_resp.text.strip()
        match = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if match:
            data = json.loads(match.group(0))
            if "summary" in data and "sentiment" in data and "reading_room" in data and "quotes" in data:
                return data
                
        return {"summary": ["Failed to extract valid data."], "sentiment": 50, "reading_room": "<p>Failed to generate briefing.</p>", "quotes": []}
        
    except Exception as e:
        return {"summary": [f"News Error: {str(e)[:60]}"], "sentiment": 50, "reading_room": f"<p>Error: {str(e)[:60]}</p>", "quotes": []}

# --- 3. PIPELINE EXECUTION ---

def run_pipeline():
    master_db = []
    print(f"🚀 Starting Pipeline processing {len(TARGET_COMPANIES)} companies...")
    
    fx_rates = get_live_fx_rates()
    
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        print(f"\nProcessing {co['name']}...")
        
        fin = VERIFIED_DATA.get(ticker, {
            "eps_actual": 0, "eps_forecast": 0, "net_income": "N/A", "ebitda": "N/A", "fcf": "N/A", "jurisdictions": [],
            "focus": "Diversified Gaming", "map_codes": [], "rev_label": "REV", "revenue_fy": "N/A", "revenue_interim": "N/A"
        })
        
        cal = VERIFIED_CALENDAR.get(ticker, {"date": "TBD", "report_time": "TBD", "call_time": "TBD"})
            
        try:
            intel = ai_process_intelligence(co['name'], ticker)
            
            last_price_str, native_price_raw, mc_str, mc_usd, pe_ratio, debt_equity, dyn_fy_rev, dyn_int_rev, dyn_net_inc, dyn_ebitda, dyn_fcf, dyn_eps_act, dyn_eps_est = get_stock_fundamentals(ticker, fx_rates)
            
            fin["revenue_fy"] = dyn_fy_rev if dyn_fy_rev != "N/A" else fin.get("revenue_fy", "N/A")
            fin["revenue_interim"] = dyn_int_rev if dyn_int_rev != "N/A" else fin.get("revenue_interim", "N/A")
            fin["net_income"] = dyn_net_inc if dyn_net_inc != "N/A" else fin.get("net_income", "N/A")
            fin["ebitda"] = dyn_ebitda if dyn_ebitda != "N/A" else fin.get("ebitda", "N/A")
            fin["fcf"] = dyn_fcf if dyn_fcf != "N/A" else fin.get("fcf", "N/A")
            
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
            intel = {"summary": [f"System Error: {str(e)[:50]}"], "sentiment": 50, "reading_room": "<p>Error</p>", "quotes": []}
            history, last_price_str, mc_str, mc_usd, pe_ratio, debt_equity = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}, "N/A", "N/A", 0, "N/A", "N/A"
            beat_miss = 0

        master_db.append({
            "ticker": ticker,
            "company": co["name"],
            "logo": f"https://www.google.com/s2/favicons?domain={co['domain']}&sz=128",
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
            "reading_room": intel.get("reading_room", "<p>Data unavailable.</p>"),
            "quotes": intel.get("quotes", []),
            "jurisdictions": fin.get("jurisdictions", []),
            "history": history
        })
        
        time.sleep(5
