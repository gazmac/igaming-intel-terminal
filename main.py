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
    {"name": "Galaxy Entertainment", "ticker": "0027.HK", "domain": "galaxyentertainment.com", "base_country": "Hong Kong"},
    {"name": "Melco Resorts", "ticker": "MLCO", "domain": "cityofdreamsmacau.com", "base_country": "Hong Kong"}, 
    {"name": "SJM Holdings", "ticker": "1980.HK", "domain": "sjmresorts.com", "base_country": "Hong Kong"}, 
    {"name": "Wynn Macau", "ticker": "1128.HK", "domain": "wynnresorts.com", "base_country": "Macau"}, 
    {"name": "Genting Singapore", "ticker": "G13.SI", "domain": "gentingsingapore.com", "base_country": "Singapore"},
    {"name": "La Française des Jeux", "ticker": "FDJ.PA", "domain": "fdjunited.com", "base_country": "France"},
    {"name": "Lottomatica Group", "ticker": "LOTO.MI", "domain": "lottomaticagroup.com", "base_country": "Italy"},
    {"name": "Rank Group", "ticker": "RNK.L", "domain": "rank.com", "base_country": "UK"},
    {"name": "Better Collective", "ticker": "BETCO.ST", "domain": "bettercollective.com", "base_country": "Denmark"},
    {"name": "Catena Media", "ticker": "CTM.ST", "domain": "askgamblers.com", "base_country": "Malta"}, 
    {"name": "Bally's Corporation", "ticker": "BALY", "domain": "ballys.com", "base_country": "USA"},
    {"name": "Boyd Gaming", "ticker": "BYD", "domain": "boydgaming.com", "base_country": "USA"},
    {"name": "Red Rock Resorts", "ticker": "RRR", "domain": "stationcasinos.com", "base_country": "USA"}, 
    {"name": "Golden Entertainment", "ticker": "GDEN", "domain": "goldenent.com", "base_country": "USA"},
    {"name": "Monarch Casino", "ticker": "MCRI", "domain": "monarchcasino.com", "base_country": "USA"},
    {"name": "Century Casinos", "ticker": "CNTY", "domain": "cnty.com", "base_country": "USA"},
    {"name": "Genius Sports", "ticker": "GENI", "domain": "geniussports.com", "base_country": "UK"},
    {"name": "Brightstar Lottery (fka IGT)", "ticker": "BRSL", "domain": "brightstarlottery.com", "base_country": "UK"},
    {"name": "Inspired Entertainment", "ticker": "INSE", "domain": "inseinc.com", "base_country": "USA"},
    {"name": "Star Entertainment", "ticker": "SGR.AX", "domain": "starentertainmentgroup.com.au", "base_country": "Australia"},
    {"name": "Genting Malaysia", "ticker": "GENM.KL", "domain": "gentingmalaysia.com", "base_country": "Malaysia"},
    {"name": "VICI Properties", "ticker": "VICI", "domain": "viciproperties.com", "base_country": "USA"},
    {"name": "Gaming & Leisure Prop", "ticker": "GLPI", "domain": "glpropinc.com", "base_country": "USA"},
    {"name": "Full House Resorts", "ticker": "FLL", "domain": "americanplace.com", "base_country": "USA"}, 
    {"name": "OPAP S.A.", "ticker": "OPAP.AT", "domain": "opap.gr", "base_country": "Greece"},
    {"name": "Zeal Network", "ticker": "TIMA.F", "domain": "zealnetwork.de", "base_country": "Germany"},
    {"name": "Gaming Realms", "ticker": "GMR.L", "domain": "gamingrealms.com", "base_country": "UK"},
    {"name": "Groupe Partouche", "ticker": "PARP.PA", "domain": "groupepartouche.com", "base_country": "France"},
    {"name": "Bet-at-home", "ticker": "ACX.DE", "domain": "bet-at-home.ag", "base_country": "Germany"},
    {"name": "Gambling.com Group", "ticker": "GAMB", "domain": "gambling.com", "base_country": "Jersey"},
    {"name": "BetMGM (MGM/Entain JV)", "ticker": "BETMGM", "domain": "betmgm.com", "base_country": "USA"},
    {"name": "Accel Entertainment", "ticker": "ACEL", "domain": "accelentertainment.com", "base_country": "USA"}
]

OTC_MAP = {
    "ENT.L": "GMVHF", "EVO.ST": "EVVTY", "EVOK.L": "EIHDF", 
    "BETS-B.ST": "BTSBF", "PTEC.L": "PYTCF", 
    "ALL.AX": "ARLUF", "KAMBI.ST": "KMBIF",
    "0027.HK": "GXYEF", "1980.HK": "SJMHF", "1128.HK": "WYNMF",
    "G13.SI": "GIGNF", "FDJ.PA": "LFDJF", "RNK.L": "RANKF",
    "SGR.AX": "EHGRF", "GENM.KL": "GMALY",
    "OPAP.AT": "GOFPY", "LOTO.MI": "LTMGF", "PARP.PA": "PARPF" 
}

VERIFIED_DATA = {
    "FLUT": {"rev_label": "NGR", "revenue_fy": "$14.05B (FY '25)", "revenue_interim": "$3.79B (Q4 '25)", "focus": "B2C Sportsbook & iGaming", "map_codes": ["US", "GB", "IE", "AU", "IT", "BR"], "eps_actual": 1.74, "eps_forecast": 1.91, "net_income": "$162M", "ebitda": "$2.36B", "fcf": "$941M", "jurisdictions": ["US", "UK", "Ireland", "Australia", "Italy"]},
    "DKNG": {"rev_label": "REV", "revenue_fy": "$4.77B (FY '25)", "revenue_interim": "$1.39B (Q4 '25)", "focus": "B2C Sportsbook & iGaming", "map_codes": ["US", "CA", "PR"], "eps_actual": 0.25, "eps_forecast": 0.18, "net_income": "-$507M", "ebitda": "$181M", "fcf": "$270M", "jurisdictions": ["US", "Ontario", "Puerto Rico"]},
    "ENT.L": {"rev_label": "NGR", "revenue_fy": "£5.33B (FY '25)", "revenue_interim": "£2.70B (H2 '25)", "focus": "B2C Sportsbook, iGaming & Retail", "map_codes": ["GB", "IT", "BR", "AU", "ES"], "eps_actual": 0.62, "eps_forecast": 0.55, "net_income": "-£681M", "ebitda": "£1.16B", "fcf": "£151M", "jurisdictions": ["UK", "Italy", "Brazil", "Australia"]},
    "EVO.ST": {"rev_label": "REV", "revenue_fy": "€2.21B (FY '25)", "revenue_interim": "€625M (Q4 '25)", "focus": "B2B Live Casino Technology", "map_codes": ["SE", "US", "CA", "MT", "LV", "GE", "RO"], "eps_actual": 1.54, "eps_forecast": 1.46, "net_income": "€1.24B", "ebitda": "€1.56B", "fcf": "€250M", "jurisdictions": ["Europe", "North America", "LatAm", "Asia"]},
    "MGM": {"rev_label": "REV", "revenue_fy": "$17.2B (FY '25)", "revenue_interim": "$4.3B (Q4 '25)", "focus": "Land-based Resorts & B2C Digital", "map_codes": ["US", "CN", "JP"], "eps_actual": -1.10, "eps_forecast": 0.56, "net_income": "$157M", "ebitda": "$528M", "fcf": "$300M", "jurisdictions": ["US", "Macau", "Japan"]},
    "CZR": {"rev_label": "REV", "revenue_fy": "$11.4B (FY '25)", "revenue_interim": "$2.8B (Q4 '25)", "focus": "Land-based Resorts & B2C Digital", "map_codes": ["US", "CA", "GB", "AE"], "eps_actual": -0.34, "eps_forecast": 0.10, "net_income": "-$72M", "ebitda": "$900M", "fcf": "$150M", "jurisdictions": ["US", "Canada", "UK", "UAE"]},
    "PENN": {"rev_label": "REV", "revenue_fy": "$6.3B (FY '25)", "revenue_interim": "$1.6B (Q4 '25)", "focus": "Land-based Casinos & B2C Digital", "map_codes": ["US", "CA"], "eps_actual": 0.07, "eps_forecast": 0.02, "net_income": "$15M", "ebitda": "$350M", "fcf": "$80M", "jurisdictions": ["US", "Canada"]},
    "LVS": {"rev_label": "REV", "revenue_fy": "$11.5B (FY '25)", "revenue_interim": "$2.9B (Q4 '25)", "focus": "Land-based Casino Resorts", "map_codes": ["CN", "SG"], "eps_actual": 0.65, "eps_forecast": 0.55, "net_income": "$450M", "ebitda": "$1.2B", "fcf": "$600M", "jurisdictions": ["Macau", "Singapore"]},
    "WYNN": {"rev_label": "REV", "revenue_fy": "$7.14B (FY '25)", "revenue_interim": "$1.87B (Q4 '25)", "focus": "Luxury Land-based Resorts", "map_codes": ["US", "CN", "AE"], "eps_actual": 0.82, "eps_forecast": 2.29, "net_income": "$327.3M", "ebitda": "$2.22B", "fcf": "$800M", "jurisdictions": ["US", "Macau", "UAE"], "fallback_price": "$105.40", "fallback_mcap": "$11.8B", "fallback_pe": "25.2x", "fallback_debt": "850%"},
    "EVOK.L": {"rev_label": "NGR", "revenue_fy": "£1.75B (FY '25)", "revenue_interim": "£850M (H2 '25)", "focus": "B2C Sportsbook, iGaming & Retail", "map_codes": ["GB", "IT", "ES", "RO"], "eps_actual": -0.05, "eps_forecast": 0.01, "net_income": "-£191M", "ebitda": "£312M", "fcf": "£20M", "jurisdictions": ["UK", "Italy", "Spain"]},
    "SRAD": {"rev_label": "REV", "revenue_fy": "$980M (FY '25)", "revenue_interim": "$280M (Q4 '25)", "focus": "B2B Sports Data & Technology", "map_codes": ["CH", "US", "GB", "DE", "AT"], "eps_actual": 0.14, "eps_forecast": 0.10, "net_income": "$35M", "ebitda": "$55M", "fcf": "$40M", "jurisdictions": ["Global B2B", "US", "Europe"]},
    "BETS-B.ST": {"rev_label": "REV", "revenue_fy": "€1.0B (FY '25)", "revenue_interim": "€260M (Q4 '25)", "focus": "B2C & B2B iGaming/Sportsbook", "map_codes": ["SE", "MT", "IT", "AR", "CO", "PE"], "eps_actual": 0.35, "eps_forecast": 0.32, "net_income": "€45M", "ebitda": "€75M", "fcf": "€50M", "jurisdictions": ["Nordics", "LatAm", "CEECA"]},
    "PTEC.L": {"rev_label": "REV", "revenue_fy": "€1.79B (FY '25)", "revenue_interim": "€940M (H2 '25)", "focus": "B2B iGaming & Sportsbook Tech", "map_codes": ["GB", "IT", "BG", "UA", "EE"], "eps_actual": 0.71, "eps_forecast": 0.62, "net_income": "€195M", "ebitda": "€480.4M", "fcf": "€95M", "jurisdictions": ["UK", "Italy", "LatAm"]},
    "CHDN": {"rev_label": "REV", "revenue_fy": "$2.8B (FY '25)", "revenue_interim": "$750M (Q4 '25)", "focus": "Racing, Casinos & Online Wagering", "map_codes": ["US"], "eps_actual": 1.35, "eps_forecast": 1.20, "net_income": "$90M", "ebitda": "$300M", "fcf": "$120M", "jurisdictions": ["US"]},
    "LNW": {"rev_label": "REV", "revenue_fy": "$3.1B (FY '25)", "revenue_interim": "$800M (Q4 '25)", "focus": "B2B Gaming Machines & iGaming", "map_codes": ["US", "AU", "GB", "SE"], "eps_actual": 0.45, "eps_forecast": 0.50, "net_income": "$45M", "ebitda": "$280M", "fcf": "$100M", "jurisdictions": ["US", "Australia", "UK"]},
    "ALL.AX": {"rev_label": "REV", "revenue_fy": "A$6.4B (FY '25)", "revenue_interim": "A$3.2B (H2 '25)", "focus": "B2B Slots, Social Casino & iGaming", "map_codes": ["AU", "US", "GB", "IL"], "eps_actual": 0.95, "eps_forecast": 0.90, "net_income": "A$600M", "ebitda": "A$1.1B", "fcf": "A$750M", "jurisdictions": ["US", "Australia", "Global"]},
    "SGHC": {"rev_label": "NGR", "revenue_fy": "€1.4B (FY '25)", "revenue_interim": "€360M (Q4 '25)", "focus": "B2C Sportsbook & iGaming", "map_codes": ["ZA", "CA", "GB", "MT", "FR"], "eps_actual": 0.08, "eps_forecast": 0.10, "net_income": "€35M", "ebitda": "€75M", "fcf": "€45M", "jurisdictions": ["Canada", "Africa", "Europe"]},
    "RSI": {"rev_label": "REV", "revenue_fy": "$950M (FY '25)", "revenue_interim": "$250M (Q4 '25)", "focus": "B2C Casino-First iGaming", "map_codes": ["US", "CO", "MX", "CA", "PE"], "eps_actual": 0.12, "eps_forecast": 0.08, "net_income": "$15M", "ebitda": "$40M", "fcf": "$20M", "jurisdictions": ["US", "Colombia", "Mexico"]},
    "BRAG": {"rev_label": "REV", "revenue_fy": "€105M (FY '25)", "revenue_interim": "€28M (Q4 '25)", "focus": "B2B iGaming Content & PAM", "map_codes": ["CA", "US", "NL", "BR", "FI"], "eps_actual": -0.02, "eps_forecast": 0.01, "net_income": "-€1M", "ebitda": "€4M", "fcf": "€1M", "jurisdictions": ["US", "Europe", "Canada"]},
    "KAMBI.ST": {"rev_label": "REV", "revenue_fy": "€180M (FY '25)", "revenue_interim": "€45M (Q4 '25)", "focus": "B2B Sportsbook Technology", "map_codes": ["MT", "SE", "GB", "US", "RO", "CO"], "eps_actual": 0.18, "eps_forecast": 0.15, "net_income": "€5M", "ebitda": "€15M", "fcf": "€8M", "jurisdictions": ["Global B2B", "US", "LatAm"]},
    "0027.HK": {"rev_label": "REV", "revenue_fy": "HK$31.5B (FY '25)", "revenue_interim": "HK$8.5B (Q4 '25)", "focus": "Macau Casino Resorts", "map_codes": ["CN", "HK"], "eps_actual": 1.20, "eps_forecast": 1.15, "net_income": "HK$5.2B", "ebitda": "HK$8.1B", "fcf": "HK$3.5B", "jurisdictions": ["Macau"]},
    "MLCO": {"rev_label": "REV", "revenue_fy": "$5.16B (FY '25)", "revenue_interim": "$1.29B (Q4 '25)", "focus": "Macau & Asia Resorts", "map_codes": ["CN", "PH", "CY"], "eps_actual": 0.16, "eps_forecast": -0.05, "net_income": "$185M", "ebitda": "$1.43B", "fcf": "$250M", "jurisdictions": ["Macau", "Philippines", "Cyprus"]},
    "1980.HK": {"rev_label": "REV", "revenue_fy": "HK$28.17B (FY '25)", "revenue_interim": "HK$7.2B (Q4 '25)", "focus": "Macau Casino Resorts", "map_codes": ["CN", "HK"], "eps_actual": -0.15, "eps_forecast": -0.10, "net_income": "-HK$429M", "ebitda": "HK$3.2B", "fcf": "-HK$200M", "jurisdictions": ["Macau"]},
    "1128.HK": {"rev_label": "REV", "revenue_fy": "$3.1B (FY '25)", "revenue_interim": "$800M (Q4 '25)", "focus": "Macau Luxury Resorts", "map_codes": ["CN", "HK"], "eps_actual": 0.35, "eps_forecast": 0.30, "net_income": "$320M", "ebitda": "$900M", "fcf": "$450M", "jurisdictions": ["Macau"]},
    "G13.SI": {"rev_label": "REV", "revenue_fy": "S$2.45B (FY '25)", "revenue_interim": "S$1.2B (H2 '25)", "focus": "Singapore Integrated Resorts", "map_codes": ["SG"], "eps_actual": 0.03, "eps_forecast": 0.04, "net_income": "S$390.3M", "ebitda": "S$815.8M", "fcf": "S$450M", "jurisdictions": ["Singapore"]},
    "FDJ.PA": {"rev_label": "NGR", "revenue_fy": "€2.82B (FY '25)", "revenue_interim": "€1.86B (H1 '25)", "focus": "European Lottery & iGaming", "map_codes": ["FR", "IE"], "eps_actual": 1.35, "eps_forecast": 1.25, "net_income": "€425M", "ebitda": "€670M", "fcf": "€380M", "jurisdictions": ["France", "Ireland"], "fallback_price": "€25.84", "fallback_mcap": "€4.77B", "fallback_pe": "18.5x", "fallback_debt": "85%"},
    "LOTO.MI": {"rev_label": "NGR", "revenue_fy": "€1.75B (FY '25)", "revenue_interim": "€950M (H1 '25)", "focus": "Italian Sportsbook & Gaming", "map_codes": ["IT"], "eps_actual": 0.45, "eps_forecast": 0.40, "net_income": "€180M", "ebitda": "€580M", "fcf": "€250M", "jurisdictions": ["Italy"]},
    "RNK.L": {"rev_label": "NGR", "revenue_fy": "£734M (FY '25)", "revenue_interim": "£382M (H1 '25)", "focus": "UK Retail Casinos & Digital", "map_codes": ["GB", "ES"], "eps_actual": 0.05, "eps_forecast": 0.04, "net_income": "£25M", "ebitda": "£120M", "fcf": "£45M", "jurisdictions": ["UK", "Spain"]},
    "BETCO.ST": {"rev_label": "REV", "revenue_fy": "€350M (FY '25)", "revenue_interim": "€180M (H1 '25)", "focus": "Global Sports Media Affiliate", "map_codes": ["DK", "US", "GB", "SE"], "eps_actual": 0.40, "eps_forecast": 0.35, "net_income": "€50M", "ebitda": "€110M", "fcf": "€65M", "jurisdictions": ["Europe", "US"]},
    "CTM.ST": {"rev_label": "REV", "revenue_fy": "€46.6M (FY '25)", "revenue_interim": "€15.6M (Q4 '25)", "focus": "iGaming Lead Generation", "map_codes": ["MT", "US", "SE"], "eps_actual": -0.15, "eps_forecast": -0.05, "net_income": "-€16.5M", "ebitda": "€10.6M", "fcf": "€4M", "jurisdictions": ["US", "Europe"]},
    "BALY": {"rev_label": "REV", "revenue_fy": "$2.4B (FY '25)", "revenue_interim": "$620M (Q4 '25)", "focus": "US Regional Casinos & iGaming", "map_codes": ["US", "GB"], "eps_actual": -0.55, "eps_forecast": -0.40, "net_income": "-$180M", "ebitda": "$510M", "fcf": "-$50M", "jurisdictions": ["US", "UK"]},
    "BYD": {"rev_label": "REV", "revenue_fy": "$3.8B (FY '25)", "revenue_interim": "$950M (Q4 '25)", "focus": "US Regional & Locals Casinos", "map_codes": ["US"], "eps_actual": 1.45, "eps_forecast": 1.35, "net_income": "$520M", "ebitda": "$1.3B", "fcf": "$600M", "jurisdictions": ["US"]},
    "RRR": {"rev_label": "REV", "revenue_fy": "$1.8B (FY '25)", "revenue_interim": "$460M (Q4 '25)", "focus": "Las Vegas Locals Casinos", "map_codes": ["US"], "eps_actual": 0.85, "eps_forecast": 0.80, "net_income": "$250M", "ebitda": "$750M", "fcf": "$320M", "jurisdictions": ["Nevada (US)"]},
    "GDEN": {"rev_label": "REV", "revenue_fy": "$1.1B (FY '25)", "revenue_interim": "$270M (Q4 '25)", "focus": "Taverns & Regional Casinos", "map_codes": ["US"], "eps_actual": 0.50, "eps_forecast": 0.45, "net_income": "$80M", "ebitda": "$260M", "fcf": "$110M", "jurisdictions": ["US"]},
    "MCRI": {"rev_label": "REV", "revenue_fy": "$520M (FY '25)", "revenue_interim": "$130M (Q4 '25)", "focus": "Regional US Casinos", "map_codes": ["US"], "eps_actual": 1.15, "eps_forecast": 1.10, "net_income": "$90M", "ebitda": "$170M", "fcf": "$80M", "jurisdictions": ["US"]},
    "CNTY": {"rev_label": "REV", "revenue_fy": "$550M (FY '25)", "revenue_interim": "$140M (Q4 '25)", "focus": "International Regional Casinos", "map_codes": ["US", "CA", "PL"], "eps_actual": -0.20, "eps_forecast": -0.15, "net_income": "-$35M", "ebitda": "$110M", "fcf": "$25M", "jurisdictions": ["US", "Canada", "Poland"]},
    "GENI": {"rev_label": "REV", "revenue_fy": "$410M (FY '25)", "revenue_interim": "$120M (Q4 '25)", "focus": "B2B Sports Data Rights", "map_codes": ["GB", "US", "CO"], "eps_actual": 0.05, "eps_forecast": 0.02, "net_income": "$15M", "ebitda": "$55M", "fcf": "$20M", "jurisdictions": ["Global B2B"]},
    "BRSL": {"rev_label": "REV", "revenue_fy": "$2.65B (FY '25)", "revenue_interim": "$668M (Q4 '25)", "focus": "Pure-Play Global Lottery", "map_codes": ["US", "IT", "GB"], "eps_actual": 0.45, "eps_forecast": 0.40, "net_income": "$220M", "ebitda": "$1.2B", "fcf": "$600M", "jurisdictions": ["US", "Italy", "Global"], "fallback_price": "$16.47", "fallback_mcap": "$3.34B", "fallback_pe": "15.2x", "fallback_debt": "150%"},
    "INSE": {"rev_label": "REV", "revenue_fy": "$320M (FY '25)", "revenue_interim": "$80M (Q4 '25)", "focus": "VLTs & Virtual Sports", "map_codes": ["US", "GB", "GR"], "eps_actual": 0.35, "eps_forecast": 0.30, "net_income": "$25M", "ebitda": "$100M", "fcf": "$35M", "jurisdictions": ["UK", "North America"]},
    "SGR.AX": {"rev_label": "REV", "revenue_fy": "A$1.8B (FY '25)", "revenue_interim": "A$850M (H2 '25)", "focus": "Australian Casino Resorts", "map_codes": ["AU"], "eps_actual": -0.85, "eps_forecast": -0.50, "net_income": "-A$1.2B", "ebitda": "A$280M", "fcf": "-A$150M", "jurisdictions": ["Australia"]},
    "GENM.KL": {"rev_label": "REV", "revenue_fy": "RM 10.2B (FY '25)", "revenue_interim": "RM 2.6B (Q4 '25)", "focus": "Asian Integrated Resorts", "map_codes": ["MY", "US", "GB", "BS"], "eps_actual": 0.15, "eps_forecast": 0.12, "net_income": "RM 600M", "ebitda": "RM 3.1B", "fcf": "RM 1.2B", "jurisdictions": ["Malaysia", "UK", "US"], "fallback_price": "RM 2.65", "fallback_mcap": "RM 15.8B", "fallback_pe": "15.3x", "fallback_debt": "115%"},
    "VICI": {"rev_label": "REV", "revenue_fy": "$3.6B (FY '25)", "revenue_interim": "$950M (Q4 '25)", "focus": "Gaming & Hospitality REIT", "map_codes": ["US", "CA"], "eps_actual": 0.65, "eps_forecast": 0.60, "net_income": "$1.8B", "ebitda": "$2.9B", "fcf": "$2.1B", "jurisdictions": ["US", "Canada"]},
    "GLPI": {"rev_label": "REV", "revenue_fy": "$1.4B (FY '25)", "revenue_interim": "$360M (Q4 '25)", "focus": "Gaming & Leisure REIT", "map_codes": ["US"], "eps_actual": 0.75, "eps_forecast": 0.70, "net_income": "$650M", "ebitda": "$1.2B", "fcf": "$800M", "jurisdictions": ["US"]},
    "FLL": {"rev_label": "REV", "revenue_fy": "$300M (FY '25)", "revenue_interim": "$75.5M (Q4 '25)", "focus": "US Regional Casinos", "map_codes": ["US"], "eps_actual": -0.34, "eps_forecast": -0.23, "net_income": "-$10M", "ebitda": "$48.1M", "fcf": "$5M", "jurisdictions": ["US"]},
    "OPAP.AT": {"rev_label": "NGR", "revenue_fy": "€2.2B (FY '25)", "revenue_interim": "€1.1B (H1 '25)", "focus": "Greek Lottery & Betting Monopoly", "map_codes": ["GR", "CY"], "eps_actual": 1.15, "eps_forecast": 1.05, "net_income": "€420M", "ebitda": "€750M", "fcf": "€500M", "jurisdictions": ["Greece", "Cyprus"]},
    "TIMA.F": {"rev_label": "REV", "revenue_fy": "€140M (FY '25)", "revenue_interim": "€75M (H1 '25)", "focus": "Online Lottery Broker", "map_codes": ["DE", "GB"], "eps_actual": 0.85, "eps_forecast": 0.80, "net_income": "€30M", "ebitda": "€45M", "fcf": "€35M", "jurisdictions": ["Germany"]},
    "GMR.L": {"rev_label": "REV", "revenue_fy": "£28M (FY '25)", "revenue_interim": "£15M (H1 '25)", "focus": "Mobile Slingo & iGaming Content", "map_codes": ["GB", "US", "CA"], "eps_actual": 0.03, "eps_forecast": 0.02, "net_income": "£5M", "ebitda": "£10M", "fcf": "£7M", "jurisdictions": ["US", "UK"]},
    "PARP.PA": {"rev_label": "REV", "revenue_fy": "€445M (FY '25)", "revenue_interim": "€225M (H1 '25)", "focus": "French Casino Operator", "map_codes": ["FR", "CH"], "eps_actual": 0.45, "eps_forecast": 0.40, "net_income": "€25M", "ebitda": "€85M", "fcf": "€40M", "jurisdictions": ["France", "Switzerland"]},
    "ACX.DE": {"rev_label": "REV", "revenue_fy": "€60M (FY '25)", "revenue_interim": "€30M (H1 '25)", "focus": "European Sportsbook", "map_codes": ["DE", "AT"], "eps_actual": -0.15, "eps_forecast": -0.10, "net_income": "-€5M", "ebitda": "€2M", "fcf": "-€1M", "jurisdictions": ["DACH Region"]},
    "GAMB": {"rev_label": "REV", "revenue_fy": "$115M (FY '25)", "revenue_interim": "$32M (Q4 '25)", "focus": "iGaming Performance Marketing", "map_codes": ["US", "GB", "IE"], "eps_actual": 0.35, "eps_forecast": 0.30, "net_income": "$25M", "ebitda": "$45M", "fcf": "$30M", "jurisdictions": ["US", "UK"]},
    "BETMGM": {"rev_label": "REV", "revenue_fy": "$2.8B (FY '25)", "revenue_interim": "$780M (Q4 '25)", "focus": "B2C Sportsbook & iGaming", "map_codes": ["US", "CA", "PR"], "eps_actual": 0, "eps_forecast": 0, "net_income": "$175M", "ebitda": "$220M", "fcf": "N/A", "jurisdictions": ["US", "Ontario", "Puerto Rico"]},
    "ACEL": {"rev_label": "REV", "revenue_fy": "$1.33B (FY '25)", "revenue_interim": "$341.4M (Q4 '25)", "focus": "Distributed Gaming & Slot Routes", "map_codes": ["US"], "eps_actual": 0.60, "eps_forecast": 0.41, "net_income": "$51.3M", "ebitda": "$210.1M", "fcf": "$150.9M", "jurisdictions": ["US"], "fallback_price": "$11.07", "fallback_mcap": "$950M", "fallback_pe": "21.1x", "fallback_debt": "150%"}
}

VERIFIED_CALENDAR = {
    "FLUT": {"date": "May 6, 2026", "report_time": "Pre-Market US", "call_time": "8:30 AM EST"},
    "DKNG": {"date": "May 8, 2026", "report_time": "Pre-Market", "call_time": "8:30 AM EST"},
    "ENT.L": {"date": "Aug 12, 2026", "report_time": "7:00 AM BST", "call_time": "9:00 AM BST"},
    "EVO.ST": {"date": "Apr 22, 2026", "report_time": "7:30 AM CET", "call_time": "9:00 AM CET"},
    "MGM": {"date": "May 1, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "CZR": {"date": "Apr 28, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "PENN": {"date": "May 7, 2026", "report_time": "7:00 AM EST", "call_time": "9:00 AM EST"},
    "LVS": {"date": "Apr 22, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "WYNN": {"date": "May 6, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "EVOK.L": {"date": "Apr 15, 2026", "report_time": "7:00 AM BST", "call_time": "8:30 AM BST"},
    "SRAD": {"date": "May 12, 2026", "report_time": "Pre-Market", "call_time": "8:00 AM EST"},
    "BETS-B.ST": {"date": "Apr 24, 2026", "report_time": "7:30 AM CET", "call_time": "9:00 AM CET"},
    "PTEC.L": {"date": "Mar 25, 2026", "report_time": "7:00 AM GMT", "call_time": "9:00 AM GMT"},
    "CHDN": {"date": "Apr 22, 2026", "report_time": "Post-Market", "call_time": "9:00 AM EST (Next Day)"},
    "LNW": {"date": "May 8, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "ALL.AX": {"date": "May 13, 2026", "report_time": "8:00 AM AEST", "call_time": "10:30 AM AEST"},
    "SGHC": {"date": "May 14, 2026", "report_time": "Pre-Market", "call_time": "8:30 AM EST"},
    "RSI": {"date": "May 6, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "BRAG": {"date": "May 14, 2026", "report_time": "Pre-Market", "call_time": "8:30 AM EST"},
    "KAMBI.ST": {"date": "Apr 29, 2026", "report_time": "7:45 AM CET", "call_time": "10:45 AM CET"},
    "0027.HK": {"date": "May 14, 2026", "report_time": "4:30 PM HKT", "call_time": "6:30 PM HKT"},
    "MLCO": {"date": "May 7, 2026", "report_time": "Pre-Market", "call_time": "8:30 AM EST"},
    "1980.HK": {"date": "May 12, 2026", "report_time": "4:30 PM HKT", "call_time": "6:30 PM HKT"},
    "1128.HK": {"date": "May 6, 2026", "report_time": "4:00 PM EST", "call_time": "5:00 PM EST"},
    "G13.SI": {"date": "May 14, 2026", "report_time": "5:30 PM SGT", "call_time": "10:00 AM SGT (Next Day)"},
    "FDJ.PA": {"date": "Apr 16, 2026", "report_time": "5:45 PM CET", "call_time": "6:30 PM CET"},
    "LOTO.MI": {"date": "Apr 30, 2026", "report_time": "7:30 AM CET", "call_time": "9:00 AM CET"},
    "RNK.L": {"date": "Aug 15, 2026", "report_time": "7:00 AM BST", "call_time": "9:00 AM BST"},
    "BETCO.ST": {"date": "May 20, 2026", "report_time": "8:00 AM CET", "call_time": "10:00 AM CET"},
    "CTM.ST": {"date": "May 21, 2026", "report_time": "7:00 AM CET", "call_time": "9:00 AM CET"},
    "BALY": {"date": "May 1, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "BYD": {"date": "Apr 23, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "RRR": {"date": "May 4, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "GDEN": {"date": "May 8, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "MCRI": {"date": "Jul 22, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "CNTY": {"date": "May 9, 2026", "report_time": "Pre-Market", "call_time": "10:00 AM EST"},
    "GENI": {"date": "May 14, 2026", "report_time": "Pre-Market", "call_time": "8:00 AM EST"},
    "BRSL": {"date": "May 12, 2026", "report_time": "Pre-Market", "call_time": "8:00 AM EST"},
    "INSE": {"date": "May 9, 2026", "report_time": "Post-Market", "call_time": "5:00 PM EST"},
    "SGR.AX": {"date": "Aug 26, 2026", "report_time": "Pre-Market AEST", "call_time": "10:00 AM AEST"},
    "GENM.KL": {"date": "May 28, 2026", "report_time": "5:30 PM MYT", "call_time": "9:00 AM MYT (Next Day)"},
    "VICI": {"date": "May 1, 2026", "report_time": "Post-Market", "call_time": "10:00 AM EST (Next Day)"},
    "GLPI": {"date": "Apr 24, 2026", "report_time": "Post-Market", "call_time": "9:00 AM EST (Next Day)"},
    "FLL": {"date": "May 6, 2026", "report_time": "Post-Market", "call_time": "4:30 PM EST"},
    "OPAP.AT": {"date": "May 26, 2026", "report_time": "5:30 PM EET", "call_time": "4:00 PM EET (Next Day)"},
    "TIMA.F": {"date": "May 7, 2026", "report_time": "7:30 AM CET", "call_time": "10:00 AM CET"},
    "GMR.L": {"date": "Sep 15, 2026", "report_time": "7:00 AM BST", "call_time": "9:00 AM BST"},
    "PARP.PA": {"date": "Jun 10, 2026", "report_time": "6:00 PM CET", "call_time": "No Call"},
    "ACX.DE": {"date": "May 5, 2026", "report_time": "7:30 AM CET", "call_time": "10:00 AM CET"},
    "GAMB": {"date": "May 16, 2026", "report_time": "Pre-Market", "call_time": "8:00 AM EST"},
    "BETMGM": {"date": "Tied to MGM/Entain", "report_time": "N/A", "call_time": "N/A"},
    "ACEL": {"date": "May 7, 2026", "report_time": "Post-Market", "call_time": "5:30 PM EST"}
}

def get_live_fx_rates():
    print("🌍 Fetching live Forex rates...")
    rates = {'USD': 1.0, '$': 1.0}
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
    price, mc_usd_val = 0, 0
    price_str, mc_display, pe_str, de_str = "N/A", "N/A", "N/A", "N/A"
    fy_rev_str, interim_rev_str = "N/A", "N/A"
    dyn_net_inc, dyn_ebitda, dyn_fcf = "N/A", "N/A", "N/A"
    dyn_eps_act, dyn_eps_est, dyn_date = None, None, None
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
        
        try:
            cal_data = ytk.calendar
            if isinstance(cal_data, dict) and 'Earnings Date' in cal_data:
                dates = cal_data['Earnings Date']
                if isinstance(dates, list) and len(dates) > 0:
                    first_date = dates[0]
                    if hasattr(first_date, 'strftime'):
                        dyn_date = first_date.strftime('%b %d, %Y')
                    else:
                        dyn_date = pd.to_datetime(first_date).strftime('%b %d, %Y')
        except Exception: pass
            
        return price_str, price, mc_display, mc_usd_val, pe_str, de_str, fy_rev_str, interim_rev_str, dyn_net_inc, dyn_ebitda, dyn_fcf, dyn_eps_act, dyn_eps_est, dyn_date
        
    except Exception:
        return "N/A", 0, "N/A", 0, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", None, None, None

def fetch_stock_history(ticker, native_price_raw):
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

        prompt = f"""Act as an expert iGaming financial analyst. Review these recent financial headlines for {company_name}: {' | '.join(headlines)}. 
Generate a strictly valid JSON response. 
CRITICAL RULE: DO NOT WRAP YOUR RESPONSE IN MARKDOWN BLOCKQUOTES. DO NOT USE ```json. START IMMEDIATELY WITH {{ AND END WITH }}.

Format exactly with these four keys:
1. "summary": A list of 3 string bullet points summarizing the news.
2. "sentiment": An integer from 0 to 100 representing market sentiment.
3. "reading_room": An HTML formatted string using <p>, <strong>, <ul>, and <li> tags. Provide an 'Executive Analyst Briefing' based on the news. Use single quotes for any HTML classes or attributes.
4. "quotes": A list of exactly 2 distinct string sentences containing strategic management quotes. CRITICAL INSTRUCTION: You MUST attribute the quote to the REAL, VERIFIED NAME of the executive (e.g., 'Jason Robins, CEO:' or 'Amy Howe, CEO:'). You are STRICTLY FORBIDDEN from using placeholder terms like 'Company Management', 'Management', or 'The CEO'."""
        
        ai_resp = client.models.generate_content(
            model='gemini-2.5-flash', 
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        
        raw_text = ai_resp.text.strip()
        raw_text = re.sub(r'^```json\s*', '', raw_text)
        raw_text = re.sub(r'^```\s*', '', raw_text)
        raw_text = re.sub(r'\s*```$', '', raw_text)
        
        try:
            data = json.loads(raw_text)
            return data
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(0))
                    return data
                except Exception:
                    pass
            return {"summary": ["Data temporarily unavailable while AI processes news."], "sentiment": 50, "reading_room": "<p>AI output could not be parsed.</p>", "quotes": []}
        
    except Exception as e:
        return {"summary": [f"News Error: {str(e)[:60]}"], "sentiment": 50, "reading_room": f"<p>Error: {str(e)[:60]}</p>", "quotes": []}

# --- 3. PIPELINE EXECUTION ---

def run_pipeline():
    master_db = []
    print(f"🚀 Starting Pipeline processing {len(TARGET_COMPANIES)} companies...")
    
    # NEW: Generate UTC Timestamp for the frontend
    run_time_utc = datetime.utcnow().isoformat() + "Z"
    
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
            
            last_price_str, native_price_raw, mc_str, mc_usd, pe_ratio, debt_equity, dyn_fy_rev, dyn_int_rev, dyn_net_inc, dyn_ebitda, dyn_fcf, dyn_eps_act, dyn_eps_est, dyn_date = get_stock_fundamentals(ticker, fx_rates)
            
            last_price_str = last_price_str if last_price_str != "N/A" else fin.get("fallback_price", "N/A")
            mc_str = mc_str if mc_str != "N/A" else fin.get("fallback_mcap", "N/A")
            pe_ratio = pe_ratio if pe_ratio != "N/A" else fin.get("fallback_pe", "N/A")
            debt_equity = debt_equity if debt_equity != "N/A" else fin.get("fallback_debt", "N/A")
            
            fin["revenue_fy"] = dyn_fy_rev if dyn_fy_rev != "N/A" else fin.get("revenue_fy", "N/A")
            fin["revenue_interim"] = dyn_int_rev if dyn_int_rev != "N/A" else fin.get("revenue_interim", "N/A")
            fin["net_income"] = dyn_net_inc if dyn_net_inc != "N/A" else fin.get("net_income", "N/A")
            fin["ebitda"] = dyn_ebitda if dyn_ebitda != "N/A" else fin.get("ebitda", "N/A")
            fin["fcf"] = dyn_fcf if dyn_fcf != "N/A" else fin.get("fcf", "N/A")
            
            if dyn_date and dyn_date != "N/A":
                cal["date"] = dyn_date
            
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
            "logo": f"https://icon.horse/icon/{co['domain']}",
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
            "history": history,
            "last_updated": run_time_utc # INJECTED TIMESTAMP
        })
        
        time.sleep(10)

    if master_db:
        with open('gambling_stocks_live.json', 'w') as f:
            json.dump(master_db, f, indent=4)
        print(f"\n✅ Pipeline Complete. Saved {len(master_db)} companies.")
    else:
        sys.exit(1)

if __name__ == "__main__":
    try: run_pipeline()
    except Exception: sys.exit(1)
