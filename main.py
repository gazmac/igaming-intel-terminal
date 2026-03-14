import json
import os
import pandas as pd
import time
import yfinance as yf
import re
import sys
import feedparser
import urllib.parse
from google import genai
from datetime import datetime

# --- 1. CONFIGURATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_ACTUAL_API_KEY_HERE")
client = genai.Client(api_key=GEMINI_API_KEY)

try:
    with open('verified_calendar.json', 'r') as f:
        VERIFIED_CALENDAR = json.load(f)
except FileNotFoundError:
    VERIFIED_CALENDAR = {}

# DEFAULT CALENDAR OVERRIDES
DEFAULT_CALENDAR = {
    "6425.T": {
        "date": "May 14, 2026", 
        "report_time": "After Market", 
        "call_time": "TBD"
    },
    "2767.T": {
        "date": "May 12, 2026", 
        "report_time": "After Market", 
        "call_time": "TBD"
    },
    "TLC.AX": {
        "date": "Aug 20, 2026", 
        "report_time": "Pre Market", 
        "call_time": "10:00 AM AEST"
    },
    "SKC.NZ": {
        "date": "Aug 21, 2026", 
        "report_time": "Pre Market", 
        "call_time": "TBD"
    },
    "JIN.AX": {
        "date": "Aug 25, 2026", 
        "report_time": "Pre Market", 
        "call_time": "TBD"
    },
    "ESON.LS": {
        "date": "May 25, 2026", 
        "report_time": "After Market", 
        "call_time": "TBD"
    },
    "GMBL": {
        "date": "May 15, 2026", 
        "report_time": "TBD", 
        "call_time": "TBD"
    },
    "DELTACORP.NS": {
        "date": "Apr 28, 2026", 
        "report_time": "After Market", 
        "call_time": "TBD"
    },
    "AGI.AX": {
        "date": "Aug 26, 2026", 
        "report_time": "Pre Market", 
        "call_time": "TBD"
    }
}

for k, v in DEFAULT_CALENDAR.items():
    if k not in VERIFIED_CALENDAR or VERIFIED_CALENDAR[k].get('date') == 'TBD':
        VERIFIED_CALENDAR[k] = v

# --- HISTORICAL SENTIMENT TRACKER ---
PREV_DATA = {}
try:
    if os.path.exists('gambling_stocks_live.json'):
        with open('gambling_stocks_live.json', 'r') as f:
            prev_list = json.load(f)
            for item in prev_list:
                PREV_DATA[item['ticker']] = item
except Exception as e:
    pass

# --- TARGET ETFS ---
TARGET_ETFS = [
    {
        "name": "Roundhill Sports Betting & iGaming ETF",
        "ticker": "BETZ",
        "logo": "https://logo.clearbit.com/roundhillinvestments.com"
    },
    {
        "name": "VanEck Gaming ETF",
        "ticker": "BJK",
        "logo": "https://logo.clearbit.com/vaneck.com"
    },
    {
        "name": "Pacer BlueStar Digital Entertainment ETF",
        "ticker": "ODDS",
        "logo": "https://logo.clearbit.com/paceretfs.com"
    },
    {
        "name": "HANetf Sports Betting & iGaming UCITS ETF",
        "ticker": "BETZ.L",
        "logo": "https://logo.clearbit.com/hanetf.com"
    }
]

# --- TARGET COMPANIES ---
TARGET_COMPANIES = [
    {
        "name": "Flutter Entertainment", 
        "ticker": "FLUT", 
        "domain": "flutter.com", 
        "base_country": "Ireland"
    },
    {
        "name": "DraftKings", 
        "ticker": "DKNG", 
        "domain": "draftkings.com", 
        "base_country": "USA"
    },
    {
        "name": "Entain PLC", 
        "ticker": "ENT.L", 
        "domain": "entaingroup.com", 
        "base_country": "UK"
    },
    {
        "name": "Evolution AB", 
        "ticker": "EVO.ST", 
        "domain": "evolution.com", 
        "base_country": "Sweden"
    },
    {
        "name": "MGM Resorts", 
        "ticker": "MGM", 
        "domain": "mgmresorts.com", 
        "base_country": "USA"
    },
    {
        "name": "Caesars Entertainment", 
        "ticker": "CZR", 
        "domain": "caesars.com", 
        "base_country": "USA"
    },
    {
        "name": "Penn Entertainment", 
        "ticker": "PENN", 
        "domain": "pennentertainment.com", 
        "base_country": "USA"
    },
    {
        "name": "Las Vegas Sands", 
        "ticker": "LVS", 
        "domain": "sands.com", 
        "base_country": "USA"
    },
    {
        "name": "Wynn Resorts", 
        "ticker": "WYNN", 
        "domain": "wynnresorts.com", 
        "base_country": "USA"
    },
    {
        "name": "Evoke plc", 
        "ticker": "EVOK.L", 
        "domain": "evokeplc.com", 
        "base_country": "UK"
    },
    {
        "name": "Sportradar", 
        "ticker": "SRAD", 
        "domain": "sportradar.com", 
        "base_country": "Switzerland"
    },
    {
        "name": "Betsson AB", 
        "ticker": "BETS-B.ST", 
        "domain": "betssongroup.com", 
        "base_country": "Sweden"
    },
    {
        "name": "Playtech", 
        "ticker": "PTEC.L", 
        "domain": "playtech.com", 
        "base_country": "UK"
    },
    {
        "name": "Churchill Downs", 
        "ticker": "CHDN", 
        "domain": "churchilldownsincorporated.com", 
        "base_country": "USA"
    },
    {
        "name": "Light & Wonder", 
        "ticker": "LNW", 
        "domain": "lnw.com", 
        "base_country": "USA"
    },
    {
        "name": "Aristocrat Leisure", 
        "ticker": "ALL.AX", 
        "domain": "aristocrat.com", 
        "base_country": "Australia"
    },
    {
        "name": "Super Group", 
        "ticker": "SGHC", 
        "domain": "supergroup.com", 
        "base_country": "Guernsey"
    },
    {
        "name": "Rush Street Interactive", 
        "ticker": "RSI", 
        "domain": "rushstreetinteractive.com", 
        "base_country": "USA"
    },
    {
        "name": "Bragg Gaming Group", 
        "ticker": "BRAG", 
        "domain": "bragg.group", 
        "base_country": "Canada"
    },
    {
        "name": "Kambi Group", 
        "ticker": "KAMBI.ST", 
        "domain": "kambi.com", 
        "base_country": "Malta"
    },
    {
        "name": "Galaxy Entertainment", 
        "ticker": "0027.HK", 
        "domain": "galaxyentertainment.com", 
        "base_country": "Hong Kong"
    },
    {
        "name": "Melco Resorts", 
        "ticker": "MLCO", 
        "domain": "cityofdreamsmacau.com", 
        "base_country": "Hong Kong"
    }, 
    {
        "name": "SJM Holdings", 
        "ticker": "1980.HK", 
        "domain": "sjmresorts.com", 
        "base_country": "Hong Kong"
    }, 
    {
        "name": "Wynn Macau", 
        "ticker": "1128.HK", 
        "domain": "wynnresorts.com", 
        "base_country": "Macau"
    }, 
    {
        "name": "Genting Singapore", 
        "ticker": "G13.SI", 
        "domain": "gentingsingapore.com", 
        "base_country": "Singapore"
    },
    {
        "name": "La Française des Jeux", 
        "ticker": "FDJ.PA", 
        "domain": "groupefdj.com", 
        "base_country": "France"
    },
    {
        "name": "Lottomatica Group", 
        "ticker": "LTMC.MI", 
        "domain": "lottomaticagroup.com", 
        "base_country": "Italy"
    },
    {
        "name": "Rank Group", 
        "ticker": "RNK.L", 
        "domain": "rank.com", 
        "base_country": "UK"
    },
    {
        "name": "Better Collective", 
        "ticker": "BETCO.ST", 
        "domain": "bettercollective.com", 
        "base_country": "Denmark"
    },
    {
        "name": "Catena Media", 
        "ticker": "CTM.ST", 
        "domain": "catenamedia.com", 
        "base_country": "Malta", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/catena_media.png"
    }, 
    {
        "name": "Bally's Corporation", 
        "ticker": "BALY", 
        "domain": "ballys.com", 
        "base_country": "USA"
    },
    {
        "name": "Boyd Gaming", 
        "ticker": "BYD", 
        "domain": "boydgaming.com", 
        "base_country": "USA"
    },
    {
        "name": "Red Rock Resorts", 
        "ticker": "RRR", 
        "domain": "stationcasinos.com", 
        "base_country": "USA"
    }, 
    {
        "name": "Golden Entertainment", 
        "ticker": "GDEN", 
        "domain": "goldenent.com", 
        "base_country": "USA"
    },
    {
        "name": "Monarch Casino", 
        "ticker": "MCRI", 
        "domain": "monarchcasino.com", 
        "base_country": "USA"
    },
    {
        "name": "Century Casinos", 
        "ticker": "CNTY", 
        "domain": "cnty.com", 
        "base_country": "USA"
    },
    {
        "name": "Genius Sports", 
        "ticker": "GENI", 
        "domain": "geniussports.com", 
        "base_country": "UK"
    },
    {
        "name": "Brightstar Lottery (fka IGT)", 
        "ticker": "BRSL", 
        "domain": "brightstarlottery.com", 
        "base_country": "UK"
    },
    {
        "name": "Inspired Entertainment", 
        "ticker": "INSE", 
        "domain": "inseinc.com", 
        "base_country": "USA"
    },
    {
        "name": "Star Entertainment", 
        "ticker": "SGR.AX", 
        "domain": "starentertainmentgroup.com.au", 
        "base_country": "Australia"
    },
    {
        "name": "Genting Malaysia", 
        "ticker": "GENM.KL", 
        "domain": "gentingmalaysia.com", 
        "base_country": "Malaysia"
    },
    {
        "name": "VICI Properties", 
        "ticker": "VICI", 
        "domain": "viciproperties.com", 
        "base_country": "USA"
    },
    {
        "name": "Gaming & Leisure Prop", 
        "ticker": "GLPI", 
        "domain": "glpropinc.com", 
        "base_country": "USA"
    },
    {
        "name": "OPAP S.A.", 
        "ticker": "OPAP.AT", 
        "domain": "opap.gr", 
        "base_country": "Greece"
    },
    {
        "name": "Zeal Network", 
        "ticker": "TIMA.F", 
        "domain": "zealnetwork.de", 
        "base_country": "Germany"
    },
    {
        "name": "Gaming Realms", 
        "ticker": "GMR.L", 
        "domain": "gamingrealms.com", 
        "base_country": "UK"
    },
    {
        "name": "Groupe Partouche", 
        "ticker": "PARP.PA", 
        "domain": "groupepartouche.com", 
        "base_country": "France"
    },
    {
        "name": "Bet-at-home", 
        "ticker": "ACX.DE", 
        "domain": "bet-at-home.ag", 
        "base_country": "Germany"
    },
    {
        "name": "Gambling.com Group", 
        "ticker": "GAMB", 
        "domain": "gambling.com", 
        "base_country": "Jersey",
        "logo_override": "https://logo.clearbit.com/gambling.com"
    },
    {
        "name": "BetMGM (MGM/Entain JV)", 
        "ticker": "BETMGM", 
        "domain": "betmgm.com", 
        "base_country": "USA"
    },
    {
        "name": "Full House Resorts", 
        "ticker": "FLL", 
        "domain": "fullhouseresorts.com", 
        "base_country": "USA", 
        "logo_override": "https://logo.clearbit.com/fullhouseresorts.com"
    },
    {
        "name": "Accel Entertainment", 
        "ticker": "ACEL", 
        "domain": "accelentertainment.com", 
        "base_country": "USA", 
        "logo_override": "https://logo.clearbit.com/accelentertainment.com"
    },
    {
        "name": "Codere Online", 
        "ticker": "CDRO", 
        "domain": "codere.com", 
        "base_country": "Luxembourg", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/codere_online.png"
    },
    {
        "name": "The Lottery Corporation", 
        "ticker": "TLC.AX", 
        "domain": "thelotterycorporation.com.au", 
        "base_country": "Australia", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/the_lottery_corp.png"
    },
    {
        "name": "Kangwon Land", 
        "ticker": "035250.KS", 
        "domain": "kangwonland.com", 
        "base_country": "South Korea", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/kangwon_land.png"
    },
    {
        "name": "Tsuburaya Fields", 
        "ticker": "2767.T", 
        "domain": "tsuburaya-fields.co.jp", 
        "base_country": "Japan", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/tsuburaya_fields.png"
    },
    {
        "name": "SkyCity Entertainment", 
        "ticker": "SKC.NZ", 
        "domain": "skycityentertainmentgroup.com", 
        "base_country": "New Zealand"
    },
    {
        "name": "Universal Entertainment", 
        "ticker": "6425.T", 
        "domain": "universal-777.com", 
        "base_country": "Japan", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/universal_entertainment.jpg"
    },
    {
        "name": "Jumbo Interactive", 
        "ticker": "JIN.AX", 
        "domain": "jumbointeractive.com", 
        "base_country": "Australia", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/jumbo_interactive.png"
    },
    {
        "name": "Ainsworth Game Tech", 
        "ticker": "AGI.AX", 
        "domain": "agtslots.com", 
        "base_country": "Australia", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/ainsworth_game_tech.png"
    },
    {
        "name": "Delta Corp", 
        "ticker": "DELTACORP.NS", 
        "domain": "deltacorp.in", 
        "base_country": "India", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/delta_corp.png"
    },
    {
        "name": "Golden Matrix Group", 
        "ticker": "GMGI", 
        "domain": "goldenmatrix.com", 
        "base_country": "USA", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/golden_matrix_group.png"
    },
    {
        "name": "Estoril Sol", 
        "ticker": "ESON.LS", 
        "domain": "estoril-solsgps.com", 
        "base_country": "Portugal", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/estoril_sol.png"
    },
    {
        "name": "Esports Entertainment", 
        "ticker": "GMBL", 
        "domain": "esportsentertainmentgroup.com", 
        "base_country": "Malta", 
        "logo_override": "https://raw.githubusercontent.com/gazmac/igaming-intel-terminal/main/logos/esports_entertainment.png"
    }
]

# --- OTC MAP ---
OTC_MAP = {
    "ENT.L": "GMVHF",
    "EVO.ST": "EVVTY",
    "EVOK.L": "EIHDF", 
    "BETS-B.ST": "BTSBF",
    "PTEC.L": "PYTCF", 
    "ALL.AX": "ARLUF",
    "KAMBI.ST": "KMBIF",
    "0027.HK": "GXYEF",
    "1980.HK": "SJMHF",
    "1128.HK": "WYNMF",
    "G13.SI": "GIGNF",
    "FDJ.PA": "LFDJF",
    "RNK.L": "RANKF",
    "SGR.AX": "EHGRF",
    "GENM.KL": "GMALY",
    "OPAP.AT": "GOFPY",
    "LTMC.MI": "LTMGF",
    "PARP.PA": "PARPF",
    "TLC.AX": "TLRCF",
    "SKC.NZ": "SKYCG",
    "JIN.AX": "JUMBF"
}

# --- VERIFIED DATA DICTIONARY ---
VERIFIED_DATA = {
    "FLUT": {
        "rev_label": "NGR",
        "revenue_fy": "$14.05B (FY '25)",
        "revenue_interim": "$3.79B (Q4 '25)",
        "focus": "B2C Sportsbook & iGaming",
        "map_codes": [
            "US", 
            "GB", 
            "IE", 
            "AU", 
            "IT", 
            "BR"
        ],
        "eps_actual": 1.74,
        "eps_forecast": 1.91,
        "net_income": "$162M",
        "ebitda": "$2.36B",
        "fcf": "$941M",
        "jurisdictions": [
            "US", 
            "UK", 
            "Ireland", 
            "Australia", 
            "Italy"
        ]
    },
    "DKNG": {
        "rev_label": "REV",
        "revenue_fy": "$4.77B (FY '25)",
        "revenue_interim": "$1.39B (Q4 '25)",
        "focus": "B2C Sportsbook & iGaming",
        "map_codes": [
            "US", 
            "CA", 
            "PR"
        ],
        "eps_actual": 0.25,
        "eps_forecast": 0.18,
        "net_income": "-$507M",
        "ebitda": "$181M",
        "fcf": "$270M",
        "jurisdictions": [
            "US", 
            "Ontario", 
            "Puerto Rico"
        ]
    },
    "ENT.L": {
        "rev_label": "NGR",
        "revenue_fy": "£5.33B (FY '25)",
        "revenue_interim": "£2.70B (H2 '25)",
        "focus": "B2C Sportsbook, iGaming & Retail",
        "map_codes": [
            "GB", 
            "IT", 
            "BR", 
            "AU", 
            "ES"
        ],
        "eps_actual": 0.62,
        "eps_forecast": 0.55,
        "net_income": "-£681M",
        "ebitda": "£1.16B",
        "fcf": "£151M",
        "jurisdictions": [
            "UK", 
            "Italy", 
            "Brazil", 
            "Australia"
        ]
    },
    "EVO.ST": {
        "rev_label": "REV",
        "revenue_fy": "€2.21B (FY '25)",
        "revenue_interim": "€625M (Q4 '25)",
        "focus": "B2B Live Casino Technology",
        "map_codes": [
            "SE", 
            "US", 
            "CA", 
            "MT", 
            "LV", 
            "GE", 
            "RO"
        ],
        "eps_actual": 1.54,
        "eps_forecast": 1.46,
        "net_income": "€1.24B",
        "ebitda": "€1.56B",
        "fcf": "€250M",
        "jurisdictions": [
            "Europe", 
            "North America", 
            "LatAm", 
            "Asia"
        ]
    },
    "MGM": {
        "rev_label": "REV",
        "revenue_fy": "$17.2B (FY '25)",
        "revenue_interim": "$4.3B (Q4 '25)",
        "focus": "Land-based Resorts & B2C Digital",
        "map_codes": [
            "US", 
            "CN", 
            "JP"
        ],
        "eps_actual": -1.10,
        "eps_forecast": 0.56,
        "net_income": "$157M",
        "ebitda": "$528M",
        "fcf": "$300M",
        "jurisdictions": [
            "US", 
            "Macau", 
            "Japan"
        ]
    },
    "CZR": {
        "rev_label": "REV",
        "revenue_fy": "$11.4B (FY '25)",
        "revenue_interim": "$2.8B (Q4 '25)",
        "focus": "Land-based Resorts & B2C Digital",
        "map_codes": [
            "US", 
            "CA", 
            "GB", 
            "AE"
        ],
        "eps_actual": -0.34,
        "eps_forecast": 0.10,
        "net_income": "-$72M",
        "ebitda": "$900M",
        "fcf": "$150M",
        "jurisdictions": [
            "US", 
            "Canada", 
            "UK", 
            "UAE"
        ]
    },
    "PENN": {
        "rev_label": "REV",
        "revenue_fy": "$6.3B (FY '25)",
        "revenue_interim": "$1.6B (Q4 '25)",
        "focus": "Land-based Casinos & B2C Digital",
        "map_codes": [
            "US", 
            "CA"
        ],
        "eps_actual": 0.07,
        "eps_forecast": 0.02,
        "net_income": "$15M",
        "ebitda": "$350M",
        "fcf": "$80M",
        "jurisdictions": [
            "US", 
            "Canada"
        ]
    },
    "LVS": {
        "rev_label": "REV",
        "revenue_fy": "$11.5B (FY '25)",
        "revenue_interim": "$2.9B (Q4 '25)",
        "focus": "Land-based Casino Resorts",
        "map_codes": [
            "CN", 
            "SG"
        ],
        "eps_actual": 0.65,
        "eps_forecast": 0.55,
        "net_income": "$450M",
        "ebitda": "$1.2B",
        "fcf": "$600M",
        "jurisdictions": [
            "Macau", 
            "Singapore"
        ]
    },
    "WYNN": {
        "rev_label": "REV",
        "revenue_fy": "$7.14B (FY '25)",
        "revenue_interim": "$1.87B (Q4 '25)",
        "focus": "Luxury Land-based Resorts",
        "map_codes": [
            "US", 
            "CN", 
            "AE"
        ],
        "eps_actual": 0.82,
        "eps_forecast": 2.29,
        "net_income": "$327.3M",
        "ebitda": "$2.22B",
        "fcf": "$800M",
        "jurisdictions": [
            "US", 
            "Macau", 
            "UAE"
        ],
        "fallback_price": "$105.40",
        "fallback_mcap": "$11.8B",
        "fallback_pe": "25.2x",
        "fallback_debt": "850%"
    },
    "EVOK.L": {
        "rev_label": "NGR",
        "revenue_fy": "£1.75B (FY '25)",
        "revenue_interim": "£850M (H2 '25)",
        "focus": "B2C Sportsbook, iGaming & Retail",
        "map_codes": [
            "GB", 
            "IT", 
            "ES", 
            "RO"
        ],
        "eps_actual": -0.05,
        "eps_forecast": 0.01,
        "net_income": "-£191M",
        "ebitda": "£312M",
        "fcf": "£20M",
        "jurisdictions": [
            "UK", 
            "Italy", 
            "Spain"
        ]
    },
    "SRAD": {
        "rev_label": "REV",
        "revenue_fy": "$980M (FY '25)",
        "revenue_interim": "$280M (Q4 '25)",
        "focus": "B2B Sports Data & Technology",
        "map_codes": [
            "CH", 
            "US", 
            "GB", 
            "DE", 
            "AT"
        ],
        "eps_actual": 0.14,
        "eps_forecast": 0.10,
        "net_income": "$35M",
        "ebitda": "$55M",
        "fcf": "$40M",
        "jurisdictions": [
            "Global B2B", 
            "US", 
            "Europe"
        ]
    },
    "BETS-B.ST": {
        "rev_label": "REV",
        "revenue_fy": "€1.0B (FY '25)",
        "revenue_interim": "€260M (Q4 '25)",
        "focus": "B2C & B2B iGaming/Sportsbook",
        "map_codes": [
            "SE", 
            "MT", 
            "IT", 
            "AR", 
            "CO", 
            "PE"
        ],
        "eps_actual": 0.35,
        "eps_forecast": 0.32,
        "net_income": "€45M",
        "ebitda": "€75M",
        "fcf": "€50M",
        "jurisdictions": [
            "Nordics", 
            "LatAm", 
            "CEECA"
        ]
    },
    "PTEC.L": {
        "rev_label": "REV",
        "revenue_fy": "£1.52B (FY '25)",
        "revenue_interim": "£800M (H2 '25)",
        "focus": "B2B iGaming & Sportsbook Tech",
        "map_codes": [
            "GB", 
            "IT", 
            "BG", 
            "UA", 
            "EE"
        ],
        "eps_actual": 0.71,
        "eps_forecast": 0.62,
        "net_income": "£165M",
        "ebitda": "£400.4M",
        "fcf": "£85M",
        "jurisdictions": [
            "UK", 
            "Italy", 
            "LatAm"
        ]
    },
    "CHDN": {
        "rev_label": "REV",
        "revenue_fy": "$2.8B (FY '25)",
        "revenue_interim": "$750M (Q4 '25)",
        "focus": "Racing, Casinos & Online Wagering",
        "map_codes": [
            "US"
        ],
        "eps_actual": 1.35,
        "eps_forecast": 1.20,
        "net_income": "$90M",
        "ebitda": "$300M",
        "fcf": "$120M",
        "jurisdictions": [
            "US"
        ]
    },
    "LNW": {
        "rev_label": "REV",
        "revenue_fy": "$3.1B (FY '25)",
        "revenue_interim": "$800M (Q4 '25)",
        "focus": "B2B Gaming Machines & iGaming",
        "map_codes": [
            "US", 
            "AU", 
            "GB", 
            "SE"
        ],
        "eps_actual": 0.45,
        "eps_forecast": 0.50,
        "net_income": "$45M",
        "ebitda": "$280M",
        "fcf": "$100M",
        "jurisdictions": [
            "US", 
            "Australia", 
            "UK"
        ]
    },
    "ALL.AX": {
        "rev_label": "REV",
        "revenue_fy": "A$6.4B (FY '25)",
        "revenue_interim": "A$3.2B (H2 '25)",
        "focus": "B2B Slots, Social Casino & iGaming",
        "map_codes": [
            "AU", 
            "US", 
            "GB", 
            "IL"
        ],
        "eps_actual": 0.95,
        "eps_forecast": 0.90,
        "net_income": "A$600M",
        "ebitda": "A$1.1B",
        "fcf": "A$750M",
        "jurisdictions": [
            "US", 
            "Australia", 
            "Global"
        ]
    },
    "SGHC": {
        "rev_label": "NGR",
        "revenue_fy": "$1.4B (FY '25)",
        "revenue_interim": "$360M (Q3 '25)",
        "focus": "B2C Sportsbook & iGaming",
        "map_codes": [
            "ZA", 
            "CA", 
            "GB", 
            "MT", 
            "FR"
        ],
        "eps_actual": 0.08,
        "eps_forecast": 0.10,
        "net_income": "$35M",
        "ebitda": "$75M",
        "fcf": "$45M",
        "jurisdictions": [
            "Canada", 
            "Africa", 
            "Europe"
        ]
    },
    "RSI": {
        "rev_label": "REV",
        "revenue_fy": "$950M (FY '25)",
        "revenue_interim": "$250M (Q3 '25)",
        "focus": "B2C Casino-First iGaming",
        "map_codes": [
            "US", 
            "CO", 
            "MX", 
            "CA", 
            "PE"
        ],
        "eps_actual": 0.12,
        "eps_forecast": 0.08,
        "net_income": "$15M",
        "ebitda": "$40M",
        "fcf": "$20M",
        "jurisdictions": [
            "US", 
            "Colombia", 
            "Mexico"
        ]
    },
    "BRAG": {
        "rev_label": "REV",
        "revenue_fy": "$105M (FY '25)",
        "revenue_interim": "$28M (Q3 '25)",
        "focus": "B2B iGaming Content & PAM",
        "map_codes": [
            "CA", 
            "US", 
            "NL", 
            "BR", 
            "FI"
        ],
        "eps_actual": -0.02,
        "eps_forecast": 0.01,
        "net_income": "-$1M",
        "ebitda": "$4M",
        "fcf": "$1M",
        "jurisdictions": [
            "US", 
            "Europe", 
            "Canada"
        ]
    },
    "KAMBI.ST": {
        "rev_label": "REV",
        "revenue_fy": "€180M (FY '25)",
        "revenue_interim": "€45M (Q4 '25)",
        "focus": "B2B Sportsbook Technology",
        "map_codes": [
            "MT", 
            "SE", 
            "GB", 
            "US", 
            "RO", 
            "CO"
        ],
        "eps_actual": 0.18,
        "eps_forecast": 0.15,
        "net_income": "€5M",
        "ebitda": "€15M",
        "fcf": "€8M",
        "jurisdictions": [
            "Global B2B", 
            "US", 
            "LatAm"
        ]
    },
    "0027.HK": {
        "rev_label": "REV",
        "revenue_fy": "HK$31.5B (FY '25)",
        "revenue_interim": "HK$8.5B (Q4 '25)",
        "focus": "Macau Casino Resorts",
        "map_codes": [
            "CN", 
            "HK"
        ],
        "eps_actual": 1.20,
        "eps_forecast": 1.15,
        "net_income": "HK$5.2B",
        "ebitda": "HK$8.1B",
        "fcf": "HK$3.5B",
        "jurisdictions": [
            "Macau"
        ]
    },
    "MLCO": {
        "rev_label": "REV",
        "revenue_fy": "$5.16B (FY '25)",
        "revenue_interim": "$1.29B (Q4 '25)",
        "focus": "Macau & Asia Resorts",
        "map_codes": [
            "CN", 
            "PH", 
            "CY"
        ],
        "eps_actual": 0.16,
        "eps_forecast": -0.05,
        "net_income": "$185M",
        "ebitda": "$1.43B",
        "fcf": "$250M",
        "jurisdictions": [
            "Macau", 
            "Philippines", 
            "Cyprus"
        ]
    },
    "1980.HK": {
        "rev_label": "REV",
        "revenue_fy": "HK$28.17B (FY '25)",
        "revenue_interim": "HK$7.2B (Q4 '25)",
        "focus": "Macau Casino Resorts",
        "map_codes": [
            "CN", 
            "HK"
        ],
        "eps_actual": -0.15,
        "eps_forecast": -0.10,
        "net_income": "-HK$429M",
        "ebitda": "HK$3.2B",
        "fcf": "-HK$200M",
        "jurisdictions": [
            "Macau"
        ]
    },
    "1128.HK": {
        "rev_label": "REV",
        "revenue_fy": "$3.1B (FY '25)",
        "revenue_interim": "$800M (Q4 '25)",
        "focus": "Macau Luxury Resorts",
        "map_codes": [
            "CN", 
            "HK"
        ],
        "eps_actual": 0.35,
        "eps_forecast": 0.30,
        "net_income": "$320M",
        "ebitda": "$900M",
        "fcf": "$450M",
        "jurisdictions": [
            "Macau"
        ]
    },
    "G13.SI": {
        "rev_label": "REV",
        "revenue_fy": "S$2.45B (FY '25)",
        "revenue_interim": "S$1.2B (H2 '25)",
        "focus": "Singapore Integrated Resorts",
        "map_codes": [
            "SG"
        ],
        "eps_actual": 0.03,
        "eps_forecast": 0.04,
        "net_income": "S$390.3M",
        "ebitda": "S$815.8M",
        "fcf": "S$450M",
        "jurisdictions": [
            "Singapore"
        ]
    },
    "FDJ.PA": {
        "rev_label": "NGR",
        "revenue_fy": "€2.82B (FY '25)",
        "revenue_interim": "€1.86B (H1 '25)",
        "focus": "European Lottery & iGaming",
        "map_codes": [
            "FR", 
            "IE"
        ],
        "eps_actual": 1.35,
        "eps_forecast": 1.25,
        "net_income": "€425M",
        "ebitda": "€670M",
        "fcf": "€380M",
        "jurisdictions": [
            "France", 
            "Ireland"
        ],
        "fallback_price": "€25.84",
        "fallback_mcap": "€4.77B",
        "fallback_pe": "18.5x",
        "fallback_debt": "85%"
    },
    "LTMC.MI": {
        "rev_label": "NGR",
        "revenue_fy": "€1.75B (FY '25)",
        "revenue_interim": "€950M (H1 '25)",
        "focus": "Italian Sportsbook & Gaming",
        "map_codes": [
            "IT"
        ],
        "eps_actual": 0.45,
        "eps_forecast": 0.40,
        "net_income": "€180M",
        "ebitda": "€580M",
        "fcf": "€250M",
        "jurisdictions": [
            "Italy"
        ]
    },
    "RNK.L": {
        "rev_label": "NGR",
        "revenue_fy": "£734M (FY '25)",
        "revenue_interim": "£382M (H1 '25)",
        "focus": "UK Retail Casinos & Digital",
        "map_codes": [
            "GB", 
            "ES"
        ],
        "eps_actual": 0.05,
        "eps_forecast": 0.04,
        "net_income": "£25M",
        "ebitda": "£120M",
        "fcf": "£45M",
        "jurisdictions": [
            "UK", 
            "Spain"
        ]
    },
    "BETCO.ST": {
        "rev_label": "REV",
        "revenue_fy": "€350M (FY '25)",
        "revenue_interim": "€180M (H1 '25)",
        "focus": "Global Sports Media Affiliate",
        "map_codes": [
            "DK", 
            "US", 
            "GB", 
            "SE"
        ],
        "eps_actual": 0.40,
        "eps_forecast": 0.35,
        "net_income": "€50M",
        "ebitda": "€110M",
        "fcf": "€65M",
        "jurisdictions": [
            "Europe", 
            "US"
        ]
    },
    "CTM.ST": {
        "rev_label": "REV",
        "revenue_fy": "€46.6M (FY '25)",
        "revenue_interim": "€15.6M (Q4 '25)",
        "focus": "iGaming Lead Generation",
        "map_codes": [
            "MT", 
            "US", 
            "SE"
        ],
        "eps_actual": -0.15,
        "eps_forecast": -0.05,
        "net_income": "-€16.5M",
        "ebitda": "€10.6M",
        "fcf": "€4M",
        "jurisdictions": [
            "US", 
            "Europe"
        ]
    },
    "BALY": {
        "rev_label": "REV",
        "revenue_fy": "$2.4B (FY '25)",
        "revenue_interim": "$620M (Q4 '25)",
        "focus": "US Regional Casinos & iGaming",
        "map_codes": [
            "US", 
            "GB"
        ],
        "eps_actual": -0.55,
        "eps_forecast": -0.40,
        "net_income": "-$180M",
        "ebitda": "$510M",
        "fcf": "-$50M",
        "jurisdictions": [
            "US", 
            "UK"
        ]
    },
    "BYD": {
        "rev_label": "REV",
        "revenue_fy": "$3.8B (FY '25)",
        "revenue_interim": "$950M (Q4 '25)",
        "focus": "US Regional & Locals Casinos",
        "map_codes": [
            "US"
        ],
        "eps_actual": 1.45,
        "eps_forecast": 1.35,
        "net_income": "$520M",
        "ebitda": "$1.3B",
        "fcf": "$600M",
        "jurisdictions": [
            "US"
        ]
    },
    "RRR": {
        "rev_label": "REV",
        "revenue_fy": "$1.8B (FY '25)",
        "revenue_interim": "$460M (Q4 '25)",
        "focus": "Las Vegas Locals Casinos",
        "map_codes": [
            "US"
        ],
        "eps_actual": 0.85,
        "eps_forecast": 0.80,
        "net_income": "$250M",
        "ebitda": "$750M",
        "fcf": "$320M",
        "jurisdictions": [
            "Nevada (US)"
        ]
    },
    "GDEN": {
        "rev_label": "REV",
        "revenue_fy": "$1.1B (FY '25)",
        "revenue_interim": "$270M (Q4 '25)",
        "focus": "Taverns & Regional Casinos",
        "map_codes": [
            "US"
        ],
        "eps_actual": 0.50,
        "eps_forecast": 0.45,
        "net_income": "$80M",
        "ebitda": "$260M",
        "fcf": "$110M",
        "jurisdictions": [
            "US"
        ]
    },
    "MCRI": {
        "rev_label": "REV",
        "revenue_fy": "$520M (FY '25)",
        "revenue_interim": "$130M (Q4 '25)",
        "focus": "Regional US Casinos",
        "map_codes": [
            "US"
        ],
        "eps_actual": 1.15,
        "eps_forecast": 1.10,
        "net_income": "$90M",
        "ebitda": "$170M",
        "fcf": "$80M",
        "jurisdictions": [
            "US"
        ]
    },
    "CNTY": {
        "rev_label": "REV",
        "revenue_fy": "$550M (FY '25)",
        "revenue_interim": "$140M (Q4 '25)",
        "focus": "International Regional Casinos",
        "map_codes": [
            "US", 
            "CA", 
            "PL"
        ],
        "eps_actual": -0.20,
        "eps_forecast": -0.15,
        "net_income": "-$35M",
        "ebitda": "$110M",
        "fcf": "$25M",
        "jurisdictions": [
            "US", 
            "Canada", 
            "Poland"
        ]
    },
    "GENI": {
        "rev_label": "REV",
        "revenue_fy": "$410M (FY '25)",
        "revenue_interim": "$120M (Q4 '25)",
        "focus": "B2B Sports Data Rights",
        "map_codes": [
            "GB", 
            "US", 
            "CO"
        ],
        "eps_actual": 0.05,
        "eps_forecast": 0.02,
        "net_income": "$15M",
        "ebitda": "$55M",
        "fcf": "$20M",
        "jurisdictions": [
            "Global B2B"
        ]
    },
    "BRSL": {
        "rev_label": "REV",
        "revenue_fy": "$2.65B (FY '25)",
        "revenue_interim": "$668M (Q4 '25)",
        "focus": "Pure-Play Global Lottery",
        "map_codes": [
            "US", 
            "IT", 
            "GB"
        ],
        "eps_actual": 0.45,
        "eps_forecast": 0.40,
        "net_income": "$220M",
        "ebitda": "$1.2B",
        "fcf": "$600M",
        "jurisdictions": [
            "US", 
            "Italy", 
            "Global"
        ],
        "fallback_price": "$16.47",
        "fallback_mcap": "$3.34B",
        "fallback_pe": "15.2x",
        "fallback_debt": "150%"
    },
    "INSE": {
        "rev_label": "REV",
        "revenue_fy": "$320M (FY '25)",
        "revenue_interim": "$80M (Q3 '25)",
        "focus": "VLTs & Virtual Sports",
        "map_codes": [
            "US", 
            "GB", 
            "GR"
        ],
        "eps_actual": -0.18,
        "eps_forecast": 0.24,
        "net_income": "-$4.5M",
        "ebitda": "$100M",
        "fcf": "$35M",
        "jurisdictions": [
            "UK", 
            "North America"
        ]
    },
    "SGR.AX": {
        "rev_label": "REV",
        "revenue_fy": "A$1.8B (FY '25)",
        "revenue_interim": "A$850M (H2 '25)",
        "focus": "Australian Casino Resorts",
        "map_codes": [
            "AU"
        ],
        "eps_actual": -0.85,
        "eps_forecast": -0.50,
        "net_income": "-A$1.2B",
        "ebitda": "A$280M",
        "fcf": "-A$150M",
        "jurisdictions": [
            "Australia"
        ]
    },
    "GENM.KL": {
        "rev_label": "REV",
        "revenue_fy": "RM 10.2B (FY '25)",
        "revenue_interim": "RM 2.6B (Q4 '25)",
        "focus": "Asian Integrated Resorts",
        "map_codes": [
            "MY", 
            "US", 
            "GB", 
            "BS"
        ],
        "eps_actual": 0.15,
        "eps_forecast": 0.12,
        "net_income": "RM 600M",
        "ebitda": "RM 3.1B",
        "fcf": "RM 1.2B",
        "jurisdictions": [
            "Malaysia", 
            "UK", 
            "US"
        ],
        "fallback_price": "RM 2.65",
        "fallback_mcap": "RM 15.8B",
        "fallback_pe": "15.3x",
        "fallback_debt": "115%"
    },
    "VICI": {
        "rev_label": "REV",
        "revenue_fy": "$3.6B (FY '25)",
        "revenue_interim": "$950M (Q4 '25)",
        "focus": "Gaming & Hospitality REIT",
        "map_codes": [
            "US", 
            "CA"
        ],
        "eps_actual": 0.65,
        "eps_forecast": 0.60,
        "net_income": "$1.8B",
        "ebitda": "$2.9B",
        "fcf": "$2.1B",
        "jurisdictions": [
            "US", 
            "Canada"
        ]
    },
    "GLPI": {
        "rev_label": "REV",
        "revenue_fy": "$1.4B (FY '25)",
        "revenue_interim": "$360M (Q4 '25)",
        "focus": "Gaming & Leisure REIT",
        "map_codes": [
            "US"
        ],
        "eps_actual": 0.75,
        "eps_forecast": 0.70,
        "net_income": "$650M",
        "ebitda": "$1.2B",
        "fcf": "$800M",
        "jurisdictions": [
            "US"
        ]
    },
    "FLL": {
        "rev_label": "REV",
        "revenue_fy": "$300M (FY '25)",
        "revenue_interim": "$75.5M (Q4 '25)",
        "focus": "US Regional Casinos",
        "map_codes": [
            "US"
        ],
        "eps_actual": -0.34,
        "eps_forecast": -0.23,
        "net_income": "-$10M",
        "ebitda": "$48.1M",
        "fcf": "$5M",
        "jurisdictions": [
            "US"
        ]
    },
    "OPAP.AT": {
        "rev_label": "NGR",
        "revenue_fy": "€2.2B (FY '25)",
        "revenue_interim": "€1.1B (H1 '25)",
        "focus": "Greek Lottery & Betting Monopoly",
        "map_codes": [
            "GR", 
            "CY"
        ],
        "eps_actual": 1.15,
        "eps_forecast": 1.05,
        "net_income": "€420M",
        "ebitda": "€750M",
        "fcf": "€500M",
        "jurisdictions": [
            "Greece", 
            "Cyprus"
        ]
    },
    "TIMA.F": {
        "rev_label": "REV",
        "revenue_fy": "€140M (FY '25)",
        "revenue_interim": "€75M (H1 '25)",
        "focus": "Online Lottery Broker",
        "map_codes": [
            "DE", 
            "GB"
        ],
        "eps_actual": 0.85,
        "eps_forecast": 0.80,
        "net_income": "€30M",
        "ebitda": "€45M",
        "fcf": "€35M",
        "jurisdictions": [
            "Germany"
        ]
    },
    "GMR.L": {
        "rev_label": "REV",
        "revenue_fy": "£28M (FY '25)",
        "revenue_interim": "£15M (H1 '25)",
        "focus": "Mobile Slingo & iGaming Content",
        "map_codes": [
            "GB", 
            "US", 
            "CA"
        ],
        "eps_actual": 0.03,
        "eps_forecast": 0.02,
        "net_income": "£5M",
        "ebitda": "£10M",
        "fcf": "£7M",
        "jurisdictions": [
            "US", 
            "UK"
        ]
    },
    "PARP.PA": {
        "rev_label": "REV",
        "revenue_fy": "€445M (FY '25)",
        "revenue_interim": "€225M (H1 '25)",
        "focus": "French Casino Operator",
        "map_codes": [
            "FR", 
            "CH"
        ],
        "eps_actual": 0.45,
        "eps_forecast": 0.40,
        "net_income": "€25M",
        "ebitda": "€85M",
        "fcf": "€40M",
        "jurisdictions": [
            "France", 
            "Switzerland"
        ]
    },
    "ACX.DE": {
        "rev_label": "REV",
        "revenue_fy": "€60M (FY '25)",
        "revenue_interim": "€30M (H1 '25)",
        "focus": "European Sportsbook",
        "map_codes": [
            "DE", 
            "AT"
        ],
        "eps_actual": -0.15,
        "eps_forecast": -0.10,
        "net_income": "-€5M",
        "ebitda": "€2M",
        "fcf": "-€1M",
        "jurisdictions": [
            "DACH Region"
        ]
    },
    "GAMB": {
        "rev_label": "REV",
        "revenue_fy": "$165.4M (FY '25)",
        "revenue_interim": "$46.2M (Q4 '25)",
        "focus": "iGaming Performance Marketing",
        "map_codes": [
            "US", 
            "GB", 
            "IE"
        ],
        "eps_actual": 0.30,
        "eps_forecast": 0.21,
        "net_income": "-$26.9M",
        "ebitda": "$15.5M",
        "fcf": "$36.3M",
        "jurisdictions": [
            "US", 
            "UK"
        ]
    },
    "BETMGM": {
        "rev_label": "REV",
        "revenue_fy": "$2.8B (FY '25)",
        "revenue_interim": "$780M (Q4 '25)",
        "focus": "B2C Sportsbook & iGaming",
        "map_codes": [
            "US", 
            "CA", 
            "PR"
        ],
        "eps_actual": 0,
        "eps_forecast": 0,
        "net_income": "$175M",
        "ebitda": "$220M",
        "fcf": "N/A",
        "jurisdictions": [
            "US", 
            "Ontario", 
            "Puerto Rico"
        ]
    },
    "ACEL": {
        "rev_label": "REV",
        "revenue_fy": "$1.33B (FY '25)",
        "revenue_interim": "$341.4M (Q4 '25)",
        "focus": "Distributed Gaming & Slot Routes",
        "map_codes": [
            "US"
        ],
        "eps_actual": 0.60,
        "eps_forecast": 0.41,
        "net_income": "$51.3M",
        "ebitda": "$210.1M",
        "fcf": "$150.9M",
        "jurisdictions": [
            "US"
        ],
        "fallback_price": "$11.07",
        "fallback_mcap": "$950M",
        "fallback_pe": "21.1x",
        "fallback_debt": "150%"
    },
    "CDRO": {
        "rev_label": "REV",
        "revenue_fy": "€151M (FY '25)",
        "revenue_interim": "€43M (Q4 '25)",
        "focus": "LatAm & Euro Sportsbook",
        "map_codes": [
            "ES", 
            "MX", 
            "CO", 
            "PA", 
            "AR"
        ],
        "eps_actual": -0.05,
        "eps_forecast": 0.02,
        "net_income": "€1M",
        "ebitda": "€15M",
        "fcf": "€5M",
        "jurisdictions": [
            "Spain", 
            "LatAm"
        ]
    },
    "TLC.AX": {
        "rev_label": "REV",
        "revenue_fy": "A$3.5B (FY '25)",
        "revenue_interim": "A$1.7B (H1 '25)",
        "focus": "Australian Lotteries & Keno",
        "map_codes": [
            "AU"
        ],
        "eps_actual": 0.15,
        "eps_forecast": 0.14,
        "net_income": "A$280M",
        "ebitda": "A$700M",
        "fcf": "A$500M",
        "jurisdictions": [
            "Australia"
        ]
    },
    "035250.KS": {
        "rev_label": "REV",
        "revenue_fy": "₩1.46T (FY '25)",
        "revenue_interim": "₩365B (Q4 '25)",
        "focus": "Korean Domestic Casino",
        "map_codes": [
            "KR"
        ],
        "eps_actual": 1807.0,
        "eps_forecast": 1750.0,
        "net_income": "₩328B",
        "ebitda": "₩400B",
        "fcf": "₩250B",
        "jurisdictions": [
            "South Korea"
        ]
    },
    "2767.T": {
        "rev_label": "REV",
        "revenue_fy": "¥115B (FY '25)",
        "revenue_interim": "¥28B (Q4 '25)",
        "focus": "Pachinko & Amusement Media",
        "map_codes": [
            "JP"
        ],
        "eps_actual": 180.0,
        "eps_forecast": 175.0,
        "net_income": "¥10B",
        "ebitda": "¥18B",
        "fcf": "¥12B",
        "jurisdictions": [
            "Japan"
        ]
    },
    "SKC.NZ": {
        "rev_label": "REV",
        "revenue_fy": "NZ$926M (FY '25)",
        "revenue_interim": "NZ$450M (H1 '25)",
        "focus": "NZ & Aussie Casino Resorts",
        "map_codes": [
            "NZ", 
            "AU"
        ],
        "eps_actual": 0.12,
        "eps_forecast": 0.10,
        "net_income": "NZ$45M",
        "ebitda": "NZ$280M",
        "fcf": "NZ$150M",
        "jurisdictions": [
            "New Zealand", 
            "Australia"
        ]
    },
    "6425.T": {
        "rev_label": "REV",
        "revenue_fy": "¥148B (FY '25)",
        "revenue_interim": "¥35B (Q4 '25)",
        "focus": "Pachinko & Philippine Resort",
        "map_codes": [
            "JP", 
            "PH"
        ],
        "eps_actual": 225.0,
        "eps_forecast": 210.0,
        "net_income": "¥25B",
        "ebitda": "¥40B",
        "fcf": "¥20B",
        "jurisdictions": [
            "Japan", 
            "Philippines"
        ]
    },
    "JIN.AX": {
        "rev_label": "REV",
        "revenue_fy": "A$148M (FY '25)",
        "revenue_interim": "A$72M (H1 '25)",
        "focus": "Digital Lottery Retailing",
        "map_codes": [
            "AU", 
            "GB", 
            "CA"
        ],
        "eps_actual": 0.65,
        "eps_forecast": 0.60,
        "net_income": "A$45M",
        "ebitda": "A$65M",
        "fcf": "A$50M",
        "jurisdictions": [
            "Australia", 
            "UK", 
            "Canada"
        ]
    },
    "AGI.AX": {
        "rev_label": "REV",
        "revenue_fy": "A$284M (FY '25)",
        "revenue_interim": "A$140M (H1 '25)",
        "focus": "B2B Slot Machines",
        "map_codes": [
            "AU", 
            "US", 
            "AR"
        ],
        "eps_actual": 0.05,
        "eps_forecast": 0.04,
        "net_income": "A$15M",
        "ebitda": "A$40M",
        "fcf": "A$20M",
        "jurisdictions": [
            "Australia", 
            "US", 
            "LatAm"
        ]
    },
    "DELTACORP.NS": {
        "rev_label": "REV",
        "revenue_fy": "₹709 Cr (FY '25)",
        "revenue_interim": "₹151 Cr (Q4 '25)",
        "focus": "Indian Offshore Casinos",
        "map_codes": [
            "IN", 
            "NP"
        ],
        "eps_actual": 5.57,
        "eps_forecast": 5.00,
        "net_income": "₹120 Cr",
        "ebitda": "₹180 Cr",
        "fcf": "₹100 Cr",
        "jurisdictions": [
            "India", 
            "Nepal"
        ]
    },
    "GMGI": {
        "rev_label": "REV",
        "revenue_fy": "$179M (FY '25)",
        "revenue_interim": "$43M (Q4 '25)",
        "focus": "B2B iGaming & Sports",
        "map_codes": [
            "US", 
            "RS", 
            "MX"
        ],
        "eps_actual": -0.05,
        "eps_forecast": 0.03,
        "net_income": "-$1.4M",
        "ebitda": "$15.7M",
        "fcf": "$12.6M",
        "jurisdictions": [
            "US", 
            "Balkans", 
            "LatAm"
        ]
    },
    "ESON.LS": {
        "rev_label": "REV",
        "revenue_fy": "€255M (FY '25)",
        "revenue_interim": "€65M (Q4 '25)",
        "focus": "Portuguese Casinos & iGaming",
        "map_codes": [
            "PT"
        ],
        "eps_actual": 0.30,
        "eps_forecast": 0.25,
        "net_income": "€18M",
        "ebitda": "€45M",
        "fcf": "€25M",
        "jurisdictions": [
            "Portugal"
        ]
    },
    "GMBL": {
        "rev_label": "REV",
        "revenue_fy": "$15M (FY '25)",
        "revenue_interim": "$3M (Q3 '25)",
        "focus": "Esports Betting & Events",
        "map_codes": [
            "US", 
            "MT"
        ],
        "eps_actual": -1.50,
        "eps_forecast": -1.00,
        "net_income": "-$25M",
        "ebitda": "-$10M",
        "fcf": "-$5M",
        "jurisdictions": [
            "Malta", 
            "US"
        ]
    }
}

def get_live_fx_rates():
    rates = {'USD': 1.0, '$': 1.0}
    pairs = {
        'GBP': 'GBPUSD=X', 
        'GBp': 'GBPUSD=X', 
        'EUR': 'EURUSD=X', 
        'SEK': 'SEKUSD=X',
        'AUD': 'AUDUSD=X', 
        'CAD': 'CADUSD=X', 
        'HKD': 'HKDUSD=X', 
        'SGD': 'SGDUSD=X',
        'MYR': 'MYRUSD=X', 
        'KRW': 'KRWUSD=X', 
        'JPY': 'JPYUSD=X', 
        'NZD': 'NZDUSD=X', 
        'INR': 'INRUSD=X'
    }
    for currency, ticker in pairs.items():
        try:
            val = yf.Ticker(ticker).fast_info['lastPrice']
            if currency == 'GBp': 
                val = val / 100.0 
            rates[currency] = val
        except Exception:
            rates[currency] = 1.0 
    return rates

def format_money(raw_val, sym):
    if pd.isna(raw_val): 
        return "N/A"
    is_neg = raw_val < 0
    abs_val = abs(raw_val)
    if abs_val >= 1e9: 
        res = f"{sym}{round(abs_val/1e9, 2)}B"
    elif abs_val >= 1e6: 
        res = f"{sym}{round(abs_val/1e6, 2)}M"
    else: 
        res = f"{sym}{round(abs_val, 2)}"
    return f"-{res}" if is_neg else res

def get_stock_fundamentals(ticker, fx_rates):
    price, mc_usd_val = 0, 0
    price_str, mc_display, pe_str, de_str = "N/A", "N/A", "N/A", "N/A"
    fy_rev_str, interim_rev_str = "N/A", "N/A"
    dyn_net_inc, dyn_ebitda, dyn_fcf = "N/A", "N/A", "N/A"
    dyn_eps_act, dyn_eps_est, dyn_date = None, None, None
    daily_change_pct = "N/A"
    pe_raw, de_raw = None, None
    description = "Company description unavailable."
    sym, currency = "$", "USD"
    
    try:
        ytk = yf.Ticker(ticker)
        
        try:
            info = ytk.info
            description = info.get('longBusinessSummary', description)
        except Exception: 
            pass
        
        try:
            price = ytk.fast_info['lastPrice']
            currency = ytk.fast_info['currency']
            prev_close = ytk.fast_info.get('previousClose')
            if price and prev_close and prev_close > 0:
                daily_change_pct = round(((price - prev_close) / prev_close) * 100, 2)
            if daily_change_pct == "N/A" and price > 0:
                hist = ytk.history(period="5d")
                if len(hist) >= 2:
                    fallback_prev = hist['Close'].iloc[-2]
                    daily_change_pct = round(((price - fallback_prev) / fallback_prev) * 100, 2)
        except Exception: 
            pass 
            
        if currency == "GBp": 
            sym = "GBp "
        elif currency == "GBP": 
            sym = "£"
        elif currency == "SEK": 
            sym = "SEK "
        elif currency == "EUR": 
            sym = "€"
        elif currency == "AUD": 
            sym = "A$"
        elif currency == "CAD": 
            sym = "C$"
        elif currency == "HKD": 
            sym = "HK$"
        elif currency == "SGD": 
            sym = "S$"
        elif currency == "MYR": 
            sym = "RM "
        elif currency == "KRW": 
            sym = "₩" 
        elif currency == "JPY": 
            sym = "¥"  
        elif currency == "NZD": 
            sym = "NZ$" 
        elif currency == "INR": 
            sym = "₹"  
        else: 
            sym = "$"
        
        if price > 0: 
            price_str = f"{sym}{round(price, 2)}"
            
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
        except Exception: 
            pass

        try:
            pe_r = info.get('trailingPE') or info.get('forwardPE')
            if pe_r is not None and pe_r > 0: 
                pe_raw = pe_r
                pe_str = f"{round(pe_raw, 2)}"
            else:
                eps = info.get('trailingEps')
                if eps is not None:
                    if eps <= 0: 
                        pe_str = "Neg EPS"
                    elif price > 0:
                        pe_raw = price / eps
                        pe_str = f"{round(pe_raw, 2)}" 

            de_r = info.get('debtToEquity')
            if de_r is not None: 
                de_raw = de_r
                de_str = f"{round(de_raw, 2)}%"
            else:
                total_debt = info.get('totalDebt')
                total_equity = info.get('totalStockholderEquity') 
                if total_debt is not None and total_equity is not None:
                    if total_equity <= 0: 
                        de_str = "Neg Equity" 
                    else:
                        de_raw = (total_debt / total_equity) * 100
                        de_str = f"{round(de_raw, 2)}%" 
                elif total_debt == 0:
                    de_raw = 0
                    de_str = "0.00%"
        except Exception: 
            pass 

        try:
            income_annual = ytk.income_stmt
            if not income_annual.empty:
                raw_rev_fy = None
                if 'Total Revenue' in income_annual.index: 
                    raw_rev_fy = income_annual.loc['Total Revenue'].iloc[0]
                elif 'Operating Revenue' in income_annual.index: 
                    raw_rev_fy = income_annual.loc['Operating Revenue'].iloc[0]
                if pd.notna(raw_rev_fy):
                    fy_year = pd.to_datetime(income_annual.columns[0]).year
                    fy_rev_str = f"{format_money(raw_rev_fy, sym)} (FY '{str(fy_year)[-2:]})"
                
                raw_ebitda = None
                for key in ['Normalized EBITDA', 'EBITDA']:
                    if key in income_annual.index:
                        raw_ebitda = income_annual.loc[key].iloc[0]
                        break
                if pd.notna(raw_ebitda): 
                    dyn_ebitda = format_money(raw_ebitda, sym)
        except Exception: 
            pass

        try:
            cf = ytk.cashflow
            if not cf.empty:
                raw_fcf = None
                if 'Free Cash Flow' in cf.index: 
                    raw_fcf = cf.loc['Free Cash Flow'].iloc[0]
                elif 'Operating Cash Flow' in cf.index and 'Capital Expenditure' in cf.index:
                    raw_fcf = cf.loc['Operating Cash Flow'].iloc[0] + cf.loc['Capital Expenditure'].iloc[0]
                if pd.notna(raw_fcf): 
                    dyn_fcf = format_money(raw_fcf, sym)
        except Exception: 
            pass
        
        try:
            ed = ytk.earnings_dates
            if ed is not None and not ed.empty:
                past_ed = ed[ed['Reported EPS'].notna()]
                if not past_ed.empty:
                    dyn_eps_act = past_ed['Reported EPS'].iloc[0]
                    dyn_eps_est = past_ed['Estimate EPS'].iloc[0]
        except Exception: 
            pass
        
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
        except Exception: 
            pass
            
        return price_str, price, mc_display, mc_usd_val, pe_str, de_str, fy_rev_str, interim_rev_str, dyn_net_inc, dyn_ebitda, dyn_fcf, dyn_eps_act, dyn_eps_est, dyn_date, daily_change_pct, pe_raw, de_raw, description
        
    except Exception:
        return "N/A", 0, "N/A", 0, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", None, None, None, "N/A", None, None, "Company description unavailable."

def fetch_stock_history(ticker, native_price_raw):
    is_otc = ticker in OTC_MAP
    fetch_ticker = OTC_MAP.get(ticker, ticker)
    history = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}
    
    try:
        ytk = yf.Ticker(fetch_ticker)
        
        df_test = ytk.history(period="1mo")
        if df_test.empty and is_otc:
            fetch_ticker = ticker
            ytk = yf.Ticker(fetch_ticker)
            is_otc = False

        df_1d = ytk.history(period="1d", interval="15m")
        if df_1d.empty:
            df_1d = ytk.history(period="5d", interval="1d") 

        if not df_1d.empty:
            history["1d"] = [[int(pd.Timestamp(idx).timestamp() * 1000), float(row['Close'])] for idx, row in df_1d.iterrows()]

        df_5y = ytk.history(period="5y", interval="1d")
        if not df_5y.empty:
            df_5y.index = df_5y.index.tz_localize(None)
            def slice_data(days):
                cutoff = df_5y.index[-1] - pd.Timedelta(days=days)
                sliced = df_5y[df_5y.index >= cutoff]
                return [[int(pd.Timestamp(idx).timestamp() * 1000), float(row['Close'])] for idx, row in sliced.iterrows()]
            
            history["1w"] = slice_data(7)
            history["1m"] = slice_data(30)
            history["3m"] = slice_data(90)
            history["6m"] = slice_data(180)
            history["1y"] = slice_data(365)
            
            df_5y_weekly = df_5y.resample('W').last().dropna()
            history["5y"] = [[int(pd.Timestamp(idx).timestamp() * 1000), float(row['Close'])] for idx, row in df_5y_weekly.iterrows()]
            
        if is_otc and native_price_raw and history["1m"]:
            latest_otc = history["1m"][-1][1]
            if latest_otc > 0:
                ratio = native_price_raw / latest_otc
                for period in history:
                    history[period] = [[pt[0], round(pt[1] * ratio, 2)] for pt in history[period]]
    except Exception:
        pass
    
    return history

def ai_process_intelligence(company_name, ticker, fundamentals, prev_sent):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key or api_key == "YOUR_ACTUAL_API_KEY_HERE":
        return {
            "summary": ["System Error: API key missing."], 
            "sentiment": 50, 
            "rating": "Hold", 
            "reading_room": "<p>API Key required.</p>", 
            "quotes": []
        }
        
    try:
        feed_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}"
        try:
            feed = feedparser.parse(feed_url)
            headlines = [entry.title for entry in feed.entries[:5]]
        except Exception:
            headlines = []
            
        if not headlines:
            fallback_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={company_name.split()[0]}"
            try:
                feed = feedparser.parse(fallback_url)
                headlines = [entry.title for entry in feed.entries[:5]]
            except Exception: 
                pass

        if not headlines:
            safe_name = urllib.parse.quote(f"{company_name} stock")
            google_url = f"https://news.google.com/rss/search?q={safe_name}&hl=en-US&gl=US&ceid=US:en"
            try:
                feed = feedparser.parse(google_url)
                headlines = [entry.title for entry in feed.entries[:5]]
            except Exception: 
                pass
            
        if not headlines:
            return {
                "summary": [f"No recent news found for {company_name}."], 
                "sentiment": 50, 
                "rating": "Hold", 
                "reading_room": "<p>Awaiting fresh press releases.</p>", 
                "quotes": []
            }

        client = genai.Client(api_key=api_key)
        
        prompt = f"""Act as an expert iGaming financial analyst. 
        Company: {company_name} ({ticker})
        Recent Headlines: {' | '.join(headlines)}
        Previous Sentiment Score: {prev_sent}/100
        Fundamentals: P/E Ratio: {fundamentals.get('pe_ratio')}, Debt-to-Equity: {fundamentals.get('debt_to_equity')}, EPS Beat/Miss: {fundamentals.get('eps_beat_miss_pct')}%, Revenue/NGR: {fundamentals.get('revenue')}, Free Cash Flow (FCF): {fundamentals.get('fcf')}
        
        Generate a strictly valid JSON response. 
        Format exactly with these five keys:
        1. "summary": A list of 3 string bullet points summarizing the news. **CRITICAL:** If the new sentiment score you calculate differs from the Previous Sentiment Score ({prev_sent}) by more than 10 points (a spike or drop), you MUST explicitly explain the rationale for this momentum shift in one of the bullet points.
        2. "sentiment": An integer from 0 to 100 representing market sentiment strictly based on the recent news headlines.
        3. "rating": A stock rating (Choose exactly one: "Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"). You MUST calculate this rating by weighing BOTH the fundamental health (Revenue, FCF, P/E, EPS Beats) AND the sentiment/momentum from the recent news headlines.
        4. "reading_room": An HTML formatted string using <p>, <strong>, <ul>, and <li> tags. Provide an 'Executive Analyst Briefing'.
        5. "quotes": A list of exactly 2 distinct string sentences containing strategic management quotes attributed to real names."""
        
        ai_resp = client.models.generate_content(
            model='gemini-2.5-flash', 
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        
        raw_text = ai_resp.text.strip()
        try:
            match = re.search(r'(\{.*\})', raw_text, re.DOTALL)
            if match: 
                return json.loads(match.group(1))
            return json.loads(raw_text)
        except json.JSONDecodeError:
            return {
                "summary": ["Data temporarily unavailable."], 
                "sentiment": 50, 
                "rating": "Hold", 
                "reading_room": "<p>AI parse error.</p>", 
                "quotes": []
            }
            
    except Exception e:
        print(f"  ⚠️ AI process failed for {ticker}: {e}")
        return {
            "summary": [f"News Error: Gathering delayed."], 
            "sentiment": 50, 
            "rating": "Hold", 
            "reading_room": f"<p>Latency issue.</p>", 
            "quotes": []
        }

def get_etf_fundamentals(ticker, fx_rates):
    price, nav_val, aum_val = 0, 0, 0
    price_str, mc_display, exp_ratio_str, aum_str, nav_str = "N/A", "N/A", "N/A", "N/A", "N/A"
    daily_change_pct = "N/A"
    sym, currency = "$", "USD"
    holdings = []
    
    try:
        ytk = yf.Ticker(ticker)
        
        try:
            price = ytk.fast_info['lastPrice']
            currency = ytk.fast_info['currency']
            prev_close = ytk.fast_info.get('previousClose')
            if price and prev_close and prev_close > 0:
                daily_change_pct = round(((price - prev_close) / prev_close) * 100, 2)
        except Exception: 
            pass
        
        if currency == "GBp": 
            sym = "GBp "
        elif currency == "GBP": 
            sym = "£"
        elif currency == "EUR": 
            sym = "€"
        else: 
            sym = "$"
        
        if price > 0: 
            price_str = f"{sym}{round(price, 2)}"
        
        info = ytk.info
        try:
            nav = info.get('navPrice')
            if nav: 
                nav_str = f"{sym}{round(nav, 2)}"
            
            aum = info.get('totalAssets') or info.get('netAssets')
            if aum: 
                aum_str = format_money(aum, sym)
            
            exp = info.get('expenseRatio')
            if exp: 
                exp_ratio_str = f"{round(exp * 100, 2)}%"
        except Exception: 
            pass
        
        try:
            fd = ytk.funds_data
            if fd and hasattr(fd, 'top_holdings'):
                th = fd.top_holdings
                if th is not None and not th.empty:
                    for idx, row in th.head(10).iterrows():
                        w = row.get('Weight', 0)
                        if pd.isna(w): 
                            w = 0
                        else: 
                            w = float(w) * 100
                        holdings.append({
                            "ticker": str(idx),
                            "name": str(row.get('Name', idx)),
                            "weight": round(w, 2)
                        })
        except Exception: 
            pass
        
        history = fetch_stock_history(ticker, price)
        
        return price_str, price, daily_change_pct, exp_ratio_str, aum_str, nav_str, holdings, history
    except Exception:
        return "N/A", 0, "N/A", "N/A", "N/A", "N/A", [], {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}

def run_pipeline():
    master_db = []
    etf_db = []
    print(f"🚀 Starting Pipeline processing {len(TARGET_COMPANIES)} companies and {len(TARGET_ETFS)} ETFs...")
    
    run_time_utc = datetime.utcnow().isoformat() + "Z"
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    fx_rates = get_live_fx_rates()
    
    for co in TARGET_COMPANIES:
        ticker = co['ticker']
        print(f"\nProcessing {co['name']} ({ticker})...")
        
        fin = VERIFIED_DATA.get(ticker, {
            "eps_actual": 0, 
            "eps_forecast": 0, 
            "net_income": "N/A", 
            "ebitda": "N/A", 
            "fcf": "N/A", 
            "jurisdictions": [],
            "focus": "Diversified Gaming", 
            "map_codes": [], 
            "rev_label": "REV", 
            "revenue_fy": "N/A", 
            "revenue_interim": "N/A"
        })
        
        cal = VERIFIED_CALENDAR.get(ticker, {
            "date": "TBD", 
            "report_time": "TBD", 
            "call_time": "TBD"
        })
            
        try:
            last_price_str, price_raw, mc_str, mc_usd, pe_ratio, debt_equity, dyn_fy_rev, dyn_int_rev, dyn_net_inc, dyn_ebitda, dyn_fcf, dyn_eps_act, dyn_eps_est, dyn_date, daily_change_pct, pe_raw, de_raw, description = get_stock_fundamentals(ticker, fx_rates)
            
            last_price_str = last_price_str if last_price_str != "N/A" else fin.get("fallback_price", "N/A")
            mc_str = mc_str if mc_str != "N/A" else fin.get("fallback_mcap", "N/A")
            pe_ratio = pe_ratio if pe_ratio != "N/A" else fin.get("fallback_pe", "N/A")
            debt_equity = debt_equity if debt_equity != "N/A" else fin.get("fallback_debt", "N/A")
            
            fin["revenue_fy"] = dyn_fy_rev if dyn_fy_rev != "N/A" else fin.get("revenue_fy", "N/A")
            fin["revenue_interim"] = dyn_int_rev if dyn_int_rev != "N/A" else fin.get("revenue_interim", "N/A")
            fin["net_income"] = dyn_net_inc if dyn_net_inc != "N/A" else fin.get("net_income", "N/A")
            fin["ebitda"] = dyn_ebitda if dyn_ebitda != "N/A" else fin.get("ebitda", "N/A")
            fin["fcf"] = dyn_fcf if dyn_fcf != "N/A" else fin.get("fcf", "N/A")
            
            if cal.get("date", "TBD") == "TBD" and dyn_date and dyn_date != "N/A":
                cal["date"] = dyn_date
            
            beat_miss = 0
            if dyn_eps_act is not None and dyn_eps_est is not None and dyn_eps_est != 0:
                fin["eps_actual"] = round(dyn_eps_act, 2)
                fin["eps_forecast"] = round(dyn_eps_est, 2)
                beat_miss = round(((dyn_eps_act - dyn_eps_est) / abs(dyn_eps_est)) * 100, 2)
            else:
                if fin.get("eps_forecast", 0) != 0:
                    beat_miss = round(((fin["eps_actual"] - fin["eps_forecast"]) / abs(fin["eps_forecast"])) * 100, 2)

            fund_data_for_ai = {
                "pe_ratio": pe_ratio,
                "debt_to_equity": debt_equity,
                "eps_beat_miss_pct": beat_miss,
                "revenue": fin["revenue_fy"],
                "fcf": fin["fcf"]
            }
            
            prev_sent = PREV_DATA.get(ticker, {}).get("sentiment", 50)
            intel = ai_process_intelligence(co['name'], ticker, fund_data_for_ai, prev_sent)

            history = fetch_stock_history(ticker, price_raw)
            final_logo = co.get("logo_override", f"https://www.google.com/s2/favicons?domain={co['domain']}&sz=128")
            
        except Exception as e:
            print(f"  ⚠️ Critical loop failure for {ticker}: {e}")
            intel = {
                "summary": [f"System Error: {str(e)[:50]}"], 
                "sentiment": 50, 
                "rating": "Hold", 
                "reading_room": "<p>Error</p>", 
                "quotes": []
            }
            history = {"1d": [], "1w": [], "1m": [], "3m": [], "6m": [], "1y": [], "5y": []}
            last_price_str, mc_str, mc_usd, pe_ratio, debt_equity = "N/A", "N/A", 0, "N/A", "N/A"
            beat_miss, daily_change_pct, pe_raw, de_raw, description = 0, "N/A", None, None, "Description unavailable."
            final_logo = f"https://www.google.com/s2/favicons?domain={co['domain']}&sz=128"

        curr_sentiment = intel.get("sentiment", 50)
        sent_history = PREV_DATA.get(ticker, {}).get("sentiment_history", [])
        if not sent_history or sent_history[-1]['date'] != today_str:
            sent_history.append({"date": today_str, "score": curr_sentiment})
        else:
            sent_history[-1]['score'] = curr_sentiment

        master_db.append({
            "ticker": ticker,
            "company": co["name"],
            "domain": co["domain"], 
            "logo": final_logo,
            "base_country": co["base_country"],
            "focus": fin.get("focus", "Diversified Gaming"), 
            "description": description,
            "map_codes": fin.get("map_codes", []),           
            "calendar": cal, 
            "last_price": last_price_str,
            "raw_price": price_raw,
            "daily_change_pct": daily_change_pct,
            "market_cap_str": mc_str,
            "market_cap_usd": mc_usd,
            "pe_ratio": pe_ratio,
            "pe_raw": pe_raw,
            "debt_to_equity": debt_equity,
            "de_raw": de_raw,
            "actuals": fin,
            "eps_beat_miss_pct": beat_miss,
            "news_summary": intel.get("summary", ["Data parsing failed."]),
            "sentiment": curr_sentiment,
            "sentiment_history": sent_history[-30:],
            "rating": intel.get("rating", "Hold"),
            "reading_room": intel.get("reading_room", "<p>Data unavailable.</p>"),
            "quotes": intel.get("quotes", []),
            "jurisdictions": fin.get("jurisdictions", []),
            "history": history,
            "last_updated": run_time_utc
        })
        
        time.sleep(10)

    print(f"\n🚀 Processing ETFs...")
    for e in TARGET_ETFS:
        ticker = e['ticker']
        print(f"  Fetching {ticker}...")
        p_str, p_raw, d_change, exp, aum, nav, holds, hist = get_etf_fundamentals(ticker, fx_rates)
        etf_db.append({
            "name": e['name'],
            "ticker": ticker,
            "logo": e['logo'],
            "last_price": p_str,
            "raw_price": p_raw,
            "daily_change_pct": d_change,
            "expense_ratio": exp,
            "aum": aum,
            "nav": nav,
            "holdings": holds,
            "history": hist,
            "last_updated": run_time_utc
        })

    if master_db:
        with open('gambling_stocks_live.json', 'w') as f:
            json.dump(master_db, f, indent=4)
        print(f"\n✅ Pipeline Complete. Saved {len(master_db)} companies.")
        
    if etf_db:
        with open('etf_data_live.json', 'w') as f:
            json.dump(etf_db, f, indent=4)
        print(f"✅ ETF Pipeline Complete. Saved {len(etf_db)} ETFs.")

if __name__ == "__main__":
    try: 
        run_pipeline()
    except Exception: 
        sys.exit(1)
