import json
import os
import requests
from datetime import datetime, timedelta, timezone
import yfinance as yf
from google import genai
import re
from dateutil import parser
from dateutil.tz import gettz

CALENDAR_FILE = 'verified_calendar.json'
try:
    with open(CALENDAR_FILE, 'r') as f:
        calendar_db = json.load(f)
except FileNotFoundError:
    print("❌ Error: verified_calendar.json not found.")
    exit(1)

def parse_earnings_datetime(date_str, time_str):
    """
    Combines date and time into a strict UTC datetime object.
    Allows the agent to wait until AFTER the specific market call has finished.
    """
    if date_str in ["TBD", "N/A", "Tied to MGM/Entain"]:
        return None
        
    t_str = time_str.upper()
    
    # Fallbacks for vague strings
    if "PRE-MARKET" in t_str or "PRE MARKET" in t_str:
        t_str = "8:00 AM EST" 
    elif "POST-MARKET" in t_str or "POST MARKET" in t_str:
        t_str = "4:30 PM EST" 
    elif "TBD" in t_str or "NO CALL" in t_str:
        t_str = "11:59 PM EST" 
        
    # Strip random text like "(Next Day)" to prevent parser failure
    t_str = re.sub(r'\(.*?\)', '', t_str).strip()
    full_str = f"{date_str} {t_str}"
    
    # Global Timezone Map
    tzinfos = {
        "EST": gettz("America/New_York"), "EDT": gettz("America/New_York"), "ET": gettz("America/New_York"),
        "BST": gettz("Europe/London"), "GMT": gettz("Europe/London"),
        "CET": gettz("Europe/Berlin"), "CEST": gettz("Europe/Berlin"),
        "EET": gettz("Europe/Athens"),
        "AEST": gettz("Australia/Sydney"), "AEDT": gettz("Australia/Sydney"),
        "HKT": gettz("Asia/Hong_Kong"),
        "SGT": gettz("Asia/Singapore"),
        "MYT": gettz("Asia/Kuala_Lumpur")
    }
    
    try:
        dt = parser.parse(full_str, tzinfos=tzinfos)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=gettz("America/New_York"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def is_event_completely_finished(date_str, time_str):
    if date_str in ["TBD", "N/A", "Tied to MGM/Entain"]:
        return False
        
    dt_utc = parse_earnings_datetime(date_str, time_str)
    
    if dt_utc:
        # STRICT RULE: Only trigger 6 hours AFTER the scheduled earnings call 
        # to ensure the final press release has been published to EDGAR/PR Newswire.
        trigger_time = dt_utc + timedelta(hours=6)
        return datetime.now(timezone.utc) > trigger_time
    else:
        # Failsafe if string is highly irregular
        try:
            dt = datetime.strptime(date_str, "%b %d, %Y")
            return datetime.now() > (dt + timedelta(days=2))
        except ValueError:
            return False

def run_calendar_agent():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("⚠️ Warning: GEMINI_API_KEY missing.")
        return

    client = genai.Client(api_key=api_key)
    updates_made = False

    print("🕵️ Initializing Timezone-Aware Calendar Agent...")

    for ticker, data in calendar_db.items():
        old_date = data.get("date", "")
        call_time = data.get("call_time", "TBD")
        
        if is_event_completely_finished(old_date, call_time):
            print(f"  📅 {ticker} event ({old_date} {call_time}) has strictly passed. Activating Agent...")
            
            try:
                ytk = yf.Ticker(ticker)
                news = ytk.news
                headlines = " | ".join([n['title'] for n in news[:5]]) if news else "No recent news available."
            except Exception:
                headlines = "No recent news available."

            prompt = f"""You are an elite financial data engineer. The company with ticker {ticker} recently completed its earnings cycle on {old_date}. 
            Review these recent headlines: {headlines}
            
            Task 1: Did they officially announce the exact date for their NEXT quarterly earnings?
            Task 2: If yes, extract it. If no, mathematically estimate their next earnings date by adding exactly 3 months to '{old_date}'. Do not change the historical 'report_time' or 'call_time' strings.
            
            Format your response STRICTLY as valid JSON. Do not wrap in markdown blockquotes. Start immediately with {{ and end with }}.
            Format:
            {{
                "date": "Month DD, YYYY",
                "report_time": "{data.get('report_time', 'TBD')}",
                "call_time": "{data.get('call_time', 'TBD')}"
            }}"""

            try:
                ai_resp = client.models.generate_content(
                    model='gemini-2.5-flash', 
                    contents=prompt,
                    config={"response_mime_type": "application/json"}
                )
                
                raw_text = ai_resp.text.strip()
                raw_text = re.sub(r'^```json\s*', '', raw_text)
                raw_text = re.sub(r'^```\s*', '', raw_text)
                raw_text = re.sub(r'\s*```$', '', raw_text)
                
                new_data = json.loads(raw_text)
                
                if "date" in new_data and "report_time" in new_data:
                    print(f"     ✅ Agent Updated {ticker}: {old_date} -> {new_data['date']}")
                    calendar_db[ticker] = new_data
                    updates_made = True
            except Exception as e:
                print(f"     ❌ Agent failed to parse {ticker}: {e}")

    if updates_made:
        with open(CALENDAR_FILE, 'w') as f:
            json.dump(calendar_db, f, indent=4)
        print("💾 Calendar database successfully rewritten.")
    else:
        print("💤 No exact calendar rollovers required today.")

if __name__ == "__main__":
    run_calendar_agent()
