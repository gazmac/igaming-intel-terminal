import json
import os
import requests
from datetime import datetime, timedelta
import yfinance as yf
from google import genai
import re

# Load existing JSON
CALENDAR_FILE = 'verified_calendar.json'
try:
    with open(CALENDAR_FILE, 'r') as f:
        calendar_db = json.load(f)
except FileNotFoundError:
    print("❌ Error: verified_calendar.json not found.")
    exit(1)

def is_date_in_past(date_str):
    if date_str in ["TBD", "N/A", "Tied to MGM/Entain"]:
        return False
    try:
        # Example format: "May 6, 2026"
        dt = datetime.strptime(date_str, "%b %d, %Y")
        # Add a 2-day buffer so it doesn't trigger on the exact day of earnings before the PR drops
        return datetime.now() > (dt + timedelta(days=2))
    except ValueError:
        return False

def run_calendar_agent():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("⚠️ Warning: GEMINI_API_KEY missing. Calendar Agent disabled.")
        return

    client = genai.Client(api_key=api_key)
    updates_made = False

    print("🕵️  Initializing AI Calendar Agent...")

    for ticker, data in calendar_db.items():
        old_date = data.get("date", "")
        
        if is_date_in_past(old_date):
            print(f"  📅 {ticker} earnings date ({old_date}) has passed. Activating Agent...")
            
            # Step 1: Fetch latest news to look for next earnings PR
            try:
                ytk = yf.Ticker(ticker)
                news = ytk.news
                headlines = " | ".join([n['title'] for n in news[:5]]) if news else "No recent news available."
            except Exception:
                headlines = "No recent news available."

            # Step 2: Prompt Gemini to analyze or estimate
            prompt = f"""You are an elite financial data engineer. The company with ticker {ticker} recently completed its earnings cycle on {old_date}. 
            Review these recent headlines: {headlines}
            
            Task 1: Did they announce the exact date for their NEXT quarterly earnings?
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
                
                # Sanitize markdown
                raw_text = ai_resp.text.strip()
                raw_text = re.sub(r'^```json\s*', '', raw_text)
                raw_text = re.sub(r'^```\s*', '', raw_text)
                raw_text = re.sub(r'\s*```$', '', raw_text)
                
                new_data = json.loads(raw_text)
                
                # Validate AI output
                if "date" in new_data and "report_time" in new_data:
                    print(f"     ✅ Agent Updated {ticker}: {old_date} -> {new_data['date']}")
                    calendar_db[ticker] = new_data
                    updates_made = True
            except Exception as e:
                print(f"     ❌ Agent failed to parse {ticker}: {e}")

    # Save only if changes occurred
    if updates_made:
        with open(CALENDAR_FILE, 'w') as f:
            json.dump(calendar_db, f, indent=4)
        print("💾 Calendar database successfully rewritten.")
    else:
        print("💤 No calendar rollovers required today.")

if __name__ == "__main__":
    run_calendar_agent()
