import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from O365 import Account
from pyairtable import Api

# --- 1. INITIALIZATION & CONFIG ---
load_dotenv()

# Airtable Setup
airtable_api = Api(os.getenv('AIRTABLE_TOKEN'))
table = airtable_api.table(os.getenv('AIRTABLE_BASE_ID'), os.getenv('AIRTABLE_TABLE_NAME'))

# Microsoft Setup
credentials = (os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))
account = Account(credentials, tenant_id='common')

# --- 2. DATA EXTRACTION (The Slicer) ---
def extract_field(pattern, text):
    """Safely extracts data using Regex patterns."""
    match = re.search(pattern, text)
    return match.group(1).strip() if match else "Not Found"

def parse_lead(raw_text):
    """Converts raw email body text into a structured data dictionary."""
    first_name = extract_field(r"First Name:\s*(.*?)Last Name:", raw_text)
    last_name = extract_field(r"Last Name:\s*(.*?)Mobile Phone:", raw_text)
    phone = extract_field(r"Mobile Phone:\s*(.*?)Email:", raw_text)
    email = extract_field(r"Email:\s*(.*?)Mailing Address", raw_text)
    event_date = extract_field(r"Event Date:\s*(.*?)Start Time:", raw_text)
    details = extract_field(r"Party/Event Details:\s*(.*?)Follow this link", raw_text)
    
    # Clean Headcount (Integer conversion)
    raw_headcount = extract_field(r"Estimated Attendance:\s*(\d+)", raw_text)
    headcount = int(raw_headcount) if raw_headcount != "Not Found" else 0
    
    # Clean Budget (Float conversion)
    raw_budget = extract_field(r"Budget:\s*\$?([\d,]+\.?\d*)", raw_text)
    budget = float(raw_budget.replace(',', '')) if raw_budget != "Not Found" else 0.0

    return {
        "Name": f"{first_name} {last_name}",
        "Email": email,
        "Phone": phone,
        "Date": event_date,
        "Headcount": headcount,
        "Budget": budget,
        "Details": details
    }

# --- 3. SCORING ENGINE (The Brain) ---
def score_lead(lead_data):
    """Calculates priority 1-10 based on qualifications, intent, and effort."""
    score = 0
    budget = lead_data.get("Budget", 0.0)
    hc = lead_data.get("Headcount", 0)
    details = str(lead_data.get("Details", "")).lower()

    # A. Data Completeness (Contact Info)
    if lead_data.get("Phone") != "Not Found": score += 1
    if lead_data.get("Email") != "Not Found": score += 1

    # B. Effort & Keywords (Text Analysis)
    if details and details != "not found":
        # Effort-Based: Word Count
        word_count = len(details.split())
        if word_count > 15: score += 2
        elif word_count >= 5: score += 1

        # Premium Upsell Keywords
        premium = ["private room", "vip", "food package", "cake", "arcade cards", 
                   "laser tag", "7d xperience", "bar package", "buffet", "drink tickets"]
        for word in premium:
            if word in details:
                score += 2
                break

        # Friction / Low-Spend Keywords
        friction = ["bring our own", "outside food", "just racing", "no food", 
                    "discount", "coupon", "waive", "cheap"]
        for word in friction:
            if word in details:
                score -= 2
                break

    # C. Scheduling & Revenue Minimums
    try:
        event_date = datetime.strptime(lead_data["Date"], "%m/%d/%Y")
        today = datetime.today()
        days_until = (event_date - today).days
        is_weekend = event_date.weekday() >= 4 # Fri, Sat, Sun

        # Urgency Boost
        if 0 <= days_until <= 14: score += 4
        elif 15 <= days_until <= 30: score += 2 
            
        # The Gatekeeper: Strict Booking Minimums
        threshold = 400 if is_weekend else 350
        if budget >= threshold:
            score += 3
        elif budget > 0:
            score -= 5
    except:
        pass # Handle date errors silently

    # D. Quality Yield (Spend per Guest)
    if hc > 0 and budget > 0:
        if (budget / hc) >= 45: score += 2

    # Clamp the result between 1 and 10
    return max(min(score, 10), 1)

# --- 4. THE AUTOMATION LOOP (The Daemon) ---
def run_pipeline():
    if not account.is_authenticated:
        print("❌ Auth Error: You must run the token generation script first.")
        return

    mailbox = account.mailbox()
    # Using string-based filter for better library compatibility
    unread_query = "isRead eq false"
    
    print("\n🚀 Andretti Lead Engine Active.")
    print("   Scanning every 30 minutes. Press Ctrl+C to stop.")

    while True:
        current_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{current_time}] 🔍 Checking inbox...")

        try:
            # THIS IS LINE 133 - Make sure it is indented!
            messages = mailbox.get_messages(limit=15, query=unread_query)
            leads_processed = 0

            for msg in messages:
                # Look for the specific notification subject
                if "New Event Lead Notification" in msg.subject:
                    email_uid = msg.object_id 
                    
                    # Deduplication: Check Airtable first
                    if table.all(formula=f"{{Email_UID}}='{email_uid}'"):
                        msg.mark_as_read()
                        continue 
                    
                    # Parse and Score
                    clean_data = parse_lead(msg.get_body_text())
                    priority = score_lead(clean_data)
                    
                    # Load to Airtable
                    table.create({
                        "Name": clean_data["Name"],
                        "Status": "Prospect",
                        "Priority Score": priority,
                        "Estimated Budget": clean_data["Budget"],
                        "Headcount": clean_data["Headcount"],
                        "Event Date": clean_data["Date"],
                        "Email": clean_data["Email"],
                        "Phone": clean_data["Phone"],
                        "Email_UID": email_uid 
                    })
                    
                    print(f"   🎯 MATCH: {clean_data['Name']} | Score: {priority}/10")
                    msg.mark_as_read()
                    leads_processed += 1

            if leads_processed == 0:
                print("   💤 No new leads found.")
            else:
                print(f"   ✅ Successfully added {leads_processed} leads.")

        except Exception as e:
            # This 'except' block MUST exist if there is a 'try'
            print(f"   ⚠️ Encountered an error: {e}")

        # Sleep for 30 minutes (1800 seconds)
        time.sleep(1800)

if __name__ == "__main__":
    run_pipeline()