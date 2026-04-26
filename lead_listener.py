import os
import re
from datetime import datetime
from dotenv import load_dotenv
from O365 import Account
from pyairtable import Api

load_dotenv()

# --- 1. CONNECT TO AIRTABLE ---
airtable_api = Api(os.getenv('AIRTABLE_TOKEN'))
table = airtable_api.table(os.getenv('AIRTABLE_BASE_ID'), os.getenv('AIRTABLE_TABLE_NAME'))

# --- 2. THE SLICER & SCORER ---
def extract_field(pattern, text):
    match = re.search(pattern, text)
    return match.group(1).strip() if match else "Not Found"

def parse_lead(raw_text):
    first_name = extract_field(r"First Name:\s*(.*?)Last Name:", raw_text)
    last_name = extract_field(r"Last Name:\s*(.*?)Mobile Phone:", raw_text)
    phone = extract_field(r"Mobile Phone:\s*(.*?)Email:", raw_text)
    email = extract_field(r"Email:\s*(.*?)Mailing Address", raw_text)
    event_date = extract_field(r"Event Date:\s*(.*?)Start Time:", raw_text)
    
    raw_headcount = extract_field(r"Estimated Attendance:\s*(\d+)", raw_text)
    headcount = int(raw_headcount) if raw_headcount != "Not Found" else 0
    
    raw_budget = extract_field(r"Budget:\s*\$?([\d,]+\.?\d*)", raw_text)
    budget = float(raw_budget.replace(',', '')) if raw_budget != "Not Found" else 0.0

    return {
        "Name": f"{first_name} {last_name}",
        "Email": email,
        "Phone": phone,
        "Date": event_date,
        "Headcount": headcount,
        "Budget": budget
    }

def score_lead(lead_data):
    score = 0
    budget = lead_data["Budget"]
    hc = lead_data["Headcount"]
    if budget >= 1000: score += 4
    if hc >= 20: score += 3
    try:
        event_date = datetime.strptime(lead_data["Date"], "%m/%d/%Y")
        if event_date.weekday() >= 4: score += 3 # Weekend bonus
    except: pass
    return max(min(score, 10), 1)

# --- 3. THE LISTENER LOOP ---
credentials = (os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))
account = Account(credentials, tenant_id='common')

if account.is_authenticated:
    print("\n🔍 Scanning Buffer Inbox...")
    mailbox = account.mailbox()
    # Check the 10 most recent emails
    messages = mailbox.get_messages(limit=10)
    
    processed_count = 0
    
    for msg in messages:
        if "New Event Lead Notification" in msg.subject:
            # DUPLICATE GUARD: Check if this email ID is already in Airtable
            email_uid = msg.object_id 
            formula = f"{{Email_UID}}='{email_uid}'"
            if table.all(formula=formula):
                continue # Skip if we already logged this lead
            
            # Extract and Score
            clean_data = parse_lead(msg.get_body_text())
            priority = score_lead(clean_data)
            
            # MATCHING THE AIRTABLE COLUMNS
            new_record = {
                "Name": clean_data["Name"],
                "Status": "Prospect",
                "Priority Score": priority,
                "Estimated Budget": clean_data["Budget"],
                "Headcount": clean_data["Headcount"],
                "Event Date": clean_data["Date"],
                "Email": clean_data["Email"],
                "Phone": clean_data["Phone"],
                "Email_UID": email_uid # Keeps the table clean
            }
            
            try:
                table.create(new_record)
                print(f"🚀 Teleported Lead: {clean_data['Name']} | Score: {priority}/10")
                processed_count += 1
            except Exception as e:
                print(f"❌ Airtable Error: {e}")
                print("Tip: Check that your Airtable column names match the code exactly!")

    print(f"\nDone. {processed_count} new leads added to your Discovery Pen.")
else:
    print("❌ Connection Error. Run auth_test.py.")