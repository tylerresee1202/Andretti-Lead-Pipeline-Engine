import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from O365 import Account
from pyairtable import Api

# --- 1. INITIALIZATION ---
load_dotenv()
airtable_api = Api(os.getenv('AIRTABLE_TOKEN'))
table = airtable_api.table(os.getenv('AIRTABLE_BASE_ID'), os.getenv('AIRTABLE_TABLE_NAME'))
credentials = (os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))
account = Account(credentials, tenant_id='common')

def extract_field(pattern, text):
    """Advanced extractor using non-greedy matching and safety cleanups."""
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if match:
        data = match.group(1).strip()
        # Clean up any residual labels if the email format is crushed
        data = re.sub(r'(Last Name:|Phone:|Email:|Questions:|Comments:|Form Page Title:|Start Time:|Mailing Address:).*', '', data, flags=re.IGNORECASE | re.DOTALL).strip()
        return data if data else None
    return None

# --- 2. THE PARSING ENGINES (RESTORED) ---

def parse_format_original(raw_text):
    """Parses 'New Event Lead Notification' with ALL original fields restored."""
    first = extract_field(r"First Name:\s*(.*?)(?=Last Name:|$)", raw_text)
    last = extract_field(r"Last Name:\s*(.*?)(?=Mobile Phone:|$)", raw_text)
    
    # RESTORED: Budget and Headcount Extraction
    raw_hc = extract_field(r"Estimated Attendance:\s*(\d+)", raw_text)
    raw_budget = extract_field(r"Budget:\s*\$?([\d,]+\.?\d*)", raw_text)
    
    return {
        "Name": f"{first or ''} {last or ''}".strip() or "Unknown",
        "Phone": extract_field(r"Mobile Phone:\s*(.*?)(?=Email:|$)", raw_text) or "Not Found",
        "Email": extract_field(r"Email:\s*(.*?)(?=Mailing Address:|$)", raw_text) or "Not Found",
        "Date": extract_field(r"Event Date:\s*(.*?)(?=Start Time:|$)", raw_text),
        "Headcount": int(raw_hc) if raw_hc else 0,
        "Budget": float(raw_budget.replace(',', '')) if raw_budget else 0.0,
        "Details": extract_field(r"Party/Event Details:\s*(.*?)(?=Follow this link|$)", raw_text) or "",
        "Source": "Notification Form"
    }

def parse_format_nso(raw_text):
    """Parses 'Durham NSO Contact Us' strictly for Name, Phone, Email, and Questions."""
    first = extract_field(r"First Name:\s*(.*?)(?=Last Name:|$)", raw_text)
    last = extract_field(r"Last Name:\s*(.*?)(?=Phone:|$)", raw_text)
    
    full_name = f"{first or ''} {last or ''}".strip()
    if not first:
        full_name = extract_field(r"Name:\s*(.*?)(?=Last Name:|Phone:|Email:|$)", raw_text)

    phone = extract_field(r"Phone:\s*(.*?)(?=Email:|$)", raw_text)
    email = extract_field(r"Email:\s*(.*?)(?=Questions:|Comments:|$)", raw_text)
    questions = extract_field(r"(?:Questions|Comments):\s*(.*?)(?=Form Page Title:|$)", raw_text)

    # Note: Budget/Date/Headcount are hardcoded as 0/None because this specific form doesn't need them.
    return {
        "Name": full_name or "Unknown",
        "Phone": phone or "Not Found",
        "Email": email or "Not Found",
        "Date": None,
        "Headcount": 0,
        "Budget": 0.0,
        "Details": questions or "",
        "Source": "NSO Form"
    }

# --- 3. THE SCORING ENGINE ---

def score_lead(lead_data):
    score = 0
    budget = lead_data.get("Budget", 0.0)
    details = lead_data.get("Details", "").lower()
    source = lead_data.get("Source", "Notification Form")

    # A. Contact Completeness
    if lead_data.get("Phone") != "Not Found": score += 1
    if lead_data.get("Email") != "Not Found": score += 1

    # B. Intent & Effort
    if details:
        word_count = len(details.split())
        if word_count > 15: score += 3 
        elif word_count >= 5: score += 1

        premium = ["birthday", "party", "pricing", "grown up", "event", "package", "vip"]
        if any(word in details for word in premium):
            score += 2

    # C. Date Math & Revenue Minimums (Restored for Original Form)
    event_date_str = lead_data.get("Date")
    if event_date_str:
        event_dt = None
        for fmt in ("%m/%d/%Y", "%A, %B %d, %Y"):
            try:
                event_dt = datetime.strptime(event_date_str, fmt)
                break
            except: continue

        if event_dt:
            days_until = (event_dt - datetime.today()).days
            is_weekend = event_dt.weekday() >= 4 
            if 0 <= days_until <= 14: score += 4
            elif 15 <= days_until <= 30: score += 2
            
            if budget > 0:
                threshold = 400 if is_weekend else 350
                if budget >= threshold: score += 3
                else: score -= 5

    # D. NSO "Direct Signal" Boost
    if source == "NSO Form":
        score += 3

    return max(min(score, 10), 1)

# --- 4. THE AUTOMATION LOOP (RESTORED) ---

def run_pipeline():
    if not account.is_authenticated:
        print("❌ Auth Error: Generate your token first."); return

    mailbox = account.mailbox()
    unread_query = "isRead eq false"
    
    print("\n🚀 Andretti Lead Engine Active")
    print("   Data pipelines fully restored for both form types.")

    while True:
        try:
            messages = mailbox.get_messages(limit=15, query=unread_query)
            processed_count = 0

            for msg in messages:
                clean_data = None
                subject = msg.subject
                body = msg.get_body_text()
                
                if "New Event Lead Notification" in subject:
                    print(f"  📩 Processing: Notification Lead")
                    clean_data = parse_format_original(body)
                elif "Durham NSO Contact Us" in subject:
                    print(f"  📩 Processing: NSO Inquiry")
                    clean_data = parse_format_nso(body)

                if clean_data:
                    email_uid = msg.object_id
                    if table.all(formula=f"{{Email_UID}}='{email_uid}'"):
                        msg.mark_as_read(); continue 
                    
                    priority = score_lead(clean_data)
                    
                    # BASE PAYLOAD: Always send these fields
                    payload = {
                        "Name": clean_data["Name"],
                        "Phone": clean_data["Phone"],
                        "Email": clean_data["Email"],
                        "Priority Score": priority,
                        "Status": "Prospect",
                        "Email_UID": email_uid 
                    }
                    
                    # RESTORED PAYLOAD: Only add Date/Budget/Headcount if they exist/belong to the format
                    if clean_data["Date"]:
                        payload["Event Date"] = clean_data["Date"]
                        
                    if clean_data["Source"] == "Notification Form":
                        payload["Headcount"] = clean_data["Headcount"]
                        payload["Estimated Budget"] = clean_data["Budget"]

                    # Teleport to Airtable
                    table.create(payload)
                    
                    print(f"    ✅ ADDED: {clean_data['Name']} (Score: {priority}/10)")
                    msg.mark_as_read()
                    processed_count += 1

            if processed_count == 0:
                print(f"   💤 [{datetime.now().strftime('%H:%M')}] No new leads.")

        except Exception as e:
            print(f"  ⚠️ System Error: {e}")

        time.sleep(1800)

if __name__ == "__main__":
    run_pipeline()