import os
import re
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from O365 import Account
from pyairtable import Api

# --- 1. INITIALIZATION & CONFIGURATION ---

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("AndrettiLeadEngine")

airtable_api = Api(os.getenv('AIRTABLE_TOKEN'))
table = airtable_api.table(os.getenv('AIRTABLE_BASE_ID'), os.getenv('AIRTABLE_TABLE_NAME'))
credentials = (os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))
account = Account(credentials, tenant_id='common')

# --- 2. DATA NORMALIZATION HELPERS ---

def extract_field(pattern: str, text: str) -> Optional[str]:
    """Advanced extractor using non-greedy matching and safety cleanups."""
    if not text: return None
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if match:
        data = match.group(1).strip()
        data = re.sub(r'(Last Name:|Phone:|Email:|Questions:|Comments:|Form Page Title:|Start Time:|Mailing Address:).*', '', data, flags=re.IGNORECASE | re.DOTALL).strip()
        return data if data else None
    return None

def isolate_email(text: str) -> str:
    """Specifically hunts for a valid email address with strict boundaries."""
    if not text: return "Not Found"
    match = re.search(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', text)
    return match.group(0).lower() if match else "Not Found"

def normalize_phone(raw_phone: Optional[str]) -> str:
    """Forces messy phone inputs into a clean (XXX) YYY-ZZZZ format."""
    if not raw_phone or raw_phone.lower() == "not found":
        return "Not Found"
    
    # Strip everything except digits
    digits = re.sub(r'\D', '', raw_phone)
    
    # Drop leading '1' if they typed the country code
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
        
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        
    return raw_phone # Return raw if it's an unusual length (e.g. international)

# --- 3. THE PARSING ENGINES ---

def parse_format_original(raw_text: str) -> Dict[str, Any]:
    first = extract_field(r"First Name:\s*(.*?)(?=Last Name:|$)", raw_text)
    last = extract_field(r"Last Name:\s*(.*?)(?=Mobile Phone:|$)", raw_text)
    
    raw_hc = extract_field(r"Estimated Attendance:\s*(\d+)", raw_text)
    raw_budget = extract_field(r"Budget:\s*\$?([\d,]+\.?\d*)", raw_text)
    raw_phone = extract_field(r"Mobile Phone:\s*(.*?)(?=Email:|$)", raw_text)
    raw_email = extract_field(r"Email:\s*(.*?)(?=Mailing Address:|$)", raw_text)
    
    budget_val = 0.0
    if raw_budget:
        clean_budget = re.sub(r'[^\d.]', '', raw_budget)
        try: budget_val = float(clean_budget)
        except ValueError: logger.warning(f"Failed to parse budget: {raw_budget}")

    return {
        "Name": f"{first or ''} {last or ''}".strip().title() or "Unknown", # NORMALIZED: Title Case
        "Phone": normalize_phone(raw_phone), # NORMALIZED: Standard Format
        "Email": isolate_email(raw_email),
        "Date": extract_field(r"Event Date:\s*(.*?)(?=Start Time:|$)", raw_text),
        "Headcount": int(raw_hc) if raw_hc else 0,
        "Budget": budget_val,
        "Details": extract_field(r"Party/Event Details:\s*(.*?)(?=Follow this link|$)", raw_text) or "",
        "Source": "Notification Form"
    }

def parse_format_nso(raw_text: str) -> Dict[str, Any]:
    first = extract_field(r"First Name:\s*(.*?)(?=Last Name:|$)", raw_text)
    last = extract_field(r"Last Name:\s*(.*?)(?=Phone:|$)", raw_text)
    
    full_name = f"{first or ''} {last or ''}".strip()
    if not first:
        full_name = extract_field(r"Name:\s*(.*?)(?=Last Name:|Phone:|Email:|$)", raw_text)

    raw_phone = extract_field(r"Phone:\s*(.*?)(?=Email:|$)", raw_text)
    raw_email = extract_field(r"Email:\s*(.*?)(?=Questions:|Comments:|$)", raw_text)
    questions = extract_field(r"(?:Questions|Comments):\s*(.*?)(?=Form Page Title:|$)", raw_text)

    return {
        "Name": full_name.title() if full_name else "Unknown", # NORMALIZED
        "Phone": normalize_phone(raw_phone), # NORMALIZED
        "Email": isolate_email(raw_email),
        "Date": None,
        "Headcount": 0,
        "Budget": 0.0,
        "Details": questions or "",
        "Source": "NSO Form"
    }

SUBJECT_MAPPING = {
    "New Event Lead Notification": parse_format_original,
    "Durham NSO Contact Us": parse_format_nso,
}

# --- 4. THE PURE ADDITIVE SCORING ENGINE (Tiered NLP) ---

def score_lead(lead_data: Dict[str, Any]) -> int:
    score = 1 # Base score. We only go UP from here based on value and intent.
    
    budget = lead_data.get("Budget", 0.0)
    headcount = lead_data.get("Headcount", 0)
    details = lead_data.get("Details", "").lower()
    source = lead_data.get("Source", "Notification Form")

    # A. Intent & Effort (Tiered NLP Scoring)
    if details:
        # Word count effort
        if len(details.split()) > 15: score += 1 
        
        # Tier 1: Facility & Attraction Knowledge (+1)
        facility = ["karting", "laser tag", "bowling", "arcade", "7d", "hologate", "hyperdeck", "simulator", "track", "andretti"]
        if any(word in details for word in facility):
            score += 1

        # Tier 2: General Event & Birthday Focus (+2)
        events = ["birthday", "bday", "party", "celebration", "corporate", "team", "company", "buyout", "vip"]
        if any(word in details for word in events):
            score += 2

        # Tier 3: Specific Package Intent - The Big Boost (+3)
        packages = ["apex", "full-throttle", "full throttle", "starter", "turbo", "nitro", "sprint", "speedway", "twin turbo", "signature social"]
        if any(word in details for word in packages):
            score += 3

    # B. Date Math: The "Forgiving Month"
    event_date_str = lead_data.get("Date")
    if event_date_str:
        event_dt = None
        for fmt in ("%m/%d/%Y", "%A, %B %d, %Y"):
            try:
                event_dt = datetime.strptime(event_date_str, fmt)
                break
            except ValueError: continue

        if event_dt:
            days_until = (event_dt - datetime.today()).days
            if 0 <= days_until <= 14: score += 4      # Urgent!
            elif 15 <= days_until <= 30: score += 3   # Hot month
            elif 31 <= days_until <= 60: score += 1   # Planning ahead

    # C. Headcount & Budget (Additive Only)
    if headcount >= 20: score += 2
    elif headcount > 0: score += 1
    
    if budget >= 400: score += 3
    elif budget > 0: score += 1 # They gave a budget, even if small, showing intent

    # D. NSO "Direct Signal" Boost
    if source == "NSO Form":
        score += 2

    return max(min(score, 10), 1) # Keep cleanly bounded between 1 and 10

# --- 5. THE AUTOMATION LOOP ---

def run_pipeline():
    if not account.is_authenticated:
        logger.error("Auth Error: Generate your token first.")
        return

    mailbox = account.mailbox()
    unread_query = "isRead eq false"
    
    logger.info("🚀 Andretti Lead Engine Active")
    logger.info("Processing New Leads...")

    while True:
        try:
            messages = mailbox.get_messages(limit=50, query=unread_query)
            processed_count = 0

            for msg in messages:
                subject = msg.subject
                body = msg.get_body_text()
                clean_data = None
                
                # Dynamically route the email to the correct parser based on subject
                for key, parser_func in SUBJECT_MAPPING.items():
                    if key in subject:
                        logger.info(f"📩 Processing: {key}")
                        clean_data = parser_func(body)
                        break

                if clean_data:
                    email_uid = msg.object_id
                    
                    # Deduplication check
                    if table.first(formula=f"{{Email_UID}}='{email_uid}'"):
                        logger.info(f"Duplicate found for UID {email_uid}. Marking as read.")
                        msg.mark_as_read()
                        continue 
                    
                    priority = score_lead(clean_data)
                    inquiry_date = msg.received.strftime("%Y-%m-%d")
                    
                    # Core Payload
                    payload = {
                        "Name": clean_data["Name"],
                        "Phone": clean_data["Phone"],
                        "Email": clean_data["Email"],
                        "Priority Score": priority,
                        "Status": "New Inquiry",
                        "Inquiry Date": inquiry_date, 
                        "Email_UID": email_uid 
                    }
                    
                    # Conditional Payload additions
                    if clean_data["Date"]:
                        payload["Event Date"] = clean_data["Date"]
                        
                    if clean_data["Source"] == "Notification Form":
                        payload["Headcount"] = clean_data["Headcount"]
                        payload["Estimated Budget"] = clean_data["Budget"]

                    # Push to Airtable
                    table.create(payload)
                    logger.info(f"✅ ADDED: {clean_data['Name']} (Score: {priority}/10)")
                    
                    msg.mark_as_read()
                    processed_count += 1
                    time.sleep(0.5) # API pacing

            if processed_count == 0:
                logger.info("💤 No new leads.")

        except Exception as e:
            logger.error(f"⚠️ System Error: {e}")

        # Sleep before checking the inbox again
        time.sleep(1800)

if __name__ == "__main__":
    run_pipeline()