import os
from dotenv import load_dotenv
from O365 import Account

load_dotenv()

credentials = (os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))
account = Account(credentials, tenant_id='common')

if not account.is_authenticated:
    print("\n--- THE FINAL HANDSHAKE ---")
    # Using the built-in authenticator to protect the memory state
    try:
        if account.authenticate(scopes=['https://graph.microsoft.com/Mail.Read'], redirect_uri='http://localhost:8080'):
            print("\n✅ SUCCESS! Token file created.")
        else:
            print("\n❌ FAILED to create token.")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if account.is_authenticated:
    print("\n✅ Connection is live.")
    mailbox = account.mailbox()
    for msg in mailbox.get_messages(limit=1):
        print(f"Verified! Most recent email subject: {msg.subject}")