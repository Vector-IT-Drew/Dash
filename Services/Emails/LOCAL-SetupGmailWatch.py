import argparse
import sys
import os

# Make sure the parent directory is in sys.path so you can import your functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from Services.Functions.Gmail import get_gmail_service

# ---- CONFIGURE THESE ----
SERVICE_ACCOUNT_EMAIL = 'sheets-helper@vector-main-app.iam.gserviceaccount.com'
PROJECT_ID = 'vector-main-app'
TOPIC_NAME = f'projects/{PROJECT_ID}/topics/gmail-email-updates'
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def main():
    parser = argparse.ArgumentParser(description='Set up Gmail Pub/Sub watch for a user using existing service connection.')
    parser.add_argument('email', help='The email address to set up the watch for')
    parser.add_argument('--api', default='gmail', help='The Google API to use (default: gmail)')
    args = parser.parse_args()
    email_address = args.email
    api_name = args.api

    # Use your updated function to get the service
    service = get_gmail_service(email_address, api_name=api_name, api_version="v1")
    if not service:
        print(f"Failed to get {api_name} service for {email_address}")
        sys.exit(1)

    if api_name == "gmail":
        request_body = {
            'labelIds': ['INBOX'],
            'topicName': TOPIC_NAME
        }
        print(f"Setting up watch for {email_address}...")
        result = service.users().watch(userId='me', body=request_body).execute()
        print("Watch set up successfully!")
        print("Expiration:", result.get('expiration'))
        print("History ID:", result.get('historyId'))
        print("Full response:", result)
    else:
        print(f"API '{api_name}' is not supported for watch setup in this script.")

if __name__ == '__main__':
    main()