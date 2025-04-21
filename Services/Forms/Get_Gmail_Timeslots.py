import requests
import base64
import pandas as pd
import json
import ast
import time
import numpy as np
from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
from datetime import timedelta
import pytz
import os


# Function to get busy slots
def get_busy_slots(service, calendar_id, start_date, end_date):
    """Retrieve busy time slots from Google Calendar between specified start and end dates."""
    start_time = pd.Timestamp(datetime.datetime.combine(start_date, datetime.time(9, 0))).isoformat() + "Z"
    end_time = pd.Timestamp(datetime.datetime.combine(end_date, datetime.time(18, 0))).isoformat() + "Z"
    
    events = service.freebusy().query(body={
        "timeMin": start_time,
        "timeMax": end_time,
        "timeZone": "America/New_York",
        "items": [{"id": calendar_id}]
    }).execute()

    busy_slots = events["calendars"].get(calendar_id, {}).get("busy", [])
    return busy_slots

# Function to get available 30-minute slots between 9 AM and 6 PM, excluding busy slots
def get_available_slots(service, calendar_id, start_date, days_ahead=60):
    busy_slots = get_busy_slots(service, calendar_id, start_date, start_date + datetime.timedelta(days=days_ahead))
    
    available_slots = {}
    eastern_tz = pytz.timezone("America/New_York")

    for day_offset in range(days_ahead + 1):
        current_day = start_date + datetime.timedelta(days=day_offset)
        weekday = current_day.weekday()  # Monday = 0, Sunday = 6
        
        # Set time range based on weekday or weekend
        start_hour = 10  # 10 AM for all days
        end_hour = 18 if weekday < 5 else 17  # 6 PM for weekdays, 5 PM for weekends

        current_time = pd.Timestamp(datetime.datetime.combine(current_day, datetime.time(start_hour, 0))).tz_localize(eastern_tz)
        end_time = pd.Timestamp(datetime.datetime.combine(current_day, datetime.time(end_hour, 0))).tz_localize(eastern_tz)

        while current_time <= end_time:
            slot_end = current_time + pd.Timedelta(minutes=30)

            slot = {"start": current_time.isoformat(), "end": slot_end.isoformat()}
            overlap_found = False
            
            for busy in busy_slots:
                busy_start = pd.to_datetime(busy["start"]).astimezone(eastern_tz)
                busy_end = pd.to_datetime(busy["end"]).astimezone(eastern_tz)

                if current_time < busy_end and slot_end > busy_start:
                    overlap_found = True
                    break

            if not overlap_found:
                date_str = current_time.strftime("%Y-%m-%d")
                start_time_str = current_time.strftime("%I:%M %p")

                if date_str not in available_slots:
                    available_slots[date_str] = []
                
                available_slots[date_str].append(start_time_str)

            current_time = slot_end
    
    return available_slots

def get_gmail_service(email_address):
    print('get_gmail_service', email_address)
    
    # Use environment variables instead of service account file
    service_account_info = json.loads(os.environ.get('GOOGLE_CREDS', '{}'))
    
    # Print out each part of the credentials for debugging
    print('Service Account Info:')
    print('Type:', service_account_info.get('type'))
    print('Project ID:', service_account_info.get('project_id'))
    print('Private Key ID:', service_account_info.get('private_key_id'))
    print('Client Email:', service_account_info.get('client_email'))
    print('Client ID:', service_account_info.get('client_id'))
    print('Auth URI:', service_account_info.get('auth_uri'))
    print('Token URI:', service_account_info.get('token_uri'))
    print('Private Key:', service_account_info.get('private_key')[:30] + '...')  # Print only the first part for security
    print('Auth Provider Cert URL:', service_account_info.get('auth_provider_x509_cert_url'))
    print('Client Cert URL:', service_account_info.get('client_x509_cert_url'))
    
    SCOPES = ["https://www.googleapis.com/auth/calendar"]
    
    try:
        # Create credentials from the service account info dictionary
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
        
        # Test the credentials
        print("Testing credentials...")
        if not creds.valid:
            print("Credentials are invalid. Attempting to refresh...")
            creds.refresh(requests.Request())
            if creds.valid:
                print("Credentials refreshed and are now valid.")
            else:
                print("Failed to refresh credentials.")
        
        delegated_credentials = creds.with_subject(email_address)
        service = build("calendar", "v3", credentials=delegated_credentials)
        
        # Test the service connection
        print("Testing service connection...")
        calendar_list = service.calendarList().list().execute()
        print("Service connection successful. Calendar list retrieved.")
        
        return service
    
    except Exception as e:
        print('Error connecting to Gmail service:', e)
        return None


def run(email_address):

    print('run',email_address)

    service = get_gmail_service(email_address)

    try:
        # Fetch the list of calendars
        calendar_list = service.calendarList().list().execute()
        print('Calendar List:', calendar_list)  # Log the calendar list

        # Find the calendar with the summary 'Vector Tours'
        calendar_id = [item for item in calendar_list['items'] if item['summary'] == 'Vector Tours'][0]['id']
    except IndexError:
        print('Error: No Calendar Found with the summary "Vector Tours"')
        return {}
    except Exception as e:
        print('Error: No Calendar Found!', e)
        return {}

    start_date = datetime.date.today()  # Start from today

    available_slots = get_available_slots(service, calendar_id, start_date)

    availableSlots = {}
    for date, slots in available_slots.items():
        availableSlots[date] = slots

    return availableSlots
        