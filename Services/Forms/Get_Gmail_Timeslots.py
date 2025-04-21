import requests
import base64
import pandas as pd
import json
import ast
import time
import numpy as np
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.auth.transport.requests import Request
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
    print('Busy Slots:', busy_slots)
    available_slots = {}
    eastern_tz = pytz.timezone("America/New_York")
    
    # Pre-process busy slots into a more efficient format
    busy_by_date = {}
    for busy in busy_slots:
        busy_start = pd.to_datetime(busy["start"]).astimezone(eastern_tz)
        busy_end = pd.to_datetime(busy["end"]).astimezone(eastern_tz)
        
        # Handle busy slots that span multiple days
        current = busy_start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = busy_end.replace(hour=0, minute=0, second=0, microsecond=0)
        
        while current <= end_day:
            date_str = current.strftime("%Y-%m-%d")
            if date_str not in busy_by_date:
                busy_by_date[date_str] = []
                
            day_start = max(busy_start, current)
            day_end = min(busy_end, current + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
            
            if day_start < day_end:  # Only add if there's actual overlap with this day
                busy_by_date[date_str].append((day_start, day_end))
            
            current += pd.Timedelta(days=1)

    for day_offset in range(days_ahead + 1):
        current_day = start_date + datetime.timedelta(days=day_offset)
        weekday = current_day.weekday()  # Monday = 0, Sunday = 6
        date_str = current_day.strftime("%Y-%m-%d")
        
        # Set time range based on weekday or weekend
        start_hour = 10  # 10 AM for all days
        end_hour = 18 if weekday < 5 else 17  # 6 PM for weekdays, 5 PM for weekends

        # Generate all possible 30-minute slots for the day
        day_slots = []
        current_time = pd.Timestamp(datetime.datetime.combine(current_day, datetime.time(start_hour, 0))).tz_localize(eastern_tz)
        end_time = pd.Timestamp(datetime.datetime.combine(current_day, datetime.time(end_hour, 0))).tz_localize(eastern_tz)
        
        while current_time < end_time:
            slot_end = current_time + pd.Timedelta(minutes=30)
            day_slots.append((current_time, slot_end))
            current_time = slot_end
        
        # Remove busy slots efficiently
        if date_str in busy_by_date:
            busy_periods = busy_by_date[date_str]
            available_day_slots = []
            
            for slot_start, slot_end in day_slots:
                is_available = True
                for busy_start, busy_end in busy_periods:
                    if slot_start < busy_end and slot_end > busy_start:
                        is_available = False
                        break
                
                if is_available:
                    available_day_slots.append(slot_start)
        else:
            # If no busy slots for this day, all slots are available
            available_day_slots = [slot[0] for slot in day_slots]
        
        # Format and add available slots to the result
        if available_day_slots:
            available_slots[date_str] = [slot.strftime("%I:%M %p") for slot in available_day_slots]
    
    return available_slots

def get_gmail_service(email_address):
    print('get_gmail_service', email_address)
    
    # Construct service account info from environment variables
    service_account_info = {
        "type": "service_account",
        "project_id":  "vector-main-app",
        "private_key_id": os.environ.get('GOOGLE_PRIVATE_KEY_ID'),
        "private_key": os.environ.get('GOOGLE_PRIVATE_KEY').replace('\\n', '\n'),
        "client_email": "sheets-helper@vector-main-app.iam.gserviceaccount.com",
        "client_id": "117359487626577840585",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
         "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/sheets-helper%40vector-main-app.iam.gserviceaccount.com"
    }

    # Print out each part of the credentials for debugging
    print('Service Account Info:', service_account_info)
    
    SCOPES = ["https://www.googleapis.com/auth/calendar"]
    
    try:
        # Create credentials from the service account info dictionary
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
     
        delegated_credentials = creds.with_subject(email_address)
        service = build("calendar", "v3", credentials=delegated_credentials)
      
        return service
    
    except Exception as e:
        print('Error connecting to Gmail service:', e)
        return None


def run(email_address):
    print('run', email_address)

    try:
        service = get_gmail_service(email_address)
        if not service:
            print('Failed to get Gmail service')
            return {"status": "error", "message": "Failed to connect to Gmail service"}

        calendar_list = service.calendarList().list().execute()
        print('Calendar List:', calendar_list)  # Log the calendar list

        # Find the calendar with the summary 'Vector Tours'
        calendar_id = [item for item in calendar_list['items'] if item['summary'] == 'Vector Tours'][0]['id']
        print('Found Calendar ID:', calendar_id)

        start_date = datetime.date.today()  # Start from today
        available_slots = get_available_slots(service, calendar_id, start_date)
        print('Available Slots:', available_slots)

        availableSlots = {}
        for date, slots in available_slots.items():
            availableSlots[date] = slots

        return availableSlots

    except Exception as e:
        print('Error in run function:', e)
        return {"status": "error", "message": str(e)}
        