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
from Services.Functions.Helper import format_dollar, format_phone


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


def create_event(service, calendar_id, slot, tenant_name, tenant_email, tour_type, data):
	"""Create an event in Google Calendar using a selected time slot."""
	tz = pytz.timezone("America/New_York")
	start_time = datetime.datetime.strptime(slot, "%m/%d/%Y %I:%M%p")
	start_time = tz.localize(start_time)
	end_time = start_time + datetime.timedelta(minutes=30)
	start_time_str = start_time.isoformat()
	end_time_str = end_time.isoformat()

	event = {
		"summary": f"{tour_type} Apartment Tour - {tenant_name} - {data['apartment-size'].replace('-', ' ').title()}",
		"description": f"""Tour scheduled with {tenant_name}.
                        Desired Move In: {pd.to_datetime(data['move-in-date']).strftime('%m/%d/%Y')}
                        Desired Budget: {format_dollar(float(data['budget']))}
                        Contact Info: {tenant_email} {format_phone(data['phone-number'].replace('-','').replace('+', ''))}
                                                """,
		"start": {"dateTime": start_time_str, "timeZone": "America/New_York"},
		"end": {"dateTime": end_time_str, "timeZone": "America/New_York"},
		"attendees": [{"email": tenant_email}],
		"reminders": {
			"useDefault": False,
			"overrides": [
				{"method": "email", "minutes": 1440},
				{"method": "popup", "minutes": 30}
			],
		},
	}

	event = service.events().insert(calendarId=calendar_id, body=event).execute()
	print(f"✅ Event created: {event.get('htmlLink')}")
	return event

# Function to get available timeslots from Gmail calendar
def get_available_timeslots(date):
	# Add a fake delay to simulate API latency if needed
	# time.sleep(1)
	
	# Get the email address from the request or use a default
	email_address = request.args.get('email_address', '')
	

	# Get all available slots from Gmail
	all_available_slots = get_gmail_timeslots(email_address)
	
	# If the requested date exists in available slots, return those slots
	if date in all_available_slots:
		# Convert to the format expected by the frontend
		formatted_slots = [{"time": time_str, "available": True} for time_str in all_available_slots[date]]
		return formatted_slots
	else:
		# Return empty list if no slots available for this date
		return []

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
