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
	
	SERVICE_ACCOUNT_FILE = 'vector-main-app-6861146695c6.json'

	SCOPES = ["https://www.googleapis.com/auth/calendar"]
	creds = service_account.Credentials.from_service_account_file(
	    SERVICE_ACCOUNT_FILE, scopes=SCOPES
	)

	delegated_credentials = creds.with_subject(email_address)

	service = build("calendar", "v3", credentials=delegated_credentials)

	return service


def run(email_address):
	
	service = get_gmail_service(email_address)

	try:
		calendar_id = calendar_id = [item for item in service.calendarList().list().execute()['items'] if item['summary'] == 'Vector Tours'][0]['id']
	except Exception as e:
		print('Error: No Calendar Found!', e)

	start_date = datetime.date.today()  # Start from today

	available_slots = get_available_slots(service, calendar_id, start_date)

	availableSlots = {}
	for date, slots in available_slots.items():
	    availableSlots[date] = slots

	return availableSlots
		