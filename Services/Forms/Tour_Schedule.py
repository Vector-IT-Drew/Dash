from flask import Blueprint, render_template, request, jsonify
import requests
import base64
import pandas as pd
import json
import ast
import time
import numpy as np
from Services.Forms.Get_Gmail_Timeslots import run as get_gmail_timeslots
from datetime import timedelta
import datetime
import pytz
import re
from Services.Functions.Helper import format_dollar, format_phone

from Services.Functions.Monday import get_monday_client, get_board_schema

# Create a Blueprint for tour scheduling
tour_bp = Blueprint('tour', __name__, 
                   template_folder='templates',
                   static_folder='static')

# Function to get available timeslots from Gmail calendar
def get_available_timeslots(date):
	# Add a fake delay to simulate API latency if needed
	# time.sleep(1)
	
	# Get the email address from the request or use a default
	email_address = request.args.get('email', 'it@vectorny.com')
	
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

# Main endpoint to serve the tour scheduling form
@tour_bp.route("/tour-schedule", methods=["GET"])
def tour_schedule():
	email_address = request.args.get('email', 'it@vectorny.com')
	version = request.args.get('version', 'default')
	
	# Return the HTML immediately without fetching timeslots
	return render_template('tour_schedule.html', 
						  email_address=email_address,
						  version=version)

# AJAX endpoint to get timeslots for a specific date
@tour_bp.route("/get-timeslots", methods=["GET"])
def get_timeslots():
	date = request.args.get('date')
	if not date:
		return jsonify({"error": "Date parameter is required"}), 400
	
	# Pass along the email parameter if it exists
	email = request.args.get('email', 'info@vectorproperties.com')
	
	# Get timeslots for the requested date
	timeslots = get_available_timeslots(date)
	return jsonify({"timeslots": timeslots})

# AJAX endpoint to get all available timeslots
@tour_bp.route("/get-all-timeslots", methods=["GET"])
def get_all_timeslots():
	email_address = request.args.get('email', 'it@vectorny.com')
	all_available_slots = get_gmail_timeslots(email_address)
	return jsonify(all_available_slots)

# Add an endpoint to handle form submission
@tour_bp.route("/submit-tour-request", methods=["POST"])
def submit_tour_request():
	# Get form data
	data = request.form.to_dict()
	email_address = data.get('email', 'it@vectorny.com')
	version = str(data.get('version', '1'))

	if version == '2':
		leads_board_id = '8691922085' # Legacy
	else:
		leads_board_id = '8691846626' # Lux

	client = get_monday_client()
	mapping = {val[1]: val[0] for val in get_board_schema(leads_board_id, client)[['id', 'title']].values}

	# Ensure the time-slot includes a date
	if 'time-slot' in data:
		time_slot = data['time-slot']
		if len(time_slot.split()) == 2:  # If only time is provided
			# Prepend today's date to the time
			today_date = datetime.date.today().strftime('%Y-%m-%d')
			time_slot = f"{today_date} at {time_slot}"

		app_date = pd.to_datetime(time_slot, format='%Y-%m-%d at %I:%M %p')
	else:
		# Handle the case where time-slot is missing
		return jsonify({"error": "Time slot is required"}), 400

	column_values = {
		mapping['Email']: {'email': data['email'], 'text': data['email']},
		mapping['Desired Move-In']: {'date': (pd.to_datetime(data['move-in-date'])).strftime('%Y-%m-%d')},
		mapping['Budget']: data['budget'],
		mapping['Phone']: data['phone-number'].replace('-', '').replace('+', ''),
		mapping['Unit Size']: data['apartment-size'],
		mapping['Type']: {'text': data['tour-type'], 'label': data['tour-type']},
		mapping['Appointment Date']: {'date': app_date.strftime('%Y-%m-%d'), 'time': (app_date + datetime.timedelta(hours=4)).strftime('%H:%M:%S')},
	}

	if version == '2':
		column_values[mapping['Address']] = data['inquiry_address']
		column_values[mapping['# Tenants']] = data['tenants']
		column_values[mapping['Qualification Criteria']] = {'label': data['qualification_criteria'], 'text': data['qualification_criteria']}

	print('column_values', column_values)

	# Convert column_values to a properly formatted JSON string
	column_values_json = json.dumps(column_values)

	# Construct and execute the query
	response = client.custom.execute_custom_query(f'''
	mutation {{
	  create_item(
		board_id: {leads_board_id}, 
		item_name: "{data['name']}", 
		column_values: {json.dumps(column_values_json)}
	  ) {{
		id
	  }}
	}}
	''')

	print('response', response)

	service = get_gmail_service(email_address)

	calendar_id = [item for item in service.calendarList().list().execute()['items'] if item['summary'] == 'Vector Tours'][0]['id']

	resp = create_event(service, calendar_id, app_date.strftime('%m/%d/%Y %I:%M%p'), data['name'], data['email'], data['tour-type'], data)
	print(resp)

	return jsonify({"success": True})


from googleapiclient.discovery import build
from google.oauth2 import service_account


def get_gmail_service(email_address):
	SERVICE_ACCOUNT_FILE = 'vector-main-app-6861146695c6.json'
	SCOPES = ["https://www.googleapis.com/auth/calendar"]
	creds = service_account.Credentials.from_service_account_file(
		SERVICE_ACCOUNT_FILE, scopes=SCOPES
	)
	delegated_credentials = creds.with_subject(email_address)
	service = build("calendar", "v3", credentials=delegated_credentials)
	return service

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

