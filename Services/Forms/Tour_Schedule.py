from flask import Blueprint, render_template, request, jsonify
import pandas as pd
import datetime
import pytz
import re
from Services.Functions.Gmail import get_gmail_service, create_event, get_available_slots
from Services.Functions.Monday import get_monday_client, get_board_schema

# Create a Blueprint for tour scheduling
tour_bp = Blueprint('tour', __name__, 
                   template_folder='templates',
                   static_folder='static')


# Main endpoint to serve the tour scheduling form
@tour_bp.route("/tour-schedule", methods=["GET"])
def tour_schedule():
	email_address = request.args.get('email_address', '')
	version = request.args.get('version', 'default')
	
	
	# Return the HTML immediately without fetching timeslots
	return render_template('tour_schedule.html', 
						  email_address=email_address,
						  version=version)

# # AJAX endpoint to get timeslots for a specific date
# @tour_bp.route("/get-timeslots", methods=["GET"])
# def get_timeslots():
# 	date = request.args.get('date')
# 	if not date:
# 		return jsonify({"error": "Date parameter is required"}), 400
	
# 	# Retrieve the portfolio email address
# 	email_address = request.args.get('email_address', 'default@portfolio.com')
	
# 	# Get timeslots for the requested date
# 	timeslots = get_available_timeslots(date)
# 	return jsonify({"timeslots": timeslots})

# AJAX endpoint to get all available timeslots
@tour_bp.route("/get-all-timeslots", methods=["GET"])
def get_all_timeslots():
    # Retrieve the portfolio email address
    email_address = request.args.get('email_address', 'default@portfolio.com')
    all_available_slots = {}
    try:
        service = get_gmail_service(email_address)
        if not service:
            return {"status": "error", "message": "Failed to connect to Gmail service"}

        calendar_list = service.calendarList().list().execute()
        calendar_id = [item for item in calendar_list['items'] if item['summary'] == 'Vector Tours'][0]['id']
        start_date = datetime.date.today()  # Start from today
        
        all_available_slots = get_available_slots(service, calendar_id, start_date)
		
        return jsonify(all_available_slots)

    except Exception as e:
        print('Error in run function:', e)
        return {"status": "error", "message": str(e)}


# Add an endpoint to handle form submission
@tour_bp.route("/submit-tour-request", methods=["POST"])
def submit_tour_request():
	# Get form data
	data = request.form.to_dict()
	# Retrieve the portfolio email address
	email_address = data.get('email_address', '')
	tenant_email = data.get('email', '')

	# Use email_address for calendar operations
	service = get_gmail_service(email_address)

	# Use tenant_email for tenant-specific operations
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
		
		# Check if we're using version 2 (combined datetime) or version 1 (separate date and time)
		if version == '2':
			# Version 2 already sends a formatted datetime string in time-slot
			try:
				# The format from JS is like "2023-05-15 at 10:00 AM"
				app_date = pd.to_datetime(time_slot)
			except:
				try:
					# Try with explicit format if automatic parsing fails
					app_date = pd.to_datetime(time_slot, format='%Y-%m-%d at %I:%M %p')
				except Exception as e:
					print(f"Error parsing datetime for version 2: {e}")
					return jsonify({"error": f"Invalid date/time format: {time_slot}"}), 400
		else:
			# Version 1 sends separate date and time
			selected_date = data.get('date-select', '')
			
			if not selected_date:
				return jsonify({"error": "Date selection is required"}), 400
				
			# Combine date and time
			combined_datetime = f"{selected_date} {time_slot}"
			
			try:
				app_date = pd.to_datetime(combined_datetime)
			except:
				try:
					app_date = pd.to_datetime(combined_datetime, format='%Y-%m-%d %I:%M %p')
				except Exception as e:
					print(f"Error parsing datetime for version 1: {e}")
					return jsonify({"error": f"Invalid date/time format: {combined_datetime}"}), 400
	else:
		# Handle the case where time-slot is missing
		return jsonify({"error": "Time slot is required"}), 400

	column_values = {
		mapping['Email']: {'email': tenant_email, 'text': tenant_email},
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
	column_values_json = column_values

	# Get the first group ID directly from the board data
	board_data = client.boards.fetch_boards_by_id(leads_board_id)
	groups = board_data['data']['boards'][0]['groups']
	group_id = groups[0]['id']  # Use the first group by default
	print('groups', groups)
	
	print(leads_board_id, group_id, data['name'], column_values_json)

	# Use the standard Monday.com Python API to create an item instead of GraphQL
	response = client.items.create_item(
		board_id=leads_board_id,
		group_id=group_id,  # Add the required group_id parameter
		item_name=data['name'],
		column_values=column_values_json
	)

	print('response', response)

	calendar_id = [item for item in service.calendarList().list().execute()['items'] if item['summary'] == 'Vector Tours'][0]['id']

	resp = create_event(service, calendar_id, app_date.strftime('%m/%d/%Y %I:%M%p'), data['name'], tenant_email, data['tour-type'], data)
	print(resp)

	return jsonify({"success": True})

