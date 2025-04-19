import os
import mysql.connector
from flask import Blueprint, Flask, jsonify, request, session
import logging
from mysql.connector import Error
import decimal
from datetime import datetime
from Services.Database.Connect import get_db_connection
from Services.Database.Listings import get_filtered_listings_data
from openai import OpenAI
import json
import pandas as pd
from dotenv import load_dotenv
import requests
import ast
import re

# Get the absolute path to the root directory (2 folders up)
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
dotenv_path = os.path.join(root_dir, '.env')

# Load environment variables from .env file with explicit path
load_dotenv(dotenv_path)

# Create a Blueprint instead of a Flask app
chat_bp = Blueprint('Chatbot', __name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

# After extracting preferences, filter the listings
def filter_listings_by_preferences(listings_df, preferences):
    """
    Filter listings based on user preferences
    
    Args:
        listings_df (pd.DataFrame): DataFrame of listings
        preferences (dict): User preferences
    
    Returns:
        pd.DataFrame: Filtered listings, valid preferences
    """
    filtered_df = listings_df.copy()
    valid_preferences = {}  # Start with empty dict instead of copying to avoid carrying over unwanted preferences
    
    if filtered_df.empty:
        return filtered_df, valid_preferences
    
    # Only copy over preferences that were explicitly provided by the user
    for key in preferences:
        if key in preferences and preferences[key] is not None and key != 'listing_count' and key != 'show_listings':
            valid_preferences[key] = preferences[key]
    
    # Special handling for show_listings and listing_count
    if 'show_listings' in preferences:
        valid_preferences['show_listings'] = preferences['show_listings']
    
    if 'listing_count' in preferences:
        valid_preferences['listing_count'] = preferences['listing_count']
    
    # Pre-validate preferences to remove any that would result in zero listings
    if 'maximum_rent' in valid_preferences and valid_preferences['maximum_rent'] is not None:
        if 'actual_rent' in filtered_df.columns:
            min_available_rent = filtered_df['actual_rent'].min()
            if valid_preferences['maximum_rent'] < min_available_rent:
                print(f"Ignoring invalid maximum_rent: {valid_preferences['maximum_rent']} (minimum available is {min_available_rent})")
                del valid_preferences['maximum_rent']
    
    # Process building_amenities column if needed
    print('filtered_df.columns', filtered_df.columns)
    if 'building_amenities' in filtered_df.columns:
        # Check if building_amenities is a string and convert to list if needed
        if len(filtered_df) > 0 and isinstance(filtered_df['building_amenities'].iloc[0], str):
            filtered_df['building_amenities'] = filtered_df['building_amenities'].apply(
                lambda x: json.loads(x) if isinstance(x, str) and x.strip() else []
            )
    
    # Filter by each preference one by one and check if it results in zero listings
    for key, value in list(valid_preferences.items()):  # Use list() to allow modification during iteration
        if value is None or value == [] or key == 'listing_count' or key == 'show_listings':
            continue
        
        # Apply the filter based on the preference type
        temp_df = filtered_df.copy()

        
        # Filter logic for different preference types
        if key == 'maximum_rent' and 'actual_rent' in temp_df.columns:
            temp_df = temp_df[temp_df['actual_rent'] <= value]
        elif key == 'minimum_rent' and 'actual_rent' in temp_df.columns:
            temp_df = temp_df[temp_df['actual_rent'] >= value]
        elif key == 'beds' and 'beds' in temp_df.columns:
            temp_df = temp_df[temp_df['beds'] == value]
        elif key == 'baths' and 'baths' in temp_df.columns:
            temp_df = temp_df[temp_df['baths'] >= value]
        elif key == 'borough' and 'borough' in temp_df.columns:
            temp_df = temp_df[temp_df['borough'].str.lower() == value.lower()]
        elif key == 'neighborhood' and 'neighborhood' in temp_df.columns:
            temp_df = temp_df[temp_df['neighborhood'].str.lower() == value.lower()]
        elif key == 'building_amenities' and isinstance(value, list) and len(value) > 0 and 'building_amenities' in temp_df.columns:
            # Filter by each amenity
            for amenity in value:
                temp_df = temp_df[temp_df['building_amenities'].apply(lambda x: amenity in x if isinstance(x, list) else False)]
        # Add filters for boolean features
        elif key in ['doorman', 'elevator', 'wheelchair_access', 'smoke_free', 
                    'laundry_in_building', 'laundry_in_unit', 'pet_friendly', 
                    'live_in_super', 'concierge'] and key in temp_df.columns:
            temp_df = temp_df[temp_df[key] == value]
        
        # Check if the filter resulted in zero listings
        if len(temp_df) == 0:
            print(f"Removing preference {key}={value} as it results in zero listings")
            # Don't include this preference in valid_preferences
            if key in valid_preferences:
                del valid_preferences[key]
        else:
            # Apply the filter to the main dataframe
            filtered_df = temp_df
    
    # CRITICAL: Ensure we only return preferences that were in the original request
    # This prevents adding any preferences that weren't explicitly requested
    final_preferences = {}
    for key in preferences:
        if key in valid_preferences:
            final_preferences[key] = valid_preferences[key]
    
    return filtered_df, final_preferences


@chat_bp.route("/start-chat", methods=["POST"])
def start_chat():
    print('Start chat')
    
    # Clear any existing session data
    session.clear()
    
    # Get listings data only once
    try:
        
        # This assumes get_filtered_listings_data returns the data directly, not a Response object
        listings_data = get_filtered_listings_data(include_all=True, direct_response=True)

        print('Got listings data', len(listings_data))
        
        # Check if we got valid data
        if isinstance(listings_data, dict) and 'data' in listings_data and 'count' in listings_data:
            listings = pd.DataFrame(listings_data['data'])
            
            session['listings_data'] = listings_data
            session['listings_count'] = listings_data['count']
            session.modified = True
        else:
            print('Invalid data format returned from get_filtered_listings_data')
            raise ValueError("Invalid data format returned from get_filtered_listings_data")
            
    except Exception as e:
        print(f"Error fetching listings: {e}")
       
        # Set empty defaults
        session['listings_data'] = {'data': [], 'count': 0}
        session['listings_count'] = 0
        session.modified = True

        return jsonify({"error": f"Having trouble fetching listings, please try again later. {e}"}), 500
    
    system_prompt = f"""
    You are Vector Assistant, a helpful and friendly real estate agent chatbot for Vector Properties in NYC. 
    Your primary goal is to help users find apartments that match their preferences, based on an internal database. 
    
    First, try to get a CORE PREFERENCE. If they don't have a preference, move on. 
    Only ask one question at a time, moving down the list. 
    
    Then continue going through the other options, and asking the user their preferences. Only ask them for one preference at a time.
    
    # REQUIRED CORE PREFERENCES:
    - beds: Number
    - baths: Number
    - maximum_rent: Number
    - borough: String
    - neighborhood: String

    # LOCATION EXTRACTION INSTRUCTIONS:
    - For general areas like "Manhattan" or "Brooklyn", set the borough field.
    - For specific neighborhoods like "Chelsea" or "Williamsburg", set both borough and neighborhood.
    - If the user gives a misspelled location (like "manhatten"), correct it to the proper borough.

    # OPTIONAL PROPERTY DETAILS:
    - sqft: Number
    - doorman: Boolean
    - elevator: Boolean
    - gym: Boolean
    - laundry: Boolean
    - pets_allowed: Boolean
    - amenities: [list of all preferred amenities]
    - minimum_rent: Number

     # CRITICAL VALIDATION INSTRUCTIONS:
    - NEVER acknowledge invalid preferences as acceptable
    - If a user requests a value outside our available options (like a budget below our minimum price), you MUST:
      1. Immediately inform them that their request doesn't match our available options
      2. Tell them the actual range/options available
      3. Ask them to adjust their preference
    - For example, if they say "my budget is $7,500" but our minimum price is $8,000, say:
      "I'm sorry, but $7,500 is below our minimum available price of $8,000. Our apartments range from $8,000 to $X. Would you be able to adjust your budget?"
    - Similarly, if they request 1 bedroom but we only have 2+ bedroom apartments, tell them what options are available
    - NEVER pretend to accept an invalid preference just to be polite
    - ALWAYS check if a user's preference is valid before acknowledging it

    
    # HANDLING SINGLE OPTIONS:
    - If there's only one option available for a preference (e.g., only one borough or only one bathroom option), 
      DON'T ask the user about it. Instead, inform them: "Based on your other preferences, we only have listings in [value]."
    - For example, if all remaining listings have 2 bathrooms, say: "Based on your other preferences, all available listings have 2 bathrooms."
    - Then immediately move on to the next preference question.
    - IMPORTANT: Check the DataBase Info section carefully before asking about any preference. If there's only one value listed for a field (e.g., only "Manhattan" in Boroughs), don't ask about it.
    - For Boolean features, if all listings have the same value (e.g., all have doorman=True), don't ask about it.
    
    # HANDLING AMENITY REQUESTS:
    - If the user requests an amenity, FIRST check if it exists in the unique amenities list from the database.
    - If the amenity exists, add it to the preferences.
    - If the amenity does NOT exist, inform the user that no current listings have this, then:
        - Suggest a similar available amenity if applicable.
        - If no similar options exist, politely move on to the next preference.

    # HANDLING GENERAL RESPONSES:
    - If the user asks for examples or suggestions (e.g., "what are some options?"), tell them how many listings match their criteria but NEVER list specific properties.
    - If the user gives a vague response (e.g., "someplace quiet"), ask clarifying questions about what "quiet" means to them. Do they want a low-crime neighborhood? A residential area with less foot traffic?
    - If the user refuses to provide details and just says "I don't know," move on to the next preference without insisting.
    - If the user says "no" or indicates they have no more preferences, NEVER end the conversation - instead, proceed to ask about the next preference.
    
    # CRITICAL RULES ABOUT LISTING PROPERTIES:
    - NEVER list or describe specific properties, addresses, or unit numbers. Just use the DataBase Info below to show what options are currently available.
    - NEVER use numbered lists to present property options. NEVER present property options.
    - NEVER say things like "Here are some listings that match your criteria"
    - The data below shows all the unique values that we currently have for available listings. This will get updated as the conversation goes on, and the listings narrow down. 
    - If the user ever has a preference that does not exist in the available options listed in "DataBase Info" below, let them know, and let them know the available options/alternatives. 
    - Never say anything like "Based on our data"
    - Never say "Would you like me to look for..." or "I Found listings..." Your whole purpose is to keep asking questions about their preferences, you will never show them the results. 
    - Pay special attention to phrases like 'remove X', 'no longer want X', 'cancel X', 'reset X', 'don't need X', etc. - these indicate preferences to remove
    - If the user says something like 'just search in [borough]', this means to remove any neighborhood restriction
    - When the user changes a preference (e.g., 'increase rent to 9k'), update the value accordingly
    - Make sure to only mark user preferences if you are sure the users response matches the preference
    - Only extract preferences that the user EXPLICITLY states. DO NOT extract characteristics of apartments that the chatbot happens to mention
    - DO NOT PRESERVE previous preferences that weren't explicitly mentioned by the user in the current conversation.
    - AMENITY DETECTION: Pay special attention to phrases like 'with X', 'has X', 'includes X', etc. - check if X matches any available amenity and add it to the building_amenities array.
    - For example, if user says '2 bed in bronx with game room', and 'Game Room' is in the available amenities list, add 'Game Room' to the building_amenities array.
    - CASE MATTERS: Make sure to match the EXACT case of amenities as they appear in the available amenities list. For example, use 'Bike Storage' not 'bike storage'.
    - TYPO HANDLING: Be aware of common typos like 'wiht' instead of 'with'. Don't interpret these as amenities or preferences.
    - Only add items to building_amenities if they clearly match known amenities in the database.

    # CRITICAL BOOLEAN FEATURES VS AMENITIES
    - The following are BOOLEAN FEATURES and should be set as separate boolean preferences (True/False), NOT added to building_amenities:
      - doorman
      - elevator
      - wheelchair_access
      - smoke_free
      - laundry_in_building
      - laundry_in_unit
      - pet_friendly
      - live_in_super
      - concierge
    - For example, if the user says "I want a doorman", set doorman=True, NOT building_amenities=["Doorman"]
    - Only add items to building_amenities if they match the specific amenities list from the database

    # HANDLING SHOW LISTINGS REQUESTS:
    - If the user explicitly asks to see listings with phrases like "show me the listings", "I want to see the apartments", etc., respond with:
      "Great! I'll show you the available listings that match your criteria. Here they are!"
    - NEVER say you can't show listings or that you're just gathering information - the system will handle showing the actual listings.
    - When the user asks to see listings, your job is to acknowledge their request positively and indicate that listings are being shown.

    # PRICE VALIDATION - EXTREMELY IMPORTANT:
    - If the user specifies a maximum_rent that is below the minimum available price, DO NOT set maximum_rent.
    - If the user specifies a minimum_rent that is above the maximum available price, DO NOT set minimum_rent.
    - For example, if the minimum price in the database is $8000 and the user says "my budget is $7500", DO NOT set maximum_rent=$7500.
    - If the user says "my budget is $7500" but the minimum price is $8000, DO NOT set maximum_rent=$7500.
    - ALWAYS check that the user's maximum_rent is >= the minimum available price in the database.
    - ALWAYS check that the user's minimum_rent is <= the maximum available price in the database.
    - IMPORTANT: If the user's maximum_rent is ABOVE the maximum available price, that's GOOD! In this case, DO set maximum_rent to the user's specified value.
    - For example, if the maximum price in the database is $9000 and the user says "my budget is $10000", DO set maximum_rent=$10000.
    - A high budget is never a problem - only a budget that's too low is problematic.

    HIGH PRIORITY- CRITICAL INSTRUCTION ABOUT SHOW_LISTINGS:
    - Set "show_listings" to True ONLY if the user is EXPLICITLY and DIRECTLY asking to see listings.
    - The user must use clear phrases like "show", "see", "view", "available", "options", "results", or "listings" to trigger this.
    - NEVER set show_listings=True when the user is:
      * Just answering a question (like "1" or "2" for bedrooms)
      * Just stating a preference (like "I want a doorman")
      * Just confirming something (like "yes" or "sounds good")
      * Just providing information (like "my budget is 5k")

    Examples where show_listings should be TRUE:
    - "Show me the listings"
    - "I want to see the apartments now"
    - "Let me see what you have"
    - "What options are available?"
    - "Show me the results"

    Examples where show_listings should be FALSE:
    - "1 bedroom please" (just stating a preference)
    - "1 plz" (just answering about bedrooms)
    - "yes" (just confirming)
    - "I want a doorman" (just stating a preference)
    - "my budget is 5k" (just providing information)
    - "Upper East Side" (just stating a location)

    IMPORTANT: The user must EXPLICITLY ask to SEE listings. If they don't use words like "show", "see", "view", "available", "options", "results", or "listings", then DO NOT set show_listings=True.

    DataBase Info - (Here is an overview of what is currently inside the database.):
        Minimum Beds - {listings['beds'].min()}
        Maximum Beds - {listings['beds'].max()}
        
        Minimum Baths - {listings['baths'].min()}
        Maximum Baths - {listings['baths'].max()}
        
        Minimum Price - {listings['actual_rent'].min()}
        Maximum Price - {listings['actual_rent'].max()}
                
        Boroughs - {', '.join(sorted(set([val for val in listings.borough.unique() if val and len(val) > 1])))}
        Neighborhoods - {', '.join(sorted(set([val for val in listings.neighborhood.unique() if val and len(val) > 1])))}
        
        Amenities - {', '.join(sorted(set(item for sublist in listings.building_amenities.apply(
            lambda x: json.loads(x) if isinstance(x, str) else (x if isinstance(x, list) else [])
        ) for item in sublist)))}
        
        Exposures - {', '.join(sorted(set([val for val in listings.exposure.apply(lambda x: x.strip()).unique() if val and len(val) > 1])))}
        
        # Boolean Features
        Doorman - {all(listings['doorman']) if 'doorman' in listings.columns else 'N/A'} (All), {any(listings['doorman']) if 'doorman' in listings.columns else 'N/A'} (Some)
        Elevator - {all(listings['elevator']) if 'elevator' in listings.columns else 'N/A'} (All), {any(listings['elevator']) if 'elevator' in listings.columns else 'N/A'} (Some)
        Wheelchair Access - {all(listings['wheelchair_access']) if 'wheelchair_access' in listings.columns else 'N/A'} (All), {any(listings['wheelchair_access']) if 'wheelchair_access' in listings.columns else 'N/A'} (Some)
        Smoke Free - {all(listings['smoke_free']) if 'smoke_free' in listings.columns else 'N/A'} (All), {any(listings['smoke_free']) if 'smoke_free' in listings.columns else 'N/A'} (Some)
        Laundry in Building - {all(listings['laundry_in_building']) if 'laundry_in_building' in listings.columns else 'N/A'} (All), {any(listings['laundry_in_building']) if 'laundry_in_building' in listings.columns else 'N/A'} (Some)
        Laundry in Unit - {all(listings['laundry_in_unit']) if 'laundry_in_unit' in listings.columns else 'N/A'} (All), {any(listings['laundry_in_unit']) if 'laundry_in_unit' in listings.columns else 'N/A'} (Some)
        Pet Friendly - {all(listings['pet_friendly']) if 'pet_friendly' in listings.columns else 'N/A'} (All), {any(listings['pet_friendly']) if 'pet_friendly' in listings.columns else 'N/A'} (Some)
        Live-in Super - {all(listings['live_in_super']) if 'live_in_super' in listings.columns else 'N/A'} (All), {any(listings['live_in_super']) if 'live_in_super' in listings.columns else 'N/A'} (Some)
        Concierge - {all(listings['concierge']) if 'concierge' in listings.columns else 'N/A'} (All), {any(listings['concierge']) if 'concierge' in listings.columns else 'N/A'} (Some)

"""
    
    session['messages'] = [
        {"role": "system", "content": system_prompt}
    ]
    session.modified = True  # Mark the session as modified
    
    welcome_message = (
        "Hi there! I'm Vector Assistant, your personal NYC apartment hunting guide. Let's start with the basics - how many bedrooms are you looking for in your new apartment?"
    )
    
    session['messages'].append({"role": "assistant", "content": welcome_message})
    session['preferences'] = {}
    
    return jsonify({"message": welcome_message})

@chat_bp.route("/chat", methods=["GET", "POST"])
def chat():
    # Detailed request logging
    print("=" * 50)
    print(f"CHAT ENDPOINT ACCESSED")
    print(f"Request method: {request.method}")
    print(f"Request path: {request.path}")
    print(f"Request URL: {request.url}")
    print(f"Request headers: {dict(request.headers)}")
    print(f"Request data: {request.get_data(as_text=True)}")
    print(f"Request JSON: {request.get_json(silent=True)}")
    print("=" * 50)

    if request.method != 'POST':
        return jsonify({"error": f"Please use POST method to send chat messages. You used: {request.method}"}), 405
    
    # Get data from request
    try:
        data = request.get_json(force=True)
    except Exception as e:
        return jsonify({
            "error": "Invalid Content-Type. Please set Content-Type to 'application/json'",
            "details": f"Received Content-Type: {request.headers.get('Content-Type', '')}"
        }), 415
    
    if not data:
        return jsonify({"error": "No JSON data received"}), 400
    
    message = data.get("message", "").strip()
    preferences = data.get('preferences', {})
    
    # Check if session exists - if not, initialize it by calling start_chat
    if 'messages' not in session:
        print("Initializing new session with messages")
        
        try:
            # Get listings data
            listings_data = get_filtered_listings_data(include_all=True, direct_response=True)
            
            if isinstance(listings_data, dict) and 'data' in listings_data and 'count' in listings_data:
                listings = pd.DataFrame(listings_data['data'])
                
                session['listings_data'] = listings_data
                session['listings_count'] = listings_data['count']
            else:
                # Set empty defaults if data format is invalid
                session['listings_data'] = {'data': [], 'count': 0}
                session['listings_count'] = 0
                
            # Initialize system prompt with listings data
            system_prompt = f"""
            You are Vector Assistant, a helpful and friendly real estate agent chatbot for Vector Properties in NYC. 
            Your primary goal is to help users find apartments that match their preferences, based on an internal database.
            
            # ... rest of your system prompt ...
            """
            
            # Initialize session messages
            session['messages'] = [
                {"role": "system", "content": system_prompt}
            ]
            
            # Initialize preferences
            session['preferences'] = {}
            
            # Add welcome message
            welcome_message = (
                "Hi there! I'm Vector Assistant, your personal NYC apartment hunting guide. Let's start with the basics - how many bedrooms are you looking for in your new apartment?"
            )
            
            session['messages'].append({"role": "assistant", "content": welcome_message})
            session.modified = True
            
        except Exception as e:
            print(f"Error initializing session: {e}")
            return jsonify({"error": f"Having trouble starting the chat, please try again later. {e}"}), 500
    
    # Now continue with the rest of your chat function...
    # Add user message to history
    session['messages'].append({"role": "user", "content": message})
    
    # Process the rest of your chat logic...

@chat_bp.route("/reset_chat", methods=["POST"])
def reset_chat():
    """
    Reset the chat session and start a new conversation.
    This endpoint clears all session data and calls start_chat to initialize a new session.
    """
    print("=" * 50)
    print(f"RESET CHAT ENDPOINT ACCESSED")
    print(f"Request method: {request.method}")
    print(f"Request path: {request.path}")
    print(f"Request URL: {request.url}")
    print(f"Request headers: {dict(request.headers)}")
    print("=" * 50)
    
    # Clear the entire session
    session.clear()
    
    # Call start_chat to initialize a new session
    response = start_chat()
    
    # Log the reset action
    print("Chat session has been reset and reinitialized")
    
    # Return the same response that start_chat would return
    return response
