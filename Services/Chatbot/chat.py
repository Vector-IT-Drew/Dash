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
            temp_df = temp_df[temp_df['beds'] >= value]
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
        # Add timeout to prevent hanging requests
        listings_info = requests.get(
            'https://dash-production-b25c.up.railway.app/get_filtered_listings?include_all=True',
            timeout=10  # 10 second timeout
        )
        listings_data = listings_info.json()
        listings = pd.DataFrame(listings_data['data'])

        session['listings_data'] = listings_data
        session['listings_count'] = listings_data['count']
        session.modified = True
    except Exception as e:
        print(f"Error fetching listings: {e}")
       
        session['listings_data'] = {'data': [], 'count': 0}
        session['listings_count'] = 0
        session.modified = True

        return jsonify({"error": f"Haveing trouble fetching listings, please try again later. {e}"}), 500
    
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
    - When a user mentions a budget or maximum price, IMMEDIATELY check if it's within our available range
    - This price range will be updated in future prompts. Use the new values indicated in future prompts, for the logic below.
    - Our minimum price is ${listings['actual_rent'].min()} and maximum is ${listings['actual_rent'].max()} .
    - If they say any value below ${listings['actual_rent'].min()}, tell them:
      "I'm sorry, but that budget is below our minimum available price of ${listings['actual_rent'].min()}. 
       Our apartments range from ${listings['actual_rent'].min()} to ${listings['actual_rent'].max()}. 
       Would you be able to adjust your budget?"
    - NEVER say "Perfect" or "Great" to acknowledge an invalid budget


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
    
    # Check Content-Type header and handle accordingly
    content_type = request.headers.get('Content-Type', '')
    if 'application/json' not in content_type:
        print(f"Warning: Content-Type is not application/json: {content_type}")
        # Try to get data anyway, but return helpful error if it fails
        try:
            data = request.get_json(force=True)  # Force parsing even if Content-Type is wrong
        except Exception as e:
            return jsonify({
                "error": "Invalid Content-Type. Please set Content-Type to 'application/json'",
                "details": f"Received Content-Type: {content_type}"
            }), 415
    else:
        data = request.get_json()
    
    if not data:
        return jsonify({"error": "No JSON data received"}), 400
    
    message = data.get("message", "").strip()
    preferences = data.get('preferences', {})
    
    # Check if session exists - if not, initialize it by calling start_chat
    if 'messages' not in session:
        print("Initializing new session with messages")
        start_response = start_chat()
        
        # Handle the case where start_response is a tuple (response, status_code)
        if isinstance(start_response, tuple):
            start_data = start_response[0].get_json()
        else:
            start_data = start_response.get_json()
        
        session.modified = True

        # Return the full response data, not just the message
        return jsonify(start_data)
    
    else:
        # Session exists, add the user's message to the conversation history
        print("Adding message to existing session")
        session['messages'].append({"role": "user", "content": message})
        session.modified = True

        listings = pd.DataFrame(session['listings_data']['data'])
    
    # Initialize preferences if not already in session
    if 'preferences' not in session:
        print("Initializing new preferences")
        session['preferences'] = {}
        session.modified = True
    
    
    # # Check if the previous preferences had show_listings set to True
    # show_listings_requested = False
    # if 'show_listings' in preferences and preferences['show_listings'] == True:
    #     show_listings_requested = True
    #     # Add a special message to inform the model that listings should be shown
    #     session['messages'].append({"role": "system", "content": "The user has requested to see listings. Respond as if you're showing them the listings."})
    #     session.modified = True
    
    # # Remove the special system message if it was added previously but not needed now
    # if not show_listings_requested:
    #     session['messages'] = [msg for msg in session['messages'] if msg.get("content") != "The user has requested to see listings. Respond as if you're showing them the listings."]
    #     session.modified = True
    
    # Convert conversation history to a string for the preferences extraction prompt
    
    
    
    # Extract preferences using your existing prompt
    convo = '\n'.join([(f"{mes['role']}: {mes['content']}") for mes in session['messages'][1:]])
    prompt = f"""Your goal is to take this conversation history, and extract ONLY the preferences that the user EXPLICITLY mentions in their CURRENT message.

    The user may change their preferences over time, so be sure to set these values using the most up to date conversation sentences.
    Only make assumptions for things like ("5k" should be 5000 , correct misspellings, make sure their selected amenities exist in the current options.)
    
    CRITICAL- ONLY extract preferences that the user has CLEARLY and EXPLICITLY mentioned. DO NOT infer or assume ANY preferences.
    CRITICAL- If the user hasn't mentioned a specific preference, DO NOT include it in the output.
    CRITICAL- If the user hasn't explicitly stated a value for a preference, DO NOT include it.
    CRITICAL- NEVER add default values for preferences the user hasn't mentioned.
    CRITICAL- NEVER assume preferences based on the AI's questions or suggestions.
    CRITICAL- ONLY include preferences that the user has directly and unambiguously requested.
   
    - The AI's last question was: "{session['messages'][-1]['content'] if session['messages'][-1]['role'] == 'assistant' else 'No previous question'}"
    - ONLY extract preferences from the user's MOST RECENT message: "{message}"

    For example:
    - If the user's current message is "I want 2 bedrooms", you should ONLY return {{\"beds\": 2}}
    - If the user's current message is "doorman", you should ONLY return {{\"doorman\": true}}
    - If the user's current message is "I want a doorman and elevator", you should ONLY return {{\"doorman\": true, \"elevator\": true}}

    NEVER include preferences that weren't explicitly mentioned in the user's CURRENT message.

    PRICE VALIDATION - THIS IS EXTREMELY IMPORTANT:
    - If the user specifies a maximum_rent that is below the minimum available price, DO NOT set maximum_rent.
    - If the user specifies a minimum_rent that is above the maximum available price, DO NOT set minimum_rent.
    - For example, if the minimum price in the database is $8000 and the user says "my budget is $7500", DO NOT set maximum_rent=$7500.
    - If the user says "my budget is $7500" but the minimum price is $8000, DO NOT set maximum_rent=$7500.
    - ALWAYS check that the user's maximum_rent is >= the minimum available price in the database.
    - ALWAYS check that the user's minimum_rent is <= the maximum available price in the database.
    
    If a preference does not fit in the criteria, NEVER return an incorrect value (i.e. if they mention a budget below the minimum actual_rent, NEVER set the actual_rent in this case, say their budget is too low, and mention the minimum)
    NEVER assume any values based on the systems question/response, always make sure the user is the one with the final say. 
    Make sure to only set these values as their indicated types. For example, beds must always be a number, and baths must always be a number. 
    If the User gives something else, determine what is the correct value to use, if any.
    
    IMPORTANT: Set "show_listings" to True ONLY if the user is EXPLICITLY asking to see the listings or results.
        Examples of when to set show_listings = True:
        - "Show me the listings"
        - "I want to see the apartments"
        - "Show me what you found"
        - "What options do you have?"
        - "Let me see the results"
        - "Show me what's available"

        Do NOT set show_listings = True for messages like:
        - "2" (just answering a question about bedrooms)
        - "yes" (just confirming something)
        - "doorman" (just mentioning a preference)

        For example:
        - If the user's current message is "I want 2 bedrooms", you should ONLY return {{\"beds\": 2}}
        - If the user's current message is "doorman", you should ONLY return {{\"doorman\": true}}
        - If the user's current message is "I want a doorman and elevator", you should ONLY return {{\"doorman\": true, \"elevator\": true}}
        - If the user's current message is like "show me the listings", you should ONLY return {{\"show_listings\": true}}

        NEVER include preferences that weren't explicitly mentioned in the user's CURRENT message.

    CRITICAL: For boolean features, when a user says "I want X", or similer, set X=True. For example:
    - If user says "I want a doorman" → set doorman=True (NOT building_amenities=["Doorman"])
    - If user says "I need an elevator" → set elevator=True
    - If user says "no pets" → set pet_friendly=False

    The following are BOOLEAN FEATURES that should be set directly, not as amenities:
    - doorman
    - elevator
    - wheelchair_access
    - smoke_free
    - laundry_in_building
    - laundry_in_unit
    - pet_friendly
    - live_in_super
    - concierge
    
    Here is a list of all the possible keys that can be used for a preferences:
        maximum_rent - "This is the max price a person wil pay for their unit" - Number
        minimum_rent - "This is the min price a person wil pay for their unit" - Number
        baths - "How many bathrooms do they want" - Number
        beds - "How many bedrooms do they want" - Number
        borough - "What borough do they want to be in" - String
        building_amenities - "List of all the amenities" - List of Strings (**Options are: {set(item for sublist in listings.building_amenities.apply(
            lambda x: json.loads(x) if isinstance(x, str) else (x if isinstance(x, list) else [])
        ) for item in sublist)})
        countertop_type - String
        dishwasher - Boolean
        doorman - Boolean
        elevator - Boolean
        exposure - "North, South, East, West, Northeast, Southwest, etc."
        floor_num - Number
        floor_type - String
        laundry_in_building - Boolean
        laundry_in_unit - Boolean
        neighborhood - "What neighborhood do they want to be in" - String
        outdoor_space - Boolean
        pet_friendly - Boolean
        smoke_free - Boolean
        sqft - Number
        wheelchair_access - Boolean
        live_in_super - Boolean
        concierge - Boolean
        show_listings - Boolean (set to True ONLY if user explicitly asks to see listings)

    DataBase Info - (Here is an overview of what is currently inside the database.):
        Minimum Beds - {listings['beds'].min()}
        Maximum Beds - {listings['beds'].max()}
        
        Minimum Baths - {listings['baths'].min()}
        Maximum Baths - {listings['baths'].max()}
        
        Minimum Price - {listings['actual_rent'].min()}
        Maximum Price - {listings['actual_rent'].max()}
        
        Applicant Types - {', '.join(sorted(set([val for val in listings.applicance_type.unique() if val and len(val) > 1])))}
        
        Boroughs - {', '.join(sorted(set([val for val in listings.borough.unique() if val and len(val) > 1])))}
        Neighborhoods - {', '.join(sorted(set([val for val in listings.neighborhood.unique() if val and len(val) > 1])))}
        
        Amenities - {', '.join(sorted(set(item for sublist in listings.building_amenities.apply(
            lambda x: json.loads(x) if isinstance(x, str) else (x if isinstance(x, list) else [])
        ) for item in sublist)))}
        
        Exposures - {', '.join(sorted(set([val for val in listings.exposure.apply(lambda x: x.strip() if isinstance(x, str) else '').unique() if val and len(val) > 1])))}

    Previous Convo History:
        {convo}

    Returns:
        Return ONLY a JSON dictionary, with key:value pairs for all the preferences extracted from the convo.
        DO NOT include any preference that would result in zero listings based on the database information.
        IMPORTANT: Include ALL preferences from the entire conversation, not just the most recent message.
"""

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )
    raw_response = response.choices[0].message.content
    print('raw_response', raw_response)
    try:
        # Try to parse as JSON first (handles lowercase true/false)
        try:
            new_preferences = json.loads(raw_response)
        except json.JSONDecodeError:
            # If that fails, try to extract JSON from the response
            match = re.search(r'```(?:json)?\s*({.*?})\s*```', raw_response, re.DOTALL)
            if match:
                new_preferences = json.loads(match.group(1))
            else:
                # If still no JSON found, raise an error
                return jsonify({"message": "Could not extract preferences from response, please try again."}), 400
        
        print(f"Extracted new new_preferences: {new_preferences}")
        
        # Get the previous preferences
        previous_preferences = session.get('preferences', {})

        # Only update preferences that are different from previous ones
        # This helps ensure we're only adding preferences mentioned in this message
        updated_preferences = {}
        for key, value in new_preferences.items():
            # Skip None values
            if value is None:
                continue
            
            # Special handling for list-type preferences like building_amenities
            if isinstance(value, list):
                # Only include non-empty lists
                if value:
                    # For building_amenities, we might want to merge with existing values
                    if key == 'building_amenities' and key in previous_preferences and isinstance(previous_preferences[key], list):
                        # Create a set to avoid duplicates
                        combined_list = set(previous_preferences[key])
                        for item in value:
                            combined_list.add(item)
                        updated_preferences[key] = list(combined_list)
                    else:
                        updated_preferences[key] = value
            # For all other preference types
            elif key not in previous_preferences or previous_preferences[key] != value:
                updated_preferences[key] = value

        # Update session with only the explicitly mentioned preferences
        for key, value in updated_preferences.items():
            session['preferences'][key] = value

        print(f"Updated preferences in session: {session['preferences']}")
        
    except Exception as e:
        print(f"Error parsing preferences: {e}")
        print(f"Raw response: {raw_response}")
        return jsonify({"message": "Could not extract preferences from response, please try again."}), 400
   
    # After filtering listings and removing invalid preferences
    filtered_listings, valid_preferences = filter_listings_by_preferences(listings, session['preferences'])

    # Check if any preferences were removed during filtering
    removed_preferences = {k: session['preferences'][k] for k in session['preferences'] if k not in valid_preferences and k != 'listing_count' and k != 'show_listings'}

    print('Valid Preferences', valid_preferences)
    # If preferences were removed, add a system message to inform the model
    if removed_preferences:
        removed_prefs_msg = f"The user requested {', '.join([f'{k}={v}' for k, v in removed_preferences.items()])}, but these preferences resulted in zero available listings. Please inform the user that these options are not available and suggest alternatives based on the current database information."
        session['messages'].append({"role": "system", "content": removed_prefs_msg})
        session.modified = True

    # Preserve the show_listings flag if it was set
    if 'show_listings' in session['preferences'] and session['preferences']['show_listings'] == True:
        valid_preferences['show_listings'] = True
        session['messages'].append({"role": "system", "content": "The user has requested to see listings. Respond as if you're showing them the listings."})

    # Update the session with valid preferences
    session['preferences'] = valid_preferences
    
    # Update the prompt with filtered listings information
    update_prompt = f"""
        DataBase Info - (Here is an overview of what is currently inside the database.):
            Minimum Beds - {filtered_listings['beds'].min() if not filtered_listings.empty else 'N/A'}
            Maximum Beds - {filtered_listings['beds'].max() if not filtered_listings.empty else 'N/A'}

            Minimum Baths - {filtered_listings['baths'].min() if not filtered_listings.empty else 'N/A'}
            Maximum Baths - {filtered_listings['baths'].max() if not filtered_listings.empty else 'N/A'}

            Minimum Price - {filtered_listings['actual_rent'].min() if not filtered_listings.empty else 'N/A'}
            Maximum Price - {filtered_listings['actual_rent'].max() if not filtered_listings.empty else 'N/A'}

            Applicant Types - {', '.join(sorted(set([val for val in filtered_listings.applicance_type.unique() if val and len(val) > 1]))) if not filtered_listings.empty else 'N/A'}

            Boroughs - {', '.join(sorted(set([val for val in filtered_listings.borough.unique() if val and len(val) > 1]))) if not filtered_listings.empty else 'N/A'}
            Neighborhoods - {', '.join(sorted(set([val for val in filtered_listings.neighborhood.unique() if val and len(val) > 1]))) if not filtered_listings.empty else 'N/A'}

            Amenities - {', '.join(sorted(set(item for sublist in filtered_listings.building_amenities.apply(
                lambda x: json.loads(x) if isinstance(x, str) else (x if isinstance(x, list) else [])
            ) for item in sublist))) if not filtered_listings.empty else 'N/A'}

            Exposures - {', '.join(sorted(set([val for val in filtered_listings.exposure.apply(lambda x: x.strip() if isinstance(x, str) else '').unique() if val and len(val) > 1]))) if not filtered_listings.empty else 'N/A'}
            
            # Boolean Features
            Doorman - {all(filtered_listings['doorman']) if not filtered_listings.empty and 'doorman' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['doorman']) if not filtered_listings.empty and 'doorman' in filtered_listings.columns else 'N/A'} (Some)
            Elevator - {all(filtered_listings['elevator']) if not filtered_listings.empty and 'elevator' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['elevator']) if not filtered_listings.empty and 'elevator' in filtered_listings.columns else 'N/A'} (Some)
            Wheelchair Access - {all(filtered_listings['wheelchair_access']) if not filtered_listings.empty and 'wheelchair_access' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['wheelchair_access']) if not filtered_listings.empty and 'wheelchair_access' in filtered_listings.columns else 'N/A'} (Some)
            Smoke Free - {all(filtered_listings['smoke_free']) if not filtered_listings.empty and 'smoke_free' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['smoke_free']) if not filtered_listings.empty and 'smoke_free' in filtered_listings.columns else 'N/A'} (Some)
            Laundry in Building - {all(filtered_listings['laundry_in_building']) if not filtered_listings.empty and 'laundry_in_building' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['laundry_in_building']) if not filtered_listings.empty and 'laundry_in_building' in filtered_listings.columns else 'N/A'} (Some)
            Laundry in Unit - {all(filtered_listings['laundry_in_unit']) if not filtered_listings.empty and 'laundry_in_unit' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['laundry_in_unit']) if not filtered_listings.empty and 'laundry_in_unit' in filtered_listings.columns else 'N/A'} (Some)
            Pet Friendly - {all(filtered_listings['pet_friendly']) if not filtered_listings.empty and 'pet_friendly' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['pet_friendly']) if not filtered_listings.empty and 'pet_friendly' in filtered_listings.columns else 'N/A'} (Some)
            Live-in Super - {all(filtered_listings['live_in_super']) if not filtered_listings.empty and 'live_in_super' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['live_in_super']) if not filtered_listings.empty and 'live_in_super' in filtered_listings.columns else 'N/A'} (Some)
            Concierge - {all(filtered_listings['concierge']) if not filtered_listings.empty and 'concierge' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['concierge']) if not filtered_listings.empty and 'concierge' in filtered_listings.columns else 'N/A'} (Some)
            
            Number of listings: {len(filtered_listings) if not filtered_listings.empty else 0}"""
    
    session['messages'].append({"role": "system", "content": f"Updated DataBase Info based on current preferences. {update_prompt}"})
    
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=session['messages']
    )
    response_text = response.choices[0].message.content
    print(f"🤖: {response_text}")
    
    session['messages'].append({"role": "assistant", "content": response_text})
   
    session['preferences']['listing_count'] = len(filtered_listings)
    
    # Prepare the response data
    response_data = {
        "message": response_text,
        "preferences": session['preferences'],
        "listing_count": len(filtered_listings)
    }

    if len(filtered_listings) <= 5:
        session['preferences']['show_listings'] = True

    if 'show_listings' in session['preferences'] and session['preferences']['show_listings'] == True:
        top_listings = filtered_listings.head(5).to_dict('records') if not filtered_listings.empty else []
        response_data["listings"] = top_listings

        session['preferences']['show_listings'] = False

    return jsonify(response_data)
