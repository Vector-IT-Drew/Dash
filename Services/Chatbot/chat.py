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

# Load environment variables from .env file
load_dotenv()

# Create a Blueprint instead of a Flask app
chat_bp = Blueprint('Chatbot', __name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize OpenAI client
try:
    api_key = os.getenv("OPENAI_API_KEY")
    api_key = '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'
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is not set")
    
    # Initialize OpenAI client with error handling
    client = OpenAI(api_key=api_key)
    
    # Test the client with a minimal request to verify the key works
    test_response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "system", "content": "Test"}],
        max_tokens=5
    )
    print("OpenAI client initialized successfully")
except Exception as e:
    print(f"ERROR initializing OpenAI client: {e}")
    # Create a fallback client that will raise clear errors when used
    client = None

# After extracting preferences, filter the listings
def filter_listings_by_preferences(listings_df, preferences):
    """
    Filter listings based on user preferences
    
    Args:
        listings_df (pd.DataFrame): DataFrame of listings
        preferences (dict): User preferences
    
    Returns:
        pd.DataFrame: Filtered listings
    """
    filtered_df = listings_df.copy()
    valid_preferences = preferences.copy()
    
    if filtered_df.empty:
        return filtered_df
    
    # Pre-validate preferences to remove any that would result in zero listings
    if 'maximum_rent' in valid_preferences and valid_preferences['maximum_rent'] is not None:
        if 'actual_rent' in filtered_df.columns:
            min_available_rent = filtered_df['actual_rent'].min()
            if valid_preferences['maximum_rent'] < min_available_rent:
                print(f"Ignoring invalid maximum_rent: {valid_preferences['maximum_rent']} (minimum available is {min_available_rent})")
                del valid_preferences['maximum_rent']
    
    # Process building_amenities column if needed
    if 'building_amenities' in filtered_df.columns:
        # Check if building_amenities is a string and convert to list if needed
        if isinstance(filtered_df['building_amenities'].iloc[0], str):
            filtered_df['building_amenities'] = filtered_df['building_amenities'].apply(
                lambda x: json.loads(x) if isinstance(x, str) and x.strip() else []
            )
    
    # Filter by each preference one by one and check if it results in zero listings
    for key, value in list(valid_preferences.items()):  # Use list() to allow modification during iteration
        if value is None or value == [] or key == 'listing_count' or key == 'show_listings':
            continue
        
        temp_df = filtered_df.copy()
        
        # Handle special cases
        if key == 'maximum_rent' and value is not None:
            if 'actual_rent' in temp_df.columns:
                temp_df = temp_df[temp_df['actual_rent'] <= value]
        
        elif key == 'minimum_rent' and value is not None:
            if 'actual_rent' in temp_df.columns:
                temp_df = temp_df[temp_df['actual_rent'] >= value]
        
        elif key == 'building_amenities' and isinstance(value, list) and len(value) > 0:
            if 'building_amenities' in temp_df.columns:
                # For each amenity in preferences, filter listings that have it
                for amenity in value:
                    amenity_df = temp_df[temp_df['building_amenities'].apply(
                        lambda x: any(amenity.lower() in a.lower() for a in x) if isinstance(x, list) else False
                    )]
                    # Only apply filter if it doesn't result in zero listings
                    if not amenity_df.empty:
                        temp_df = amenity_df
                    else:
                        print(f"Warning: No listings found with amenity '{amenity}'")
                        # Remove this amenity from the list
                        valid_preferences['building_amenities'].remove(amenity)
        
        # Direct column matches
        elif key in temp_df.columns:
            if key == 'beds':
                # For beds, allow for a range within 0.5 of the requested value
                temp_df = temp_df[(temp_df[key] >= value - 0.5) & (temp_df[key] <= value + 0.5)]
            
            elif key == 'baths':
                # For baths, allow for a range within 0.5+ of the requested value
                temp_df = temp_df[temp_df[key] >= value - 0.5]
            
            elif key == 'borough' or key == 'neighborhood':
                # Case-insensitive string comparison for location
                temp_df = temp_df[temp_df[key].str.lower() == value.lower()]
            
            elif key == 'exposure' and value is not None:
                # For exposure, check if the user's preference is contained within the listing's exposure
                temp_df = temp_df[temp_df[key].str.lower().str.contains(value.lower())]
            
            elif key == 'sqft' and value is not None:
                # For sqft, filter for minimum value
                temp_df = temp_df[temp_df[key] >= value]
            
            # Boolean preferences
            elif key in ['doorman', 'elevator', 'gym', 'laundry', 'pets_allowed', 
                         'laundry_in_unit', 'laundry_in_building', 'outdoor_space', 
                         'wheelchair_access', 'smoke_free', 'pet_friendly', 
                         'live_in_super', 'concierge']:
                temp_df = temp_df[temp_df[key] == value]
        
        # Check if this preference resulted in zero listings
        if temp_df.empty:
            print(f"Removing preference {key}={value} as it results in zero listings")
            del valid_preferences[key]
        else:
            # Apply the filter
            filtered_df = temp_df
    
    return filtered_df, valid_preferences



@chat_bp.route("/start-chat", methods=["POST"])
def start_chat():
    # Get listings data only once
    try:
        # Add timeout to prevent hanging requests
        listings_info = requests.get(
            'https://dash-production-b25c.up.railway.app/get_filtered_listings?include_all=True',
            timeout=10  # 10 second timeout
        )
        listings_data = listings_info.json()
        listings = pd.DataFrame(listings_data['data'])
        
        # Store listings in the session for future use
        session['listings_data'] = listings_data
        session['listings_count'] = listings_data['count']
        session.modified = True
    except Exception as e:
        print(f"Error fetching listings: {e}")
        # Fallback to empty DataFrame if fetch fails
        listings = pd.DataFrame()
        session['listings_data'] = {'data': [], 'count': 0}
        session['listings_count'] = 0
        session.modified = True
    
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
        
    Notes:
        Always Start the chat with: "Hi there! I'm Vector Assistant, your personal NYC apartment hunting guide. 
                                    Let's start with the basics - how many bedrooms are you looking for in your new apartment?"

"""
    
    
    session['messages'] = [
        {"role": "system", "content": system_prompt}
    ]
    session.modified = True  # Mark the session as modified
    
    welcome_message = (
        "Hi there! I'm Vector Assistant, your personal NYC apartment hunting guide. "
        "Let's start with the basics - how many bedrooms are you looking for in your new apartment?"
    )
    
    session['messages'].append({"role": "assistant", "content": welcome_message})
    session['preferences'] = {}
    
    return jsonify({"message": welcome_message})

@chat_bp.route("/chat", methods=["POST", "GET"])
def chat():
    # Log the caller information
    print(f"Chat endpoint called by: {request.remote_addr}")
    print(f"User agent: {request.headers.get('User-Agent')}")
    print(f"Referrer: {request.headers.get('Referer')}")
    
    if request.method == "GET":
        return jsonify({"message": "Please use POST method to send chat messages"})
    
    print('Hello@')
    
    # Get the user's message from the request
    follow_up = request.json.get("message", "").strip()
    
    # Debug session info
    print(f"Session ID: {session.sid if hasattr(session, 'sid') else 'No session ID'}")
    print(f"Session contains: {dict(session)}")
    
    # Get listings data only once and store in session if not already there
    if 'listings_data' not in session:
        try:
            # Add timeout to prevent hanging requests
            listings_info = requests.get(
                'https://dash-production-b25c.up.railway.app/get_filtered_listings?include_all=True',
                timeout=10  # 10 second timeout
            )
            listings_data = listings_info.json()
            listings = pd.DataFrame(listings_data['data'])
            
            # Store listings in the session for future use
            session['listings_data'] = listings_data
            session['listings_count'] = listings_data['count']
            session.modified = True
            print(f"Fetched fresh listings data: {listings_data['count']} listings")
        except Exception as e:
            print(f"Error fetching listings: {e}")
            # Fallback to empty DataFrame if fetch fails
            listings = pd.DataFrame()
            session['listings_data'] = {'data': [], 'count': 0}
            session['listings_count'] = 0
            session.modified = True
    else:
        # Use listings data from session
        listings_data = session['listings_data']
        listings = pd.DataFrame(listings_data['data'])
        print(f"Using cached listings data: {listings_data['count']} listings")
    
    # Check if session exists - if not, initialize it by calling start_chat
    if 'messages' not in session:
        print("Initializing new session with messages")
        # Call start_chat to initialize the session
        start_response = start_chat()
        start_data = start_response.get_json()
        
        # If this is the first message, add it to the newly created session
        if follow_up:
            # Add the user's message to the conversation history
            session['messages'].append({"role": "user", "content": follow_up})
            session.modified = True
            
            # Continue with the regular chat flow below
        else:
            # If no message was provided, just return the welcome message
            return start_response
    else:
        # Session exists, add the user's message to the conversation history
        print("Adding message to existing session")
        session['messages'].append({"role": "user", "content": follow_up})
        session.modified = True
    
    # Initialize preferences if not already in session
    if 'preferences' not in session:
        print("Initializing new preferences")
        session['preferences'] = {}
        session.modified = True
    
    # Get current preferences from session
    current_preferences = session.get('preferences', {})
    print(f"Current preferences from session: {current_preferences}")
    
    # Check if the previous preferences had show_listings set to True
    show_listings_requested = False
    if 'show_listings' in current_preferences and current_preferences['show_listings'] == True:
        show_listings_requested = True
        # Add a special message to inform the model that listings should be shown
        session['messages'].append({"role": "system", "content": "The user has requested to see listings. Respond as if you're showing them the listings."})
        session.modified = True
    
    # Remove the special system message if it was added previously but not needed now
    if not show_listings_requested:
        session['messages'] = [msg for msg in session['messages'] if msg.get("content") != "The user has requested to see listings. Respond as if you're showing them the listings."]
        session.modified = True
    
    # Convert conversation history to a string for the preferences extraction prompt
    convo = '\n'.join([(f"{mes['role']}: {mes['content']}") for mes in session['messages'][1:]])
    
    # Extract preferences using your existing prompt
    prompt = f"""Your goal is to take this conversation history, and extract all the users preferences. 

        The user may change their preferences over time, so be sure to set these values using the most up to date conversation sentences.
        Only make assumptions for things like ("5k" should be 5000 , correct misspellings, make sure their selected amenities exist in the current options.)
        
        CRITICAL: NEVER add a preference value that doesn't exist in the current database options.
        - If the user requests a value that would result in zero listings, DO NOT include it in the preferences.
        - For example, if they ask for 1 bathroom but the database only has 1.5 or 2 bathrooms, DO NOT set baths=1.
        - Instead, leave that preference unset so the AI can explain the available options to the user.
        - Always check the "DataBase Info" section to see what values are currently available.
        
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
        
        IMPORTANT: Set "show_listings" to True ONLY if the user is explicitly asking to see the listings or results.
        Examples of when to set show_listings = True:
        - "Show me the listings"
        - "I want to see the apartments"
        - "Show me what you found"
        - "What options do you have?"
        - "Let me see the results"
        - "Show me what's available"
        
        Do NOT set show_listings = True if the user is still discussing preferences or asking questions about neighborhoods, amenities, etc.
        
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
    
    # Print token usage for preferences extraction
    pref_prompt_tokens = response.usage.prompt_tokens
    pref_completion_tokens = response.usage.completion_tokens
    pref_total_tokens = response.usage.total_tokens
    print(f"\n--- Preferences Extraction Token Usage ---")
    print(f"Prompt tokens: {pref_prompt_tokens}")
    print(f"Completion tokens: {pref_completion_tokens}")
    print(f"Total tokens: {pref_total_tokens}")
    print(f"Estimated cost: ${(pref_prompt_tokens * 0.00003) + (pref_completion_tokens * 0.00006):.4f}")
    print(f"-------------------------------------------\n")

    raw_response = response.choices[0].message.content
    
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
                raise ValueError("Could not extract JSON from response")
        
        print(f"Extracted new preferences: {new_preferences}")
        
        # Special handling for building_amenities to accumulate them
        if 'building_amenities' in new_preferences and isinstance(new_preferences['building_amenities'], list):
            # If we already have amenities, add the new ones
            if 'building_amenities' in current_preferences and isinstance(current_preferences['building_amenities'], list):
                # Create a set to avoid duplicates
                existing_amenities = set(current_preferences['building_amenities'])
                for amenity in new_preferences['building_amenities']:
                    existing_amenities.add(amenity)
                new_preferences['building_amenities'] = list(existing_amenities)
        
        # Update the current preferences with the new ones
        current_preferences.update(new_preferences)
        
        # Save the updated preferences to the session
        session['preferences'] = current_preferences
        session.modified = True
        print(f"Updated preferences in session: {session['preferences']}")
        
    except Exception as e:
        print(f"Error parsing preferences: {e}")
        print(f"Raw response: {raw_response}")
    
    # Filter listings based on preferences
    filtered_listings, valid_preferences = filter_listings_by_preferences(listings, current_preferences)
    
    # Update the session with valid preferences
    session['preferences'] = valid_preferences
    session.modified = True
    print(f"Valid preferences after filtering: {session['preferences']}")
    
    # Print the valid preferences
    print(valid_preferences)
    print(f"Number of listings matching preferences: {len(filtered_listings)}")
    
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
    session.modified = True
    
    # Get response from the model
    response = client.chat.completions.create(
        model="gpt-4",
        messages=session['messages']
    )
    
    # Print token usage
    prompt_tokens = response.usage.prompt_tokens
    completion_tokens = response.usage.completion_tokens
    total_tokens = response.usage.total_tokens
    print(f"\n--- Token Usage ---")
    print(f"Prompt tokens: {prompt_tokens}")
    print(f"Completion tokens: {completion_tokens}")
    print(f"Total tokens: {total_tokens}")
    print(f"Estimated cost: ${(prompt_tokens * 0.00003) + (completion_tokens * 0.00006):.4f}")
    print(f"-------------------\n")
    
    # Extract response text
    response_text = response.choices[0].message.content
    print(f"🤖: {response_text}")
    
    # Add the assistant's response to the conversation history
    session['messages'].append({"role": "assistant", "content": response_text})
    session.modified = True
    
    # Add the number of listings to the preferences
    session['preferences']['listing_count'] = len(filtered_listings)
    session.modified = True
    
    # Return the response as JSON
    response_data = {
        "message": response_text,
        "preferences": session['preferences'],
        "listing_count": len(filtered_listings)
    }
    
    return jsonify(response_data)
