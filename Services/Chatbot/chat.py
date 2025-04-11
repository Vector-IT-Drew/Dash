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

# Load environment variables from .env file
load_dotenv()

# Create a Blueprint instead of a Flask app
chat_bp = Blueprint('Chatbot', __name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def filter_listings(listings_data, filters):
    """
    Filter listings based on user preferences
    
    Args:
        listings_data (dict): The data returned from get_filtered_listings_data
        filters (dict): User preferences to filter by
    
    Returns:
        pd.DataFrame: Filtered listings as a DataFrame
    """
    # Convert the listings data to a DataFrame
    if isinstance(listings_data, dict) and 'data' in listings_data:
        listings_df = pd.DataFrame(listings_data['data'])
    else:
        listings_df = pd.DataFrame(listings_data)
    
    if listings_df.empty:
        return listings_df
        
    filtered_df = listings_df.copy()
    print('filtered_df columns:', filtered_df.columns)
    
    # Handle empty dataframe case
    if filtered_df.empty:
        return filtered_df
    
    # Process building_amenities column if it exists
    if 'building_amenities' in filtered_df.columns:
        # Check the first non-null value to determine format
        sample = filtered_df['building_amenities'].dropna().iloc[0] if not filtered_df['building_amenities'].dropna().empty else None
        
        if sample is not None:
            print(f"Sample building_amenities value: {sample}, type: {type(sample)}")
            
            # If it's a string that looks like a JSON array, parse it
            if isinstance(sample, str) and sample.startswith('[') and sample.endswith(']'):
                try:
                    # Convert JSON strings to actual Python lists
                    filtered_df['building_amenities'] = filtered_df['building_amenities'].apply(
                        lambda x: json.loads(x) if isinstance(x, str) and x.strip() else []
                    )
                    print("Successfully converted building_amenities from JSON strings to lists")
                except Exception as e:
                    print(f"Error converting building_amenities: {e}")
    
    # Debug: Print all available building amenities to help diagnose issues
    all_amenities = set()
    if 'building_amenities' in filtered_df.columns:
        for amenities_list in filtered_df['building_amenities'].dropna():
            if isinstance(amenities_list, list):
                all_amenities.update([a.lower() for a in amenities_list])
            elif isinstance(amenities_list, str):
                # Try to handle string format if conversion failed
                try:
                    parsed = json.loads(amenities_list)
                    if isinstance(parsed, list):
                        all_amenities.update([a.lower() for a in parsed])
                except:
                    # If it's not JSON, just add the string itself
                    all_amenities.add(amenities_list.lower())
        
        print("Available amenities:", sorted(all_amenities))
    
    for key, value in filters.items():
        if value is None:
            continue
            
        # Special handling for min_rent since it's not a direct column
        if key == 'min_rent':
            if 'actual_rent' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['actual_rent'] >= value]
            continue
            
        # Skip if column doesn't exist in the dataframe
        if key not in filtered_df.columns and key != 'building_amenities':
            continue
            
        if key == 'actual_rent':
            # Maximum rent
            filtered_df = filtered_df[filtered_df['actual_rent'] <= value]
        elif key == 'sqft' and key in filtered_df.columns:
            # Minimum square footage
            filtered_df = filtered_df[filtered_df[key] >= value]
        elif key == 'building_amenities' and isinstance(value, list) and len(value) > 0:
            # Handle building_amenities list if the column exists
            if 'building_amenities' in filtered_df.columns:
                # Debug: Print what we're looking for
                print(f"Looking for amenities: {value}")
                
                # Create a copy before filtering to avoid modifying during iteration
                temp_df = filtered_df.copy()
                
                for amen_name in value:
                    # Normalize the amenity name for comparison
                    amen_name_lower = amen_name.lower().strip()
                    print(f"Checking for amenity: '{amen_name_lower}'")
                    
                    # Use a safer approach with try/except to handle potential errors
                    try:
                        # More flexible matching - check if any amenity in the list contains our search term
                        temp_df = temp_df[temp_df['building_amenities'].apply(
                            lambda x: any(amen_name_lower in a.lower() for a in x) if isinstance(x, list) 
                                  else (amen_name_lower in x.lower() if isinstance(x, str) else False)
                        )]
                        print(f"After filtering for '{amen_name_lower}', {len(temp_df)} listings remain")
                    except Exception as e:
                        print(f"Error filtering by amenity {amen_name}: {e}")
                
                # Only update filtered_df if we found matches
                if not temp_df.empty:
                    filtered_df = temp_df
                else:
                    print(f"Warning: No listings found with amenities: {value}")
        elif key in filtered_df.columns:
            # Standard equality filter - with error handling
            try:
                # For list-type values, use a different approach
                if isinstance(value, list):
                    filtered_df = filtered_df[filtered_df[key].isin(value)]
                else:
                    # Handle potential type mismatches
                    if filtered_df[key].dtype == 'object' and not isinstance(value, str):
                        # Try to convert non-string value to string for comparison
                        filtered_df = filtered_df[filtered_df[key].astype(str) == str(value)]
                    else:
                        filtered_df = filtered_df[filtered_df[key] == value]
            except ValueError as e:
                print(f"Error filtering by {key}={value}: {e}")
                # Skip this filter if it causes an error
                continue
    
    return filtered_df


def extract_preferences_from_chat(chat_history, listings_df):
    # Extract all possible amenities from the dataset for validation
    all_possible_amenities = set()
    amenity_case_map = {}  # Map lowercase amenity names to their proper case
    
    # Use building_amenities column
    if 'building_amenities' in listings_df.columns:
        for amenities_list in listings_df['building_amenities'].dropna():
            # Handle JSON string format
            if isinstance(amenities_list, str) and amenities_list.startswith('[') and amenities_list.endswith(']'):
                try:
                    amenities_list = json.loads(amenities_list)
                except:
                    amenities_list = []
                    
            if isinstance(amenities_list, list):
                for amenity in amenities_list:
                    all_possible_amenities.add(amenity.lower())
                    amenity_case_map[amenity.lower()] = amenity  # Store proper case
    
    # Updated preference template with exact column names from your dataset
    preference_template = {
        "beds": None,
        "baths": None,
        "actual_rent": None,
        "min_rent": None,  # Special case for minimum rent filter
        "borough": None,
        "neighborhood": None,
        "sqft": None,
        "exposure": None,
        "floor_num": None,
        "floor_type": None,
        "countertop_type": None,
        "dishwasher": None,
        "laundry_in_unit": None,
        "laundry_in_building": None,
        "outdoor_space": None,
        "elevator": None,
        "wheelchair_access": None,
        "smoke_free": None,
        "heat_type": None,
        "stove_type": None,
        "pet_friendly": None,
        "lease_type": None,
        "building_amenities": None
    }
    
    # Only include columns that actually exist in the DataFrame and are relevant for filtering
    relevant_columns = [
        'beds', 'baths', 'actual_rent', 'borough', 'neighborhood', 'sqft', 
        'exposure', 'floor_num', 'floor_type', 'countertop_type', 'dishwasher',
        'laundry_in_unit', 'laundry_in_building', 'outdoor_space', 'elevator',
        'wheelchair_access', 'smoke_free', 'heat_type', 'stove_type', 
        'pet_friendly', 'lease_type', 'building_amenities'
    ]
    
    # Filter to only include columns that exist in the DataFrame
    existing_columns = [col for col in relevant_columns if col in listings_df.columns]
    
    unique_values = {}
    for col in existing_columns:
        try:
            unique_values[col] = listings_df[col].dropna().unique().tolist()
        except TypeError:
            # Skip columns with unhashable types like lists
            print(f"Skipping unique values for column {col} due to unhashable type")
    
    # Add available amenities to the context with proper case
    amenities_context = f"Available amenities in the database (CASE SENSITIVE, USE EXACT CASE): {', '.join(sorted(amenity_case_map.values()))}"
    
    prompt = (
        "You are a real estate chatbot assistant. Given the chat history below, extract user preferences for an apartment "
        "in NYC and populate the following dictionary template. If a preference isn't mentioned, leave it as None. "
        "Additionally, validate the extracted values based on the unique values from the listings database.\n\n"
        f"Chat History:\n{chat_history}\n\n"
        f"Listings Unique Values:\n{unique_values}\n\n"
        f"{amenities_context}\n\n"
        f"Preference Template Keys: {', '.join(preference_template.keys())}\n\n"
        "IMPORTANT INSTRUCTIONS:\n"
        "1. Return only the filled preference dictionary in JSON format. Make sure every value exists in the 'Listings Unique Values'\n"
        "2. If a value is not in the unique values, don't use it\n"
        "3. If a value is None, don't use it/return it\n"
        "4. If a value is updated, take the latest value mentioned\n"
        "5. If the user wants to remove or reset a preference, set that preference to null or remove it from the returned JSON\n"
        "6. Pay special attention to phrases like 'remove X', 'no longer want X', 'cancel X', 'reset X', 'don't need X', etc. - these indicate preferences to remove\n"
        "7. If the user says something like 'just search in [borough]', this means to remove any neighborhood restriction\n"
        "8. When the user changes a preference (e.g., 'increase rent to 9k'), update the value accordingly\n"
        "9. Make sure to only mark user preferences if you are sure the users response matches the preference\n"
        "10. Only extract preferences that the user EXPLICITLY states. DO NOT extract characteristics of apartments that the chatbot happens to mention\n"
        "11. If the bot describes an apartment as having 'cats only' for pet_friendly, but the user never mentions wanting cats, DO NOT set pet_friendly to 'cats only'\n"
        "12. CRITICAL: If a feature exists as its own field in the preference template (like elevator, etc.), DO NOT add it to the building_amenities array. Only add items to building_amenities if they do not have their own dedicated preference field.\n"
        "13. REMOVAL DETECTION: Pay careful attention to negative statements like 'I don't need X', 'I no longer want X', 'remove X from search', etc. These should result in removing that preference.\n"
        "14. For example, if the user says 'I don't need gameroom' or 'remove pool', you should remove 'Game Room' or 'Pool' from the building_amenities array.\n"
        "15. For removal, MAKE SURE to check all previous amenities and preferences and actively remove any that the user explicitly states they no longer want.\n"
        "16. CRITICAL: NEVER add 'pet_friendly' or any other preference unless the user EXPLICITLY requests it using those exact words. If the user doesn't mention pets at all, do not include pet_friendly in the returned JSON.\n"
        "17. STRICT RULE: If a preference (like pet_friendly) isn't mentioned by the user, it should NOT be included in the output JSON at all.\n"
        "18. DO NOT PRESERVE previous preferences that weren't explicitly mentioned by the user in the current conversation.\n"
        "19. When the user says 'show me the listings' or similar phrases, do not add or modify any preferences.\n"
        "20. AMENITY DETECTION: Pay special attention to phrases like 'with X', 'has X', 'includes X', etc. - check if X matches any available amenity and add it to the building_amenities array.\n"
        "21. For example, if user says '2 bed in bronx with game room', and 'Game Room' is in the available amenities list, add 'Game Room' to the building_amenities array.\n"
        "22. CASE MATTERS: Make sure to match the EXACT case of amenities as they appear in the available amenities list. For example, use 'Bike Storage' not 'bike storage'.\n"
        "23. TYPO HANDLING: Be aware of common typos like 'wiht' instead of 'with'. Don't interpret these as amenities or preferences.\n"
        "24. When you see phrases like 'I need a 2 bedroom apartment wiht 2 bath', interpret 'wiht' as 'with' and don't add it as an amenity.\n"
        "25. Only add items to building_amenities if they clearly match known amenities in the database.\n"
        "26. EXACT AMENITY MATCHING: When a user mentions an amenity like 'bike storage', you MUST use the exact case from the available amenities list, such as 'Bike Storage'.\n"
        "27. CRITICAL: For building_amenities, ALWAYS use the exact case and spelling from the available amenities list. Never use lowercase or variations."
    )
    
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "system", "content": prompt}],
        temperature=0
    )
    
    # Process the response to ensure proper case for amenities
    try:
        preferences = json.loads(response.choices[0].message.content)
        
        # Fix case for building_amenities if present
        if 'building_amenities' in preferences and isinstance(preferences['building_amenities'], list):
            corrected_amenities = []
            for amenity in preferences['building_amenities']:
                # Try to find the proper case version
                if amenity.lower() in amenity_case_map:
                    corrected_amenities.append(amenity_case_map[amenity.lower()])
                else:
                    corrected_amenities.append(amenity)
            
            preferences['building_amenities'] = corrected_amenities
            
            # Convert back to JSON
            return json.dumps(preferences)
        
        return response.choices[0].message.content
    except:
        # If parsing fails, return the original response
        return response.choices[0].message.content

@chat_bp.route("/start-chat", methods=["POST"])
def start_chat():
    # Get listings data using the existing function
    listings_result = get_filtered_listings_data()
    
    # Check if we got a successful response
    if listings_result.get("status") != "success":
        return jsonify({"message": "Sorry, I'm having trouble connecting to our listings database. Please try again later."})
    
    # Extract the actual listings data
    listings = listings_result.get("data", [])
    
    system_prompt = (
    "You are Vector Assistant, a helpful and friendly real estate agent chatbot for Vector Properties in NYC. "
    "Your primary goal is to help users find apartments that match their preferences, based on an internal database. "
    "You do not have access to this database—your only goal is to populate the user preferences JSON. "
    "Always respond professionally and concisely while providing the best options available. "
    "Always only ask for one preference at a time—NEVER ask for 2+ preferences at once! "
    
    """Your goal is to continually ask questions to the user, trying to extract the info below from them:
    
    First, try to get a CORE PREFERENCE. If they don't have a preference, move on. 
    Only ask one question at a time, moving down the list. 
    
    # REQUIRED CORE PREFERENCES:
    - bedrooms: [number or null if not mentioned/changed]
    - bathrooms: [number or null if not mentioned/changed]
    - rent: [maximum rent as number or null if not mentioned/changed]
    - borough: [Manhattan, Brooklyn, Queens, Bronx, Staten Island, or null if not mentioned/changed]
    - neighborhood: [specific neighborhood name or null if only borough mentioned]

    # LOCATION EXTRACTION INSTRUCTIONS:
    - For general areas like "Manhattan" or "Brooklyn", set the borough field.
    - For specific neighborhoods like "Chelsea" or "Williamsburg", set both borough and neighborhood.
    - If the user gives a misspelled location (like "manhatten"), correct it to the proper borough.

    # OPTIONAL PROPERTY DETAILS:
    - sqft: [square footage as number or null if not mentioned/changed]
    - doorman: [true/false or null if not mentioned/changed]
    - elevator: [true/false or null if not mentioned/changed]
    - gym: [true/false or null if not mentioned/changed]
    - laundry: [true/false or null if not mentioned/changed]
    - pets_allowed: [description or null if not mentioned/changed]
    - amenities: [list of all preferred amenities]

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
    - If the user says "no" or indicates they have no more preferences, NEVER end the conversation - instead, proceed to ask about the next preference in the list above.

    # CRITICAL RULES ABOUT LISTING PROPERTIES:
    - NEVER list or describe specific properties, addresses, or unit numbers
    - NEVER use numbered lists to present property options
    - NEVER say things like "Here are some listings that match your criteria"
    - NEVER mention specific property details like "971 Cedar St Unit 9K"
    - Only tell the user how many listings match their criteria (e.g., "I found 5 listings that match your preferences")
    - The application will handle showing the actual listings - your job is only to help gather preferences
    - If the user asks about specific properties, respond with "I can tell you that there are [number] properties that match your criteria" but never list them
    - Continue asking questions about preferences even if there are only a few matches

    # STRICT RULES:
        - NEVER assume what the user might want—always confirm with them.
        - The conversation must continue indefinitely. Do not stop asking until the user explicitly ends the conversation.
        - NEVER return null, just dont return a key:value if its empty/no preference
        - NEVER say you're going to compile listings or gather options - you must always ask the next question.
        - If the user has specified all core preferences and some optional ones, circle back to any remaining optional preferences they haven't specified.
        """
    )

    # Initialize session data with system prompt and a warm welcome message
    session['messages'] = [
        {"role": "system", "content": system_prompt}
    ]
    session.modified = True  # Mark the session as modified
    
    # Initial welcome message from the bot
    welcome_message = (
        "Hi there! I'm Vector Assistant, your personal NYC apartment hunting guide. "
        "I'm here to help you find the perfect place to call home in the city. "
        "Let's start with the basics - how many bedrooms are you looking for in your new apartment?"
    )
    
    # Save the welcome message in the chat history
    session['messages'].append({"role": "assistant", "content": welcome_message})
    
    # Initialize preferences in session
    session['preferences'] = {}
    
    # Return the welcome message
    return jsonify({"message": welcome_message})

@chat_bp.route("/chat", methods=["POST", "GET"])
def chat():
    if request.method == "GET":
        # Handle GET request
        return jsonify({"message": "Please use POST method to send chat messages"})
    
    # Get the user's message from the request
    follow_up = request.json.get("message", "").strip()
    
    # Initialize session if it doesn't exist
    if 'messages' not in session:
        session['messages'] = []
        session['preferences'] = {}
        
        # Add initial system message
        session['messages'].append({
            "role": "system", 
            "content": "You are a helpful real estate assistant helping users find apartments in NYC. Ask questions to understand their preferences."
        })
    
    # Add the user's message to the conversation history
    session['messages'].append({"role": "user", "content": follow_up})
    
    # Get current preferences
    current_preferences = session.get('preferences', {})
    
    # Get listings data
    listings = get_filtered_listings_data()
    
    # Extract preferences from the conversation
    if follow_up:
        try:
            # Extract preferences from the conversation
            preferences_json = extract_preferences_from_chat(session['messages'], pd.DataFrame(listings))
            
            # Parse the JSON response
            try:
                new_preferences = json.loads(preferences_json)
                print("Extracted preferences:", new_preferences)
                
                # Special handling for building_amenities to accumulate them
                if 'building_amenities' in new_preferences and isinstance(new_preferences['building_amenities'], list):
                    # If we already have amenities, add the new ones
                    if 'building_amenities' in current_preferences and isinstance(current_preferences['building_amenities'], list):
                        # Create a set to avoid duplicates
                        existing_amenities = set(current_preferences['building_amenities'])
                        for amenity in new_preferences['building_amenities']:
                            existing_amenities.add(amenity)
                        # Update with the combined list
                        current_preferences['building_amenities'] = list(existing_amenities)
                    else:
                        # First time adding amenities
                        current_preferences['building_amenities'] = new_preferences['building_amenities']
                    
                    # Remove from new_preferences so we don't overwrite below
                    del new_preferences['building_amenities']
                
                # Update other preferences
                current_preferences.update(new_preferences)
                
                # Save updated preferences to session
                session['preferences'] = current_preferences
                print("Updated preferences:", current_preferences)
                
            except json.JSONDecodeError as e:
                print(f"Error parsing preferences JSON: {e}")
                print(f"Raw JSON: {preferences_json}")
        except Exception as e:
            print(f"Error extracting preferences: {e}")
    
    # Filter listings based on current preferences
    filtered_listings = filter_listings(listings, current_preferences)
    
    # Define the maximum number of listings to show without explicit request
    max_listings_without_request = 5
    
    # Check if user explicitly asked to see listings
    show_listings = any(pattern in follow_up.lower() for pattern in [
        "show me", "list", "display", "what do you have", "what's available", 
        "what is available", "see the", "view the"
    ])
    
    # Prepare system context with available options
    unique_values = {}
    unique_amenities = []
    detailed_listings_info = ""
    
    if not filtered_listings.empty:
        # Only include relevant columns for the unique values
        relevant_columns = [
            'beds', 'baths', 'borough', 'neighborhood', 'exposure', 'floor_num', 
            'floor_type', 'countertop_type', 'dishwasher', 'laundry_in_unit', 
            'laundry_in_building', 'outdoor_space', 'elevator', 'wheelchair_access', 
            'smoke_free', 'heat_type', 'stove_type', 'pet_friendly', 'lease_type', 'building_amenities'
        ]
        
        # Filter to only include columns that exist in the DataFrame
        existing_columns = [col for col in relevant_columns if col in filtered_listings.columns]
        
        unique_values = {}
        for col in existing_columns:
            try:
                unique_values[col] = filtered_listings[col].dropna().unique().tolist()
            except TypeError:
                # Skip columns with unhashable types like lists
                print(f"Skipping unique values for column {col} due to unhashable type")
        
        # Extract all unique amenities from the building_amenities column (which contains lists)
        all_amenities = []
        if 'building_amenities' in filtered_listings.columns:
            for amenities_list in filtered_listings['building_amenities'].dropna():
                if isinstance(amenities_list, list):
                    all_amenities.extend(amenities_list)

        # Count occurrences for better insights
        amenity_counts = {}
        for amenity in all_amenities:
            amenity_counts[amenity] = amenity_counts.get(amenity, 0) + 1

        # Format as a readable string with counts
        unique_amenities = [f"{amenity} ({count} listings)" for amenity, count in sorted(amenity_counts.items(), key=lambda x: x[1], reverse=True)]
        
        # Add detailed listing information when there are fewer than 15 listings
        if len(filtered_listings) < 15:
            detailed_listings_info = "DETAILED LISTING INFORMATION (FOR YOUR REFERENCE ONLY, DO NOT SHARE THESE DETAILS WITH USER):\n"
            for idx, listing in filtered_listings.iterrows():
                amenities_str = ", ".join(listing.get('building_amenities', [])) if isinstance(listing.get('building_amenities'), list) else ""
                detailed_listings_info += f"Listing {idx+1}: {listing['address']} Unit {listing['unit']}, {listing['borough']}, {listing['neighborhood']}\n"
                detailed_listings_info += f"  - {listing['beds']} bed, {listing['baths']} bath, ${listing['actual_rent']}/month, {listing['sqft']} sqft\n"
                detailed_listings_info += f"  - Pet policy: {listing.get('pet_friendly', 'N/A')}\n"
                detailed_listings_info += f"  - Elevator: {listing.get('elevator', 'N/A')}, Wheelchair access: {listing.get('wheelchair_access', 'N/A')}\n"
                detailed_listings_info += f"  - Laundry in unit: {listing.get('laundry_in_unit', 'N/A')}, Laundry in building: {listing.get('laundry_in_building', 'N/A')}\n"
                detailed_listings_info += f"  - Exposure: {listing.get('exposure', 'N/A')}\n"
                detailed_listings_info += f"  - Amenities: {amenities_str}\n\n"
    
    # Add available options to the system message for the AI to suggest
    system_context = f"""
    Current available options in filtered listings:
    {json.dumps(unique_values, indent=2)}
    
    Available amenities with counts: {', '.join(unique_amenities)}

    Current listing count: {len(filtered_listings)}
    
    {detailed_listings_info}
    
    IMPORTANT INSTRUCTIONS:
    1. When suggesting preferences, ONLY suggest options that are available in the current filtered listings shown above.
    2. Be aware of ALL available amenities in the current listings and mention them appropriately when relevant.
    3. If a preference type (like doorman or elevator) has no options in the filtered listings, do NOT ask about it.
    4. When users ask about specific features (like a pool), CHECK if it exists in the available amenities list before responding.
    5. BE ACCURATE about what amenities exist in the current listings - don't suggest amenities that aren't available.
    6. If notable amenities like pools, saunas, or rooftop decks exist in the listings, feel free to mention these to the user as potentially interesting features.

    LIFESTYLE AND NEIGHBORHOOD GUIDANCE:
    - If the user asks about neighborhoods with specific attributes (nightlife, family-friendly, quiet, etc.), provide detailed recommendations based on research and available options.

    BE CONVERSATIONAL AND HELPFUL:
    - Respond naturally like a knowledgeable real estate agent would.
    - If asked about neighborhood characteristics, provide substantial information before continuing with preference collection. Do some research to find out what the user is looking for.
    - Share insights about neighborhoods that match lifestyle preferences mentioned.
    - When possible, explain why certain areas might be good matches for the user's stated interests.
    
    IMPORTANT RULES ABOUT LISTINGS:
    - NEVER list specific properties to the user. Don't mention specific addresses, unit numbers, or detailed descriptions.
    - If there are fewer than 5 listings available, simply say something like "I found [number] listings that match your criteria!"
    - Even if the user asks for listing details, do not provide them - just acknowledge you found matches.
    - Never mention specific amenities of specific listings - the user will see these details separately.
    - Focus ONLY on asking questions about preferences and helping refine the search.
    - CRITICAL: The application will handle showing the actual listings, your job is only to help gather preferences.
    """
    
    # Also update the system_context to encourage more filtering if there are many results
    if len(filtered_listings) > max_listings_without_request:
        system_context += f"\n\nIMPORTANT: There are currently {len(filtered_listings)} listings, which is too many to display. Continue asking the user questions to narrow down their preferences until there are 5 or fewer results, unless they explicitly ask to see the listings."
    else:
        system_context += f"\n\nThere are only {len(filtered_listings)} listings that match the current criteria, which is few enough to display."
    
    session['messages'].append({"role": "system", "content": system_context})
    
    # Get new response with updated context
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=session['messages'],
        temperature=0.7  # Slightly increased for more natural responses
    )

    # Extract response text
    response_text = response.choices[0].message.content
    print("Assistant:", response_text)

    # Check if there's a mismatch between listing count and message
    if len(filtered_listings) > 0 and any(phrase in response_text.lower() for phrase in [
        "couldn't find any listings", 
        "no listings", 
        "not available", 
        "not a specific amenity available",
        "is not currently available",
        "not currently available"
    ]):
        # Get the current preferences
        current_prefs = current_preferences.copy()
        
        # Check if building_amenities is causing the issue
        if 'building_amenities' in current_prefs:
            amenities_requested = current_prefs.get('building_amenities', [])
            
            # Create a more accurate message
            amenities_str = ", ".join([f'"{a}"' for a in amenities_requested])
            response_text = (
                f"I found {len(filtered_listings)} apartments that match your criteria, including {amenities_str}. "
                f"Would you like to know more about these listings or refine your search further with additional preferences?"
            )
        else:
            # Generic correction for other cases
            response_text = f"I found {len(filtered_listings)} apartments that match your criteria. Would you like to see these listings or refine your search further?"
        
        # Update the message in the session history
        session['messages'].append({"role": "assistant", "content": response_text})
        
    # Add the assistant's response to the conversation history
    else:
        session['messages'].append({"role": "assistant", "content": response_text})
    
    # Save the updated conversation history to the session
    session.modified = True
    
    # Return the response as JSON
    response_data = {
        "message": response_text,
        "preferences": current_preferences,
        "listing_count": len(filtered_listings)
    }
    
    return jsonify(response_data)
