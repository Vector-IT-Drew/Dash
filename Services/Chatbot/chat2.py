# import os
# import mysql.connector
# from flask import Blueprint, Flask, jsonify, request, session
# import logging
# from mysql.connector import Error
# import decimal
# from datetime import datetime
# from openai import OpenAI
# import json
# import pandas as pd
# from dotenv import load_dotenv
# import requests
# import ast

# Replace with environment variable
# client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

# Replace hardcoded API key with:

# listings_info = requests.get('https://dash-production-b25c.up.railway.app/get_filtered_listings?include_all=True')
# listing_count = listings_info.json()['count']
# listings = pd.DataFrame(listings_info.json()['data'])

# # Define a basic system prompt
# prompt = f"""
#     You are Vector Assistant, a helpful and friendly real estate agent chatbot for Vector Properties in NYC. 
#     Your primary goal is to help users find apartments that match their preferences, based on an internal database. 
    
#     First, try to get a CORE PREFERENCE. If they don't have a preference, move on. 
#     Only ask one question at a time, moving down the list. 
    
#     Then continue going through the other options, and asking the user their preferences. Only ask them for one preference at a time.
    
#     # REQUIRED CORE PREFERENCES:
#     - beds: Number
#     - baths: Number
#     - maximum_rent: Number
#     - borough: String
#     - neighborhood: String

#     # LOCATION EXTRACTION INSTRUCTIONS:
#     - For general areas like "Manhattan" or "Brooklyn", set the borough field.
#     - For specific neighborhoods like "Chelsea" or "Williamsburg", set both borough and neighborhood.
#     - If the user gives a misspelled location (like "manhatten"), correct it to the proper borough.

#     # OPTIONAL PROPERTY DETAILS:
#     - sqft: Number
#     - doorman: Boolean
#     - elevator: Boolean
#     - gym: Boolean
#     - laundry: Boolean
#     - pets_allowed: Boolean
#     - amenities: [list of all preferred amenities]
#     - minimum_rent: Number

#      # CRITICAL VALIDATION INSTRUCTIONS:
#     - NEVER acknowledge invalid preferences as acceptable
#     - If a user requests a value outside our available options (like a budget below our minimum price), you MUST:
#       1. Immediately inform them that their request doesn't match our available options
#       2. Tell them the actual range/options available
#       3. Ask them to adjust their preference
#     - For example, if they say "my budget is $7,500" but our minimum price is $8,000, say:
#       "I'm sorry, but $7,500 is below our minimum available price of $8,000. Our apartments range from $8,000 to $X. Would you be able to adjust your budget?"
#     - Similarly, if they request 1 bedroom but we only have 2+ bedroom apartments, tell them what options are available
#     - NEVER pretend to accept an invalid preference just to be polite
#     - ALWAYS check if a user's preference is valid before acknowledging it

    
#     # HANDLING SINGLE OPTIONS:
#     - If there's only one option available for a preference (e.g., only one borough or only one bathroom option), 
#       DON'T ask the user about it. Instead, inform them: "Based on your other preferences, we only have listings in [value]."
#     - For example, if all remaining listings have 2 bathrooms, say: "Based on your other preferences, all available listings have 2 bathrooms."
#     - Then immediately move on to the next preference question.
#     - IMPORTANT: Check the DataBase Info section carefully before asking about any preference. If there's only one value listed for a field (e.g., only "Manhattan" in Boroughs), don't ask about it.
#     - For Boolean features, if all listings have the same value (e.g., all have doorman=True), don't ask about it.
    
#     # HANDLING AMENITY REQUESTS:
#     - If the user requests an amenity, FIRST check if it exists in the unique amenities list from the database.
#     - If the amenity exists, add it to the preferences.
#     - If the amenity does NOT exist, inform the user that no current listings have this, then:
#         - Suggest a similar available amenity if applicable.
#         - If no similar options exist, politely move on to the next preference.

#     # HANDLING GENERAL RESPONSES:
#     - If the user asks for examples or suggestions (e.g., "what are some options?"), tell them how many listings match their criteria but NEVER list specific properties.
#     - If the user gives a vague response (e.g., "someplace quiet"), ask clarifying questions about what "quiet" means to them. Do they want a low-crime neighborhood? A residential area with less foot traffic?
#     - If the user refuses to provide details and just says "I don't know," move on to the next preference without insisting.
#     - If the user says "no" or indicates they have no more preferences, NEVER end the conversation - instead, proceed to ask about the next preference.
    
#     # CRITICAL RULES ABOUT LISTING PROPERTIES:
#     - NEVER list or describe specific properties, addresses, or unit numbers. Just use the DataBase Info below to show what options are currently available.
#     - NEVER use numbered lists to present property options. NEVER present property options.
#     - NEVER say things like "Here are some listings that match your criteria"
#     - The data below shows all the unique values that we currently have for available listings. This will get updated as the conversation goes on, and the listings narrow down. 
#     - If the user ever has a preference that does not exist in the available options listed in "DataBase Info" below, let them know, and let them know the available options/alternatives. 
#     - Never say anything like "Based on our data"
#     - Never say "Would you like me to look for..." or "I Found listings..." Your whole purpose is to keep asking questions about their preferences, you will never show them the results. 
#     - Pay special attention to phrases like 'remove X', 'no longer want X', 'cancel X', 'reset X', 'don't need X', etc. - these indicate preferences to remove
#     - If the user says something like 'just search in [borough]', this means to remove any neighborhood restriction
#     - When the user changes a preference (e.g., 'increase rent to 9k'), update the value accordingly
#     - Make sure to only mark user preferences if you are sure the users response matches the preference
#     - Only extract preferences that the user EXPLICITLY states. DO NOT extract characteristics of apartments that the chatbot happens to mention
#     - DO NOT PRESERVE previous preferences that weren't explicitly mentioned by the user in the current conversation.
#     - AMENITY DETECTION: Pay special attention to phrases like 'with X', 'has X', 'includes X', etc. - check if X matches any available amenity and add it to the building_amenities array.
#     - For example, if user says '2 bed in bronx with game room', and 'Game Room' is in the available amenities list, add 'Game Room' to the building_amenities array.
#     - CASE MATTERS: Make sure to match the EXACT case of amenities as they appear in the available amenities list. For example, use 'Bike Storage' not 'bike storage'.
#     - TYPO HANDLING: Be aware of common typos like 'wiht' instead of 'with'. Don't interpret these as amenities or preferences.
#     - Only add items to building_amenities if they clearly match known amenities in the database.

#     # CRITICAL BOOLEAN FEATURES VS AMENITIES
#     - The following are BOOLEAN FEATURES and should be set as separate boolean preferences (True/False), NOT added to building_amenities:
#       - doorman
#       - elevator
#       - wheelchair_access
#       - smoke_free
#       - laundry_in_building
#       - laundry_in_unit
#       - pet_friendly
#       - live_in_super
#       - concierge
#     - For example, if the user says "I want a doorman", set doorman=True, NOT building_amenities=["Doorman"]
#     - Only add items to building_amenities if they match the specific amenities list from the database

#     # HANDLING SHOW LISTINGS REQUESTS:
#     - If the user explicitly asks to see listings with phrases like "show me the listings", "I want to see the apartments", etc., respond with:
#       "Great! I'll show you the available listings that match your criteria. Here they are!"
#     - NEVER say you can't show listings or that you're just gathering information - the system will handle showing the actual listings.
#     - When the user asks to see listings, your job is to acknowledge their request positively and indicate that listings are being shown.

#     # PRICE VALIDATION - EXTREMELY IMPORTANT:
#     - When a user mentions a budget or maximum price, IMMEDIATELY check if it's within our available range
#     - This price range will be updated in future prompts. Use the new values indicated in future prompts, for the logic below.
#     - Our minimum price is ${listings['actual_rent'].min()} and maximum is ${listings['actual_rent'].max()} .
#     - If they say any value below ${listings['actual_rent'].min()}, tell them:
#       "I'm sorry, but that budget is below our minimum available price of ${listings['actual_rent'].min()}. 
#        Our apartments range from ${listings['actual_rent'].min()} to ${listings['actual_rent'].max()}. 
#        Would you be able to adjust your budget?"
#     - NEVER say "Perfect" or "Great" to acknowledge an invalid budget


#     DataBase Info - (Here is an overview of what is currently inside the database.):
#         Minimum Beds - {listings['beds'].min()}
#         Maximum Beds - {listings['beds'].max()}
        
#         Minimum Baths - {listings['baths'].min()}
#         Maximum Baths - {listings['baths'].max()}
        
#         Minimum Price - {listings['actual_rent'].min()}
#         Maximum Price - {listings['actual_rent'].max()}
                
#         Boroughs - {', '.join(sorted(set([val for val in listings.borough.unique() if val and len(val) > 1])))}
#         Neighborhoods - {', '.join(sorted(set([val for val in listings.neighborhood.unique() if val and len(val) > 1])))}
        
#         Amenities - {', '.join(sorted(set(item for sublist in listings.building_amenities.apply(json.loads) if isinstance(sublist, list) for item in sublist)))}
        
#         Exposures - {', '.join(sorted(set([val for val in listings.exposure.apply(lambda x: x.strip()).unique() if val and len(val) > 1])))}
        
#         # Boolean Features
#         Doorman - {all(listings['doorman']) if 'doorman' in listings.columns else 'N/A'} (All), {any(listings['doorman']) if 'doorman' in listings.columns else 'N/A'} (Some)
#         Elevator - {all(listings['elevator']) if 'elevator' in listings.columns else 'N/A'} (All), {any(listings['elevator']) if 'elevator' in listings.columns else 'N/A'} (Some)
#         Wheelchair Access - {all(listings['wheelchair_access']) if 'wheelchair_access' in listings.columns else 'N/A'} (All), {any(listings['wheelchair_access']) if 'wheelchair_access' in listings.columns else 'N/A'} (Some)
#         Smoke Free - {all(listings['smoke_free']) if 'smoke_free' in listings.columns else 'N/A'} (All), {any(listings['smoke_free']) if 'smoke_free' in listings.columns else 'N/A'} (Some)
#         Laundry in Building - {all(listings['laundry_in_building']) if 'laundry_in_building' in listings.columns else 'N/A'} (All), {any(listings['laundry_in_building']) if 'laundry_in_building' in listings.columns else 'N/A'} (Some)
#         Laundry in Unit - {all(listings['laundry_in_unit']) if 'laundry_in_unit' in listings.columns else 'N/A'} (All), {any(listings['laundry_in_unit']) if 'laundry_in_unit' in listings.columns else 'N/A'} (Some)
#         Pet Friendly - {all(listings['pet_friendly']) if 'pet_friendly' in listings.columns else 'N/A'} (All), {any(listings['pet_friendly']) if 'pet_friendly' in listings.columns else 'N/A'} (Some)
#         Live-in Super - {all(listings['live_in_super']) if 'live_in_super' in listings.columns else 'N/A'} (All), {any(listings['live_in_super']) if 'live_in_super' in listings.columns else 'N/A'} (Some)
#         Concierge - {all(listings['concierge']) if 'concierge' in listings.columns else 'N/A'} (All), {any(listings['concierge']) if 'concierge' in listings.columns else 'N/A'} (Some)
        
#     Notes:
#         Always Start the chat with: "Hi there! I'm Vector Assistant, your personal NYC apartment hunting guide. 
#                                     Let's start with the basics - how many bedrooms are you looking for in your new apartment?"

# """


# messages = [{"role": "system", "content": prompt}]

# print("💬 Chat with the bot! Type 'exit' to quit.\n")

# # After extracting preferences, filter the listings
# def filter_listings_by_preferences(listings_df, preferences):
#     """
#     Filter listings based on user preferences
    
#     Args:
#         listings_df (pd.DataFrame): DataFrame of listings
#         preferences (dict): User preferences
    
#     Returns:
#         pd.DataFrame: Filtered listings
#     """
#     filtered_df = listings_df.copy()
#     valid_preferences = preferences.copy()
    
#     if filtered_df.empty:
#         return filtered_df
    
#     # Pre-validate preferences to remove any that would result in zero listings
#     if 'maximum_rent' in valid_preferences and valid_preferences['maximum_rent'] is not None:
#         if 'actual_rent' in filtered_df.columns:
#             min_available_rent = filtered_df['actual_rent'].min()
#             if valid_preferences['maximum_rent'] < min_available_rent:
#                 print(f"Ignoring invalid maximum_rent: {valid_preferences['maximum_rent']} (minimum available is {min_available_rent})")
#                 del valid_preferences['maximum_rent']
    
#     # Process building_amenities column if needed
#     if 'building_amenities' in filtered_df.columns:
#         # Check if building_amenities is a string and convert to list if needed
#         if isinstance(filtered_df['building_amenities'].iloc[0], str):
#             filtered_df['building_amenities'] = filtered_df['building_amenities'].apply(
#                 lambda x: json.loads(x) if isinstance(x, str) and x.strip() else []
#             )
    
#     # Filter by each preference one by one and check if it results in zero listings
#     for key, value in list(valid_preferences.items()):  # Use list() to allow modification during iteration
#         if value is None or value == [] or key == 'listing_count' or key == 'show_listings':
#             continue
        
#         temp_df = filtered_df.copy()
        
#         # Handle special cases
#         if key == 'maximum_rent' and value is not None:
#             if 'actual_rent' in temp_df.columns:
#                 temp_df = temp_df[temp_df['actual_rent'] <= value]
        
#         elif key == 'minimum_rent' and value is not None:
#             if 'actual_rent' in temp_df.columns:
#                 temp_df = temp_df[temp_df['actual_rent'] >= value]
        
#         elif key == 'building_amenities' and isinstance(value, list) and len(value) > 0:
#             if 'building_amenities' in temp_df.columns:
#                 # For each amenity in preferences, filter listings that have it
#                 for amenity in value:
#                     amenity_df = temp_df[temp_df['building_amenities'].apply(
#                         lambda x: any(amenity.lower() in a.lower() for a in x) if isinstance(x, list) else False
#                     )]
#                     # Only apply filter if it doesn't result in zero listings
#                     if not amenity_df.empty:
#                         temp_df = amenity_df
#                     else:
#                         print(f"Warning: No listings found with amenity '{amenity}'")
#                         # Remove this amenity from the list
#                         valid_preferences['building_amenities'].remove(amenity)
        
#         # Direct column matches
#         elif key in temp_df.columns:
#             if key == 'beds':
#                 # For beds, allow for a range within 0.5 of the requested value
#                 temp_df = temp_df[(temp_df[key] >= value - 0.5) & (temp_df[key] <= value + 0.5)]
            
#             elif key == 'baths':
#                 # For baths, allow for a range within 0.5+ of the requested value
#                 temp_df = temp_df[temp_df[key] >= value - 0.5]
            
#             elif key == 'borough' or key == 'neighborhood':
#                 # Case-insensitive string comparison for location
#                 temp_df = temp_df[temp_df[key].str.lower() == value.lower()]
            
#             elif key == 'exposure' and value is not None:
#                 # For exposure, check if the user's preference is contained within the listing's exposure
#                 temp_df = temp_df[temp_df[key].str.lower().str.contains(value.lower())]
            
#             elif key == 'sqft' and value is not None:
#                 # For sqft, filter for minimum value
#                 temp_df = temp_df[temp_df[key] >= value]
            
#             # Boolean preferences
#             elif key in ['doorman', 'elevator', 'gym', 'laundry', 'pets_allowed', 
#                          'laundry_in_unit', 'laundry_in_building', 'outdoor_space', 
#                          'wheelchair_access', 'smoke_free', 'pet_friendly', 
#                          'live_in_super', 'concierge']:
#                 temp_df = temp_df[temp_df[key] == value]
        
#         # Check if this preference resulted in zero listings
#         if temp_df.empty:
#             print(f"Removing preference {key}={value} as it results in zero listings")
#             del valid_preferences[key]
#         else:
#             # Apply the filter
#             filtered_df = temp_df
    
#     return filtered_df, valid_preferences


# # Now update your main loop to use this function
# while True:
#     user_input = input("You: ")
#     if user_input.lower() in ["exit", "quit"]:
#         break

#     messages.append({"role": "user", "content": user_input})

#     # Check if the previous preferences had show_listings set to True
#     show_listings_requested = False
#     if 'preferences' in locals() and 'show_listings' in preferences and preferences['show_listings'] == True:
#         show_listings_requested = True
#         # Add a special message to inform the model that listings should be shown
#         messages.append({"role": "system", "content": "The user has requested to see listings. Respond as if you're showing them the listings."})

#     response = client.chat.completions.create(
#         model="gpt-4",
#         messages=messages
#     )

#     reply = response.choices[0].message.content
#     # Print token usage information
#     total_prompt_tokens = response.usage.prompt_tokens
#     total_completion_tokens = response.usage.completion_tokens
#     total_tokens = response.usage.total_tokens
#     print(f"\n--- Token Usage ---")
#     print(f"Prompt tokens: {total_prompt_tokens}")
#     print(f"Completion tokens: {total_completion_tokens}")
#     print(f"Total tokens: {total_tokens}")
#     print(f"Estimated cost: ${(total_prompt_tokens * 0.00003) + (total_completion_tokens * 0.00006):.4f}")
#     print(f"-------------------\n")

#     print("🤖:", reply)
#     messages.append({"role": "assistant", "content": reply})
    
#     # Remove the special system message if it was added
#     if show_listings_requested:
#         messages = [msg for msg in messages if msg["content"] != "The user has requested to see listings. Respond as if you're showing them the listings."]
    
#     convo = '/n'.join([(f"{mes['role']}: {mes['content']}") for mes in messages[1:]])
#     prompt = f"""Your goal is to take this conversation history, and extract all the users preferences. 

#         The user may change their preferences over time, so be sure to set these values using the most up to date conversation sentences.
#         Only make assumptions for things like ("5k" should be 5000 , correct misspellings, make sure their selected amenities exist in the current options.)
        
#         CRITICAL: NEVER add a preference value that doesn't exist in the current database options.
#         - If the user requests a value that would result in zero listings, DO NOT include it in the preferences.
#         - For example, if they ask for 1 bathroom but the database only has 1.5 or 2 bathrooms, DO NOT set baths=1.
#         - Instead, leave that preference unset so the AI can explain the available options to the user.
#         - Always check the "DataBase Info" section to see what values are currently available.
        
#         PRICE VALIDATION - THIS IS EXTREMELY IMPORTANT:
#         - If the user specifies a maximum_rent that is below the minimum available price, DO NOT set maximum_rent.
#         - If the user specifies a minimum_rent that is above the maximum available price, DO NOT set minimum_rent.
#         - For example, if the minimum price in the database is $3000 and the user says "my budget is $2000", DO NOT set maximum_rent=$2000.
#         - If the user says "my budget is $7500" but the minimum price is $8000, DO NOT set maximum_rent=$7500.
#         - ALWAYS check that the user's maximum_rent is >= the minimum available price in the database.
#         - ALWAYS check that the user's minimum_rent is <= the maximum available price in the database.
        
#         If a preference does not fit in the criteria, NEVER return an incorrect value (i.e. if they mention a budget below the minimum actual_rent, NEVER set the actual_rent in this case, say their budget is too low, and mention the minimum)
#         NEVER assume any values based on the systems question/response, always make sure the user is the one with the final say. 
#         Make sure to only set these values as their indicated types. For example, beds must always be a number, and baths must always be a number. 
#         If the User gives something else, determine what is the correct value to use, if any.
        
#         IMPORTANT: Set "show_listings" to True ONLY if the user is explicitly asking to see the listings or results.
#         Examples of when to set show_listings = True:
#         - "Show me the listings"
#         - "I want to see the apartments"
#         - "Show me what you found"
#         - "What options do you have?"
#         - "Let me see the results"
#         - "Show me what's available"
        
#         Do NOT set show_listings = True if the user is still discussing preferences or asking questions about neighborhoods, amenities, etc.
        
#         # CRITICAL BOOLEAN FEATURES VS AMENITIES
#         - The following are BOOLEAN FEATURES and should be set as separate boolean preferences (True/False), NOT added to building_amenities:
#           - doorman
#           - elevator
#           - wheelchair_access
#           - smoke_free
#           - laundry_in_building
#           - laundry_in_unit
#           - pet_friendly
#           - live_in_super
#           - concierge
#         - For example, if the user says "I want a doorman", set doorman=True, NOT building_amenities=["Doorman"]
#         - Only add items to building_amenities if they match the specific amenities list from the database
        
#         Here is a list of all the possible keys that can be used for a preferences:
#             maximum_rent - "This is the max price a person wil pay for their unit" - Number
#             minimum_rent - "This is the min price a person wil pay for their unit" - Number
#             baths - "How many bathrooms do they want" - Number
#             beds - "How many bedrooms do they want" - Number
#             borough - "What borough do they want to be in" - String
#             building_amenities - "List of all the amenities" - List of Strings (**Options are: {set(item for sublist in listings.building_amenities.apply(json.loads) if isinstance(sublist, list) for item in sublist)})
#             countertop_type - String
#             dishwasher - Boolean
#             doorman - Boolean
#             elevator - Boolean
#             exposure - "North, South, East, West, Northeast, Southwest, etc."
#             floor_num - Number
#             floor_type - String
#             laundry_in_building - Boolean
#             laundry_in_unit - Boolean
#             neighborhood - "What neighborhood do they want to be in" - String
#             outdoor_space - Boolean
#             pet_friendly - Boolean
#             smoke_free - Boolean
#             sqft - Number
#             wheelchair_access - Boolean
#             live_in_super - Boolean
#             concierge - Boolean
#             show_listings - Boolean (set to True ONLY if user explicitly asks to see listings)

#         DataBase Info - (Here is an overview of what is currently inside the database.):
#             Minimum Beds - {listings['beds'].min()}
#             Maximum Beds - {listings['beds'].max()}
            
#             Minimum Baths - {listings['baths'].min()}
#             Maximum Baths - {listings['baths'].max()}
            
#             Minimum Price - {listings['actual_rent'].min()}
#             Maximum Price - {listings['actual_rent'].max()}
            
#             Applicant Types - {', '.join(sorted(set([val for val in listings.applicance_type.unique() if val and len(val) > 1])))}
            
#             Boroughs - {', '.join(sorted(set([val for val in listings.borough.unique() if val and len(val) > 1])))}
#             Neighborhoods - {', '.join(sorted(set([val for val in listings.neighborhood.unique() if val and len(val) > 1])))}
            
#             Amenities - {', '.join(sorted(set(item for sublist in listings.building_amenities.apply(json.loads) if isinstance(sublist, list) for item in sublist)))}
            
#             Exposures - {', '.join(sorted(set([val for val in listings.exposure.apply(lambda x: x.strip()).unique() if val and len(val) > 1])))}

#         Previous Convo History:
#             {convo}

#         Returns:
#             Return ONLY a JSON dictionary, with key:value pairs for all the preferences extracted from the convo.
#             DO NOT include any preference that would result in zero listings based on the database information.
#     """

#     response = client.chat.completions.create(
#         model="gpt-4",
#         messages=[{"role": "user", "content": prompt}]
#     )
    
#     # Print token usage for preferences extraction
#     pref_prompt_tokens = response.usage.prompt_tokens
#     pref_completion_tokens = response.usage.completion_tokens
#     pref_total_tokens = response.usage.total_tokens
#     print(f"\n--- Preferences Extraction Token Usage ---")
#     print(f"Prompt tokens: {pref_prompt_tokens}")
#     print(f"Completion tokens: {pref_completion_tokens}")
#     print(f"Total tokens: {pref_total_tokens}")
#     print(f"Estimated cost: ${(pref_prompt_tokens * 0.00003) + (pref_completion_tokens * 0.00006):.4f}")
#     print(f"-------------------------------------------\n")

#     raw_response = response.choices[0].message.content
    
#     try:
#         # Try to parse as JSON first (handles lowercase true/false)
#         try:
#             # Clean the response to handle potential JSON formatting issues
#             # Remove any text before the first { and after the last }
#             json_str = raw_response
#             start_idx = raw_response.find('{')
#             end_idx = raw_response.rfind('}')
            
#             if start_idx >= 0 and end_idx >= 0:
#                 json_str = raw_response[start_idx:end_idx+1]
            
#             preferences = json.loads(json_str)
#         except json.JSONDecodeError as je:
#             # If JSON parsing fails, try Python literal eval
#             # Convert lowercase true/false to Python's True/False
#             python_str = raw_response.replace('true', 'True').replace('false', 'False')
#             preferences = ast.literal_eval(python_str)
#     except (ValueError, SyntaxError) as e:
#         print(f"Error parsing preferences: {e}")
#         print(f"Raw response: {raw_response}")
#         # Fallback to using previous preferences if they exist, or empty dict if not
#         preferences = preferences if 'preferences' in locals() else {}
    
#     print(preferences)
    
#     # Filter listings based on preferences
    
#     try:
#         # Extract preferences as before
#         raw_preferences = preferences if 'preferences' in locals() else {}
        
#         # Filter listings based on preferences and get updated valid preferences
#         filtered_listings, valid_preferences = filter_listings_by_preferences(listings, raw_preferences)
        
#         # Check if any preferences were removed during filtering
#         removed_preferences = {}
#         for key, value in raw_preferences.items():
#             if key not in valid_preferences and key not in ['listing_count', 'show_listings']:
#                 removed_preferences[key] = value
        
#         # If preferences were removed, add a system message
#         if removed_preferences:
#             issue_message = "I notice that some of your preferences don't match our available listings:\n\n"
            
#             for key, value in removed_preferences.items():
#                 if key == 'maximum_rent':
#                     min_available_rent = listings['actual_rent'].min()
#                     issue_message += f"• Your budget of ${value} is below our minimum available price of ${min_available_rent}.\n"
#                 elif key == 'minimum_rent':
#                     max_available_rent = listings['actual_rent'].max()
#                     issue_message += f"• Your minimum budget of ${value} is above our maximum available price of ${max_available_rent}.\n"
#                 elif key == 'beds':
#                     available_beds = sorted(listings['beds'].unique())
#                     issue_message += f"• We don't have any {value}-bedroom apartments. Available options: {', '.join(map(str, available_beds))}.\n"
#                 elif key == 'baths':
#                     available_baths = sorted(listings['baths'].unique())
#                     issue_message += f"• We don't have any apartments with {value} bathrooms. Available options: {', '.join(map(str, available_baths))}.\n"
#                 elif key == 'borough':
#                     available_boroughs = sorted(set([val for val in listings.borough.unique() if val and len(val) > 1]))
#                     issue_message += f"• We don't have listings in {value}. Available boroughs: {', '.join(available_boroughs)}.\n"
#                 elif key == 'neighborhood':
#                     available_neighborhoods = sorted(set([val for val in listings.neighborhood.unique() if val and len(val) > 1]))
#                     issue_message += f"• We don't have listings in {value} neighborhood. Available neighborhoods: {', '.join(available_neighborhoods)}.\n"
#                 else:
#                     issue_message += f"• Your preference for {key}={value} doesn't match any available listings.\n"
            
#             issue_message += "\nPlease adjust these preferences to see available options."
#             messages.append({"role": "system", "content": issue_message})
        
#         # Update preferences with the valid ones
#         preferences = valid_preferences
        
#         # Add the number of listings to the preferences
#         preferences['listing_count'] = len(filtered_listings)
#         print(f"Number of listings matching preferences: {preferences['listing_count']}")
        
#     except Exception as e:
#         print(f"Error during filtering: {e}")
#         # Fallback to empty preferences if there's an error
#         preferences = {}
#         filtered_listings = listings.copy()
#         preferences['listing_count'] = len(filtered_listings)

#     # Update the prompt with filtered listings information
#     update_prompt = f"""
#         DataBase Info - (Here is an overview of what is currently inside the database.):
#             Minimum Beds - {filtered_listings['beds'].min() if not filtered_listings.empty else 'N/A'}
#             Maximum Beds - {filtered_listings['beds'].max() if not filtered_listings.empty else 'N/A'}

#             Minimum Baths - {filtered_listings['baths'].min() if not filtered_listings.empty else 'N/A'}
#             Maximum Baths - {filtered_listings['baths'].max() if not filtered_listings.empty else 'N/A'}

#             Minimum Price - {filtered_listings['actual_rent'].min() if not filtered_listings.empty else 'N/A'}
#             Maximum Price - {filtered_listings['actual_rent'].max() if not filtered_listings.empty else 'N/A'}

#             Applicant Types - {', '.join(sorted(set([val for val in filtered_listings.applicance_type.unique() if val and len(val) > 1]))) if not filtered_listings.empty else 'N/A'}

#             Boroughs - {', '.join(sorted(set([val for val in filtered_listings.borough.unique() if val and len(val) > 1]))) if not filtered_listings.empty else 'N/A'}
#             Neighborhoods - {', '.join(sorted(set([val for val in filtered_listings.neighborhood.unique() if val and len(val) > 1]))) if not filtered_listings.empty else 'N/A'}

#             Amenities - {', '.join(sorted(set(item for sublist in filtered_listings.building_amenities if isinstance(sublist, list) for item in sublist)))}

#             Exposures - {', '.join(sorted(set([val for val in filtered_listings.exposure.apply(lambda x: x.strip() if isinstance(x, str) else '').unique() if val and len(val) > 1]))) if not filtered_listings.empty else 'N/A'}
            
#             # Boolean Features
#             Doorman - {all(filtered_listings['doorman']) if not filtered_listings.empty and 'doorman' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['doorman']) if not filtered_listings.empty and 'doorman' in filtered_listings.columns else 'N/A'} (Some)
#             Elevator - {all(filtered_listings['elevator']) if not filtered_listings.empty and 'elevator' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['elevator']) if not filtered_listings.empty and 'elevator' in filtered_listings.columns else 'N/A'} (Some)
#             Wheelchair Access - {all(filtered_listings['wheelchair_access']) if not filtered_listings.empty and 'wheelchair_access' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['wheelchair_access']) if not filtered_listings.empty and 'wheelchair_access' in filtered_listings.columns else 'N/A'} (Some)
#             Smoke Free - {all(filtered_listings['smoke_free']) if not filtered_listings.empty and 'smoke_free' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['smoke_free']) if not filtered_listings.empty and 'smoke_free' in filtered_listings.columns else 'N/A'} (Some)
#             Laundry in Building - {all(filtered_listings['laundry_in_building']) if not filtered_listings.empty and 'laundry_in_building' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['laundry_in_building']) if not filtered_listings.empty and 'laundry_in_building' in filtered_listings.columns else 'N/A'} (Some)
#             Laundry in Unit - {all(filtered_listings['laundry_in_unit']) if not filtered_listings.empty and 'laundry_in_unit' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['laundry_in_unit']) if not filtered_listings.empty and 'laundry_in_unit' in filtered_listings.columns else 'N/A'} (Some)
#             Pet Friendly - {all(filtered_listings['pet_friendly']) if not filtered_listings.empty and 'pet_friendly' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['pet_friendly']) if not filtered_listings.empty and 'pet_friendly' in filtered_listings.columns else 'N/A'} (Some)
#             Live-in Super - {all(filtered_listings['live_in_super']) if not filtered_listings.empty and 'live_in_super' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['live_in_super']) if not filtered_listings.empty and 'live_in_super' in filtered_listings.columns else 'N/A'} (Some)
#             Concierge - {all(filtered_listings['concierge']) if not filtered_listings.empty and 'concierge' in filtered_listings.columns else 'N/A'} (All), {any(filtered_listings['concierge']) if not filtered_listings.empty and 'concierge' in filtered_listings.columns else 'N/A'} (Some)
            
#             Number of listings: {len(filtered_listings) if not filtered_listings.empty else 0}"""
    
#     messages.append({"role": 'assistant', "content": f"Updated DataBase Info based on current preferences. {update_prompt}"})