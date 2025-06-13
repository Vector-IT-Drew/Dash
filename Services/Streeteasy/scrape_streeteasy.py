import requests
import time
import numpy as np
import pandas as pd
import json
import re
import ast
from Services.Database.Connect import get_db_connection
from Services.Database.Data import run_query_system
import os
import threading
import queue


url = "https://api-internal.streeteasy.com/graphql"
cookies = {
    '_actor': 'eyJpZCI6IkdGd3E5R09hZXFMeE95dzMwY1ExZ1E9PSJ9--4ac1bcd3d25ac5f9ce9277186c92865e7ff35cb0',
    '_se_t': '1548e115-aa92-4424-b6ab-4396acfe2a0f',
    'g_state': '',
    '_gcl_gs': '2.1.k1$i1749595731$u199244780',
    '_gcl_au': '1.1.1131355835.1749595733',
    '_scor_uid': '05c141023c6e4269ab78539945011601',
    'zjs_user_id': 'null',
    'zg_anonymous_id': '%223edcd40f-9bc0-4339-9704-fe1a55687a04%22',
    '__spdt': 'c03001b703d34d29873ac4690ddc3c9a',
    'pxcts': '151b8c09-464d-11f0-89b0-a13f0a3c11fa',
    '_pxvid': '151b7e65-464d-11f0-89b0-278381b0eb6f',
    '_fbp': 'fb.1.1749595733047.732932663604136124',
    '_tt_enable_cookie': '1',
    '_ttp': '01JXE0GF2HEY0RZHVQTVKJ7JFA_.tt.1',
    'zjs_anonymous_id': '%221548e115-aa92-4424-b6ab-4396acfe2a0f%22',
    'zjs_user_id_type': '%22encoded_zuid%22',
    'google_one_tap': '0',
    'se%3Asearch%3Ashared%3Astate': '112||||',
    'srpUserId': 'ead1ff10-7c4e-4ae2-adfe-a9cadec3cd11',
    'rentalsSort': 'se_score',
    '_gcl_aw': 'GCL.1749595741.EAIaIQobChMIodjt__fnjQMVMq9aBR2B5xHGEAAYASAAEgL1VfD_BwE',
    '_pin_unauth': 'dWlkPU1HWTBORGcyTWpZdE9EVmxaUzAwTWprMkxUazFORFV0TkdOak5HVXdNalpqT1dObA',
    'windowHeight': '889',
    'last_search_tab': 'rentals',
    'tfpsi': 'ef92e8f7-e654-474f-be21-a8a740e15c87',
    'srp': 'v2',
    '__gads': 'ID=5c9c46eda44e60ce:T=1749595740:RT=1749826974:S=ALNI_MZFIdNz-5MzN8om5bWD2WgJeZLULA',
    '__gpi': 'UID=00001026887eea21:T=1749595740:RT=1749826974:S=ALNI_MZMP5iU-O4cpVtXKUqWmK9cet_EbQ',
    '__eoi': 'ID=db7563ae1491c22a:T=1749595740:RT=1749826974:S=AA-AfjaITNKSo-tmsgTHFWjZf1z2',
    'zjs_utmcampaign': '%22rental_listing%22',
    'zjs_utmsource': '%22web%22',
    'zjs_utmmedium': '%22share%22',
    'anon_searcher_stage': 'initial',
    'tracked_search': '',
    'windowWidth': '1119',
    'se_lsa': '2025-06-13+11%3A04%3A57+-0400',
    '_ses': 'U1pRQldCQXRFV00vSlFiNzBlME1wQU9XTGlKYjVlMkFBbXlKU05ENG02cnZENXU1WlF2Z0tkdkVJTUxZd3k0TkhEblVWVHNXajlqUndkZXBMdjNqOXVXazVScUlwWS9maUg1NDJQbEtZbXcveUpPQWlTaUlEd042VXlNaUV3eW05ZzE2dVJjNHR5eW1lVWFjUHBEUVNYcVNiVDc5cEp3NUZoWm1KMW0xMFMwPS0tVU5jYVJmWitRNEUvY2lrQnhuZW1oUT09--4a15e86cb89c2e58ba25f381110b70a393d26c07',
    '_rdt_uuid': '1749595733014.3ca0a4c7-ce0c-49ce-a2e3-91740dcd03e9',
    'ttcsid': '1749826967823::T4pGuk8RyHDaudd9U0DO.4.1749827099946',
    '_px3': 'e4f19caf776fd0b92d2344b0ab570061bac23a6455fc7d77ccaa2cf9d54e85f9:XPfLSqp+RROjEfRFaLf7L/VeQkLMTSi1GDYy2n6FOWQXEmt1bm2VESTiOzLZgCvm7V9vb6JU1IpME4yoAdSLbw==:1000:F87+xMNtVbYfl84xHF/jh54CtNSh+d8nrJxQoJ1Kyw7L5BoVrdZg9JwRrenSycc5vQyueTXqKyZn83lzPaABc1hhZ+1AQxy1TeDy8SDN5Xipc9mxC12c2+Mobsv8zHz0kdBAqhts7dGoDXmJbsTaGrMhNH/HTaqiqpy5Z8f+KqfTKMNWcr14EpvT4e9lOB/hexfobhDSoim1RfoTHBWHW1HvJuZt1hGWkWGyTUTPEWI=',
    'ttcsid_CV2U7URC77UD0HO3EE50': '1749826967822::qB5_o3ogD3in1BB_x7hk.4.1749827110117',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://streeteasy.com',
    'priority': 'u=1, i',
    'referer': 'https://streeteasy.com/',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    'x-csrf-token': 'CQSyCUKPGvE7bZvSDr0KrtWM2xTDfZ20vrYlSN1fQjDrsNzonQcqKaVYlRq5Bhl0SQZimXei7m8krkMwkKEBbw',
    'x-requested-with': 'XMLHttpRequest',
    # 'cookie': '_actor=eyJpZCI6IkdGd3E5R09hZXFMeE95dzMwY1ExZ1E9PSJ9--4ac1bcd3d25ac5f9ce9277186c92865e7ff35cb0; _se_t=1548e115-aa92-4424-b6ab-4396acfe2a0f; g_state=; _gcl_gs=2.1.k1$i1749595731$u199244780; _gcl_au=1.1.1131355835.1749595733; _scor_uid=05c141023c6e4269ab78539945011601; zjs_user_id=null; zg_anonymous_id=%223edcd40f-9bc0-4339-9704-fe1a55687a04%22; __spdt=c03001b703d34d29873ac4690ddc3c9a; pxcts=151b8c09-464d-11f0-89b0-a13f0a3c11fa; _pxvid=151b7e65-464d-11f0-89b0-278381b0eb6f; _fbp=fb.1.1749595733047.732932663604136124; _tt_enable_cookie=1; _ttp=01JXE0GF2HEY0RZHVQTVKJ7JFA_.tt.1; zjs_anonymous_id=%221548e115-aa92-4424-b6ab-4396acfe2a0f%22; zjs_user_id_type=%22encoded_zuid%22; google_one_tap=0; se%3Asearch%3Ashared%3Astate=112||||; srpUserId=ead1ff10-7c4e-4ae2-adfe-a9cadec3cd11; rentalsSort=se_score; _gcl_aw=GCL.1749595741.EAIaIQobChMIodjt__fnjQMVMq9aBR2B5xHGEAAYASAAEgL1VfD_BwE; _pin_unauth=dWlkPU1HWTBORGcyTWpZdE9EVmxaUzAwTWprMkxUazFORFV0TkdOak5HVXdNalpqT1dObA; windowHeight=889; last_search_tab=rentals; tfpsi=ef92e8f7-e654-474f-be21-a8a740e15c87; srp=v2; __gads=ID=5c9c46eda44e60ce:T=1749595740:RT=1749826974:S=ALNI_MZFIdNz-5MzN8om5bWD2WgJeZLULA; __gpi=UID=00001026887eea21:T=1749595740:RT=1749826974:S=ALNI_MZMP5iU-O4cpVtXKUqWmK9cet_EbQ; __eoi=ID=db7563ae1491c22a:T=1749595740:RT=1749826974:S=AA-AfjaITNKSo-tmsgTHFWjZf1z2; zjs_utmcampaign=%22rental_listing%22; zjs_utmsource=%22web%22; zjs_utmmedium=%22share%22; anon_searcher_stage=initial; tracked_search=; windowWidth=1119; se_lsa=2025-06-13+11%3A04%3A57+-0400; _ses=U1pRQldCQXRFV00vSlFiNzBlME1wQU9XTGlKYjVlMkFBbXlKU05ENG02cnZENXU1WlF2Z0tkdkVJTUxZd3k0TkhEblVWVHNXajlqUndkZXBMdjNqOXVXazVScUlwWS9maUg1NDJQbEtZbXcveUpPQWlTaUlEd042VXlNaUV3eW05ZzE2dVJjNHR5eW1lVWFjUHBEUVNYcVNiVDc5cEp3NUZoWm1KMW0xMFMwPS0tVU5jYVJmWitRNEUvY2lrQnhuZW1oUT09--4a15e86cb89c2e58ba25f381110b70a393d26c07; _rdt_uuid=1749595733014.3ca0a4c7-ce0c-49ce-a2e3-91740dcd03e9; ttcsid=1749826967823::T4pGuk8RyHDaudd9U0DO.4.1749827099946; _px3=e4f19caf776fd0b92d2344b0ab570061bac23a6455fc7d77ccaa2cf9d54e85f9:XPfLSqp+RROjEfRFaLf7L/VeQkLMTSi1GDYy2n6FOWQXEmt1bm2VESTiOzLZgCvm7V9vb6JU1IpME4yoAdSLbw==:1000:F87+xMNtVbYfl84xHF/jh54CtNSh+d8nrJxQoJ1Kyw7L5BoVrdZg9JwRrenSycc5vQyueTXqKyZn83lzPaABc1hhZ+1AQxy1TeDy8SDN5Xipc9mxC12c2+Mobsv8zHz0kdBAqhts7dGoDXmJbsTaGrMhNH/HTaqiqpy5Z8f+KqfTKMNWcr14EpvT4e9lOB/hexfobhDSoim1RfoTHBWHW1HvJuZt1hGWkWGyTUTPEWI=; ttcsid_CV2U7URC77UD0HO3EE50=1749826967822::qB5_o3ogD3in1BB_x7hk.4.1749827110117',
}

# Get column names and types from the database
def get_db_columns_and_types(db_connection, table_name):
    with db_connection.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()

    db_columns = {}
    for column in columns:
        column_name = column[0]
        column_type = column[1]
        if isinstance(column_type, bytes):
            column_type = column_type.decode()  # fix bytes to str
        db_columns[column_name] = column_type
    return db_columns


def convert_column_types(df, db_columns):
    for column in df.columns:
        if column in db_columns:
            db_type = db_columns[column]
            if isinstance(db_type, bytes):
                db_type = db_type.decode()

            if 'tinyint' in db_type:
                pass
            elif 'int' in db_type:
                df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')
            elif 'float' in db_type or 'double' in db_type or 'decimal' in db_type:
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif 'datetime' in db_type:
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif 'date' in db_type:
                df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
            elif 'json' in db_type:
                df[column] = df[column]
            else:
                df[column] = df[column].astype(str)
    return df

# Drop columns not in DB table
def filter_csv_columns(df, db_columns):
    valid_columns = [col for col in df.columns if col in db_columns]
    return df[valid_columns]

def clean_list_column(df, column_name):
    def convert_to_json_list(val):
        if pd.isna(val) or str(val).strip() == "":
            return json.dumps([])  # empty list as JSON string
        # Replace smart quotes and remove brackets
        val = str(val).replace("'","'").replace("[", "").replace("]", "")
        # Split by comma and strip whitespace
        items = [item.strip() for item in val.split(",") if item.strip()]
        return json.dumps(items)

    df[column_name] = df[column_name].apply(convert_to_json_list)
    return df


def scrape_streeteasy():    
    agents = ['lunzer', '685']
    agent_ids = [348933, 348649, 365551, 348650, 348652, 360856, 360857, 360860, 369098]
    all_ids = []
    agent_totals = {}  # Track totals per agent

    print("\n🔍 Starting StreetEasy scrape...")
    print("━" * 50)

    for agent_id in agent_ids:
        print(f"\n📊 Agent {agent_id}")
        print("─" * 30)
        page = 1
        has_next = True
        agent_ids = []  # Track IDs for this agent
        time.sleep(np.random.randint(6,12))
        
        while has_next:
            payload = {
                "operationName": "getPaginatedListings",
                "variables": {
                    "id": agent_id,
                    "listingType": "rental",
                    "page": page
                },
                "query": """
                    query getPaginatedListings($id: ID!, $listingType: String, $page: Int) {
                    agent_active_listings_paginated(input: {id: $id, listing_type: $listingType, page: $page}) {
                        items {
                        id
                        }
                        page_info {
                        current_page
                        total_pages
                        has_next_page
                        }
                    }
                    }
                """
            }

            response = requests.post(url, headers=headers, cookies=cookies, json=payload)
            
            if response.status_code != 200:
                print(f"❌ Page {page}: Failed with status {response.status_code}")
                break
            
            try:
                data = response.json()
                items = data['data']['agent_active_listings_paginated']['items']
                page_info = data['data']['agent_active_listings_paginated']['page_info']
                
                ids_this_page = [item['id'] for item in items]
                agent_ids.extend(ids_this_page)
                
                print(f"✓ Page {page}/{page_info['total_pages']}: {len(ids_this_page)} listings")
                
                has_next = page_info['has_next_page']
                page += 1
                time.sleep(np.random.choice([15, 16,19,25]))
                
            except (KeyError, requests.exceptions.JSONDecodeError) as e:
                print(f"❌ Page {page}: Error processing data")
                break

        agent_totals[agent_id] = len(agent_ids)
        all_ids.extend(agent_ids)
        print(f"📈 Agent {agent_id}: {len(agent_ids)} total listings")

    print("\n" + "━" * 50)
    print("📊 Collection Summary:")
    for agent_id, count in agent_totals.items():
        print(f"  • Agent {agent_id}: {count} listings")
    print(f"📈 Total listings collected: {len(all_ids)}")
    print("━" * 50 + "\n")

    final_df = pd.DataFrame()
    grouped_ids = [all_ids[i:i + 100] for i in range(0, len(all_ids), 100)]
    
    print("🔄 Fetching detailed listing data...")
    print("━" * 50)
    
    for i, rental_ids in enumerate(grouped_ids):
        json_data = {
            'operationName': 'Highlights',
            'variables': {
                'listing_ids': [int(val) for val in rental_ids],  
            },
            'query': '''query Highlights($listing_ids: [ID!]!) {
                rentals(ids: $listing_ids) {
                    price_history { date description price }
                    address   { pretty_address unit }
                    amenities { name }
                    agents    { name email }
                    source
                    id
                    created_at
                    listed_at
                    description
                    listed_price
                    days_on_market
                    dynamic_insight
                    size_sqft

                    views_count
                
                    leads_count
                    saves_count
                    shares_count

                    status

                    __typename
                    bathrooms
                    bedrooms

                    comparable_listings { id address { pretty_address } bedrooms bathrooms listed_price}
                    interesting_changes { type value when }
                    has_historical_activity
                    anyrooms
                    featured_details {
                        clicks
                        id
                        ends_at
                        location
                        is_homepage_featured_listing
                    }
                    images (max_count: 10) {
                        url
                    }
                    listing_traffics (days: 1000) {
                        date featured_impressions id search_impressions views
                    }
                    concessions{
                        free_months
                        lease_term
                    }
                    floorplans{
                    url
                    }
                    area{
                        
                        name
                    
                    }
                    building{
                        active_listings_count 
                        active_rentals_count
                        front_lat
                        front_lon
                        amenities{
                            name
                        }
                        building_class_description
                        building_classification
                        building_type
                        floor_count

                        title
                        year_built
                    }
                    actual_is_collect_your_own_fee
                    is_no_fee
                    
                }
            }''',
        }

        response = requests.post('https://api-internal.streeteasy.com/graphql', cookies=cookies, headers=headers, json=json_data)
        
        if response.status_code != 200:
            print(f"❌ Batch {i+1}/{len(grouped_ids)}: Failed with status {response.status_code}")
            continue
            
        try:
            batch_data = response.json()
            batch_df = pd.DataFrame(batch_data['data']['rentals'])
            final_df = pd.concat([final_df, batch_df])
            print(f"✓ Batch {i+1}/{len(grouped_ids)}: {len(batch_df)} listings processed")
        except Exception as e:
            print(f"❌ Batch {i+1}/{len(grouped_ids)}: Error processing data")
            continue
            
        time.sleep(np.random.choice([15, 16,19,25]))
        
    print("\n" + "━" * 50)
    print(f"💾 Final dataset: {len(final_df)} listings")
    print("━" * 50 + "\n")
    
    final_df.to_csv('Streeteasy Data.csv')
    return final_df

def fix_json_column(value):
    try:
        # Already valid Python object
        if isinstance(value, (list, dict)):
            return json.dumps(value)

        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None

            # Replace smart quotes and standardize
            value = value.replace("'","'").replace('"', '"')

            # Try converting single-quote JSON to double-quote
            value = re.sub(r"'", '"', value)

            # Remove trailing commas inside objects/arrays
            value = re.sub(r",\s*([\]}])", r"\1", value)

            # Try loading with json first
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                # Fall back to using ast.literal_eval which can handle single quotes etc.
                parsed = ast.literal_eval(value)

            return json.dumps(parsed)

    except Exception as e:
        print(f"⚠️ JSON error for value: {value} — {e}")
        return None

    return value


def eval_json(val):
    try:
        return json.dumps(ast.literal_eval(val))
    except:
        return None

def get_unit_df(db_connection):
    """Get units - fixed to return the dataframe"""
    # Create a cursor
    cursor = db_connection.cursor(dictionary=True)

    # Execute the query
    query = "SELECT * FROM units"
    cursor.execute(query)

    # Fetch all results
    units = cursor.fetchall()

    # Close cursor (but don't close connection here since we need it later)
    cursor.close()

    print(f"Retrieved {len(units)} units from database")

    unit_df = pd.DataFrame(units)[['unit_id', 'address', 'unit']]
    return unit_df  # This was missing!

def save_to_db():
    """Main function - processes StreetEasy data directly without CSV intermediary"""
    
    # Get database connection
    db_result = get_db_connection()
    if db_result["status"] != "connected":
        print("❌ Database connection failed")
        return
    
    db_connection = db_result["connection"]
    
    # Get units
    unit_df = get_unit_df(db_connection)
    
    cursor = db_connection.cursor()

    print("🔄 Starting StreetEasy data scrape...")
    final_df = scrape_streeteasy()
    
    if len(final_df) == 0:
        print("❌ No data to process")
        cursor.close()
        db_connection.close()
        return

    # Process the data
    print("🔧 Processing data...")
    addresses = []
    units = []
    
    for idx, row in final_df.iterrows():
        try:
            address_str = str(row['address'])
            if 'pretty_address' in address_str:
                pretty_address_match = re.search(r"'pretty_address': '([^']*)'", address_str)
                unit_match = re.search(r"'unit': '([^']*)'", address_str)
                
                if pretty_address_match:
                    addresses.append(pretty_address_match.group(1))
                else:
                    addresses.append(None)
                    
                if unit_match:
                    unit_val = unit_match.group(1).replace('#', '').strip()
                    units.append(unit_val)
                else:
                    units.append(None)
            else:
                addresses.append(None)
                units.append(None)
        except Exception as e:
            addresses.append(None)
            units.append(None)
    
    final_df['address'] = addresses
    final_df['unit'] = units

    # Format unit column for matching
    unit_df_formatted = unit_df.copy()
    unit_df_formatted['unit'] = unit_df_formatted['unit'].str.lstrip('0').str.strip()

    # Merge with unit data
    final_df = final_df.merge(
        unit_df_formatted, 
        how='left', 
        on=['address', 'unit']
    ).dropna(subset=['address'])

    if len(final_df) == 0:
        print("❌ No matching units found")
        cursor.close()
        db_connection.close()
        return

    # Process listed_price
    if 'listed_price' in final_df.columns:
        final_df['listed_price'] = final_df['listed_price'].astype(str).str.replace('$','').str.replace(',','')

    # Get database schema and filter columns
    db_columns = get_db_columns_and_types(db_connection, 'streeteasy_units')
    final_df = filter_csv_columns(final_df, db_columns)
    final_df = convert_column_types(final_df, db_columns)

    # Process price_history if exists
    if 'price_history' in final_df.columns:
        final_df['price_history'] = final_df['price_history'].apply(eval_json)

    # Select only available columns for insertion
    expected_columns = ['address', 'unit', 'amenities', 'building_amenities', 'source', 'id', 'created_at', 'listed_at', 'price_history',
        'description', 'listed_price', 'days_on_market', 'size_sqft', 'views_count', 'leads_count', 'saves_count',
        'shares_count', 'status', 'bathrooms', 'bedrooms', 'building', 'free_months', 'lease_term', 
        'total_featured_impressions', 'net_rent', 'is_vector', 'ctr', 'areaName', 'longitude',
        'latitude', 'calc_dom', 'is_no_fee', 'unit_id']
    
    available_columns = [col for col in expected_columns if col in final_df.columns]
    final_df = final_df[available_columns]

    # Build insert query
    columns = ', '.join(final_df.columns)
    placeholders = ', '.join(['%s'] * len(final_df.columns))
    query = f"INSERT INTO streeteasy_units ({columns}) VALUES ({placeholders})"

    # Prepare data for insertion
    data_to_insert = []
    for _, row in final_df.iterrows():
        row_data = []
        for col in final_df.columns:
            value = row[col]
            if pd.isna(value):
                row_data.append(None)
            else:
                row_data.append(value)
        data_to_insert.append(tuple(row_data))

    print(f"⬆️ Uploading {len(data_to_insert)} records to database...")
    cursor.executemany(query, data_to_insert)
    db_connection.commit()
    
    print(f"✅ Successfully uploaded {len(data_to_insert)} records to streeteasy_units table")
    
    cursor.close()
    db_connection.close()

# Global status queue for communication between threads
status_queue = queue.Queue()

def scrape_with_status():
    """Run the scraper and update status in the queue"""
    try:
        status_queue.put({"status": "started", "message": "Scraping started", "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')})
        save_to_db()
        status_queue.put({"status": "completed", "message": "Scraping completed successfully", "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')})
    except Exception as e:
        status_queue.put({"status": "error", "message": str(e), "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')})

def start_scrape():
    """Start the scraper in a background thread and return immediately"""
    # Clear any old status
    while not status_queue.empty():
        status_queue.get()
    
    # Start the scraper in a background thread
    thread = threading.Thread(target=scrape_with_status)
    thread.daemon = True  # Thread will exit when main program exits
    thread.start()
    
    # Return initial status
    return {
        "status": "started",
        "message": "Scraping started in background",
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }

def get_scrape_status():
    """Get the current status of the scraping process"""
    try:
        # Get the latest status without blocking
        status = status_queue.get_nowait()
        return status
    except queue.Empty:
        # If no status update, return running status
        return {
            "status": "running",
            "message": "Scraping in progress",
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }

if __name__ == "__main__":
    # For direct script execution, run normally
    save_to_db()