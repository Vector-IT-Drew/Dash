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


url = "https://api-internal.streeteasy.com/graphql"

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://streeteasy.com",
    "referer": "https://streeteasy.com/",
    "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
}

# Include all cookies as a single string
cookies = {
    "ezab_per_luxury_concierge_ab_test": "control",
    "_scor_uid": "1b4d646fa7014b7b96720d7cdbc14427",
    "zjs_user_id": "null",
    "zg_anonymous_id": '"277320f1-ec16-42f5-bf89-f7bd80fe812c"',
    "_fbp": "fb.1.1736274993484.325797811278258247",
    "zjs_anonymous_id": '"de531356-4b29-4367-ab4a-bcd6fb59b799"',
    "pxcts": "51d7952f-cd26-11ef-bdbf-7b5f98546e95",
    "_pxvid": "51d785bc-cd26-11ef-bdbf-b2912e244221",
    "_se_t": "63ca59e0-1dea-4d6c-87c4-beb5d08bfc34",
    "_ga": "GA1.2.1877643351.1736289886",
    "_ga_L14MFV0VR8": "GS1.2.1736289886.1.0.1736289886.60.0.0",
    "_pin_unauth": "dWlkPVkyWmxZamszTkdNdFpqVmxOeTAwT1RZd0xXSXpOalV0WldRMVlUWTRNVFUwTUdGag",
    "_tt_enable_cookie": "1",
    "_ttp": "01JPKBD39MZJDWMEJNWCFHYW2N_.tt.1",
    "_actor": "eyJpZCI6ImMvSXE3a3ZzdytTYS9DK1dnS0g5UUE9PSJ9--1b9dea00993daa9b3da200a9d3d402e77a32f090",
    "se_lsa": "2025-03-25+11%3A28%3A13+-0400",
    "_gcl_au": "1.1.253822218.1744134182",
    "zjs_user_id_type": '"encoded_zuid"',
    "srp": "v2",
    "__gads": "ID=a094a082e40656e7:T=1736275207:RT=1748884722:S=ALNI_MaVCUva9u5O3eG7VaGduUP1fQzKfw",
    "__gpi": "UID=00000fcbe54756b2:T=1736275207:RT=1748884722:S=ALNI_Mb8eH4seWm67-WdUcgF9rn46XBmVw",
    "__eoi": "ID=ac6cb4d8ad15bb80:T=1736275207:RT=1748884722:S=AA-AfjaJvUkkqLI1CbnkMnbuF9gT",
    "tfpsi": "aa358fb4-233b-43a5-97fc-cb6a45d5d82a",
    "_rdt_uuid": "1736274993442.516d15db-e1cb-40dc-9245-4e2f5c9b4a0a",
    "ttcsid": "1748960202183::AnoKsY65Rde4_xwNRu4U.10.1748960753170",
    "_ses": "...",  # Truncated for brevity, include full value from browser
    "_px3": "ad73e00831edbf96b4a399020556411291ea8a58bc9b757de1d1d5f415a58b8f:..."
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

    for agent_id in agent_ids:
        page = 1
        has_next = True
        time.sleep(np.random.randint(1,4))
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
                break
            
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                break

            try:
                items = data['data']['agent_active_listings_paginated']['items']
                page_info = data['data']['agent_active_listings_paginated']['page_info']
            except KeyError:
                break

            ids_this_page = [item['id'] for item in items]
            all_ids.extend(ids_this_page)

            has_next = page_info['has_next_page']
            page += 1
            time.sleep(np.random.randint(13, 25))

    print(f"📊 Total IDs collected: {len(all_ids)}")

    final_df = pd.DataFrame()
    grouped_ids = [all_ids[i:i + 100] for i in range(0, len(all_ids), 100)]
    
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
            continue
            
        try:
            batch_data = response.json()
            final_df = pd.concat([final_df, pd.DataFrame(batch_data['data']['rentals'])])
            print(f"📈 Retrieved data for batch {i+1}/{len(grouped_ids)} ({len(final_df)} total records)")
        except Exception as e:
            continue
            
        time.sleep(np.random.choice([15, 16,19,17]))
        
    final_df.to_csv('Streeteasy Data.csv')
    print(f"💾 Saved {len(final_df)} records to Streeteasy Data.csv")
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

if __name__ == "__main__":
    save_to_db()