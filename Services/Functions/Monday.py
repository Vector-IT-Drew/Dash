from monday import MondayClient
import pandas as pd
import numpy as np
import re
import ast
import json
import os
from dotenv import load_dotenv
from Services.Database.Connect import get_db_connection

load_dotenv()

def get_monday_client():
	token = os.getenv("MONDAY_API_TOKEN")
	if not token:
		raise ValueError("MONDAY_API_TOKEN environment variable is not set")
	
	client = MondayClient(
		token=token,
	)
	return client

def get_board_schema(board_id, client):

	board = client.boards.fetch_boards_by_id( board_id)['data']['boards'][0]
	board_id = board.pop('id')
	board_name = board.pop('name')
	board_perms = board.pop('permissions')
	board_tags = board.pop('tags')
	board_groups = board.pop('groups')

	board_data = pd.DataFrame(board['columns'])
	
	return board_data


def get_data(board_id, client):
    

    # Get all the items on the board and put into items_list (2d array)
    mapping = {val[1]:val[0] for val in get_board_schema(board_id, client)[['id', 'title']].values}
    mapping = {'Unit': mapping.pop('Name'), **mapping}

    items_list = []

    cursor = None

    while True:
        # Construct the GraphQL query with cursor for pagination
        query = f"""
        {{
            boards(ids: {board_id}) {{
                items_page(limit: 500, {f'cursor: "{cursor}"' if cursor else ''}) {{
                    cursor
                    items {{
                        id
                        name
                        column_values {{
                            ... on MirrorValue {{
                                column {{
                                    title
                                }}
                                text
                                value
                                display_value
                            }}
                            column {{
                                title
                            }}
                            text
                            value
                        }}
                    }}
                }}
            }}
        }}
        """

        # Execute the GraphQL query
        data = client.custom.execute_custom_query(query)

        # Extract items and cursor from the response
        items = data['data']['boards'][0]['items_page']['items']
        cursor = data['data']['boards'][0]['items_page']['cursor']

        # Append items to items_list
        for item in items:
            unit = item['name']
            item_row = [unit]
            for col in item['column_values']:
                if 'display_value' in col.keys():
                    item_row.append(col['display_value'])
                else:
                    item_row.append(col['text'])
            items_list.append(item_row)

        # If there is no more data to fetch, break the loop
        if not cursor:
            break
    df = pd.DataFrame(items_list, columns = list(mapping.keys()))
    return df

def safe_date(val):
	try:
		return val.to_pydatetime() if pd.notna(val) else None
	except:
		return None


def update_deals_from_monday():

	print("Updating deals from Monday...")

	client = get_monday_client()
	
	lux_df = get_data(6019954751, client)
	leg_df = get_data(5089134066, client)
	df = pd.concat([lux_df, leg_df])
	df['Move Out'] = pd.to_datetime(df['Move Out'])
	df['Expiry'] = pd.to_datetime(df['Expiry'])
	df['Lease Start'] = pd.to_datetime(df['Lease Start'])
	df['Move Out'] = df['Move Out'].combine_first(df['Move-Out'])

	db_result = get_db_connection()
	if db_result["status"] != "connected":
		raise Exception("Failed to connect to database: " + db_result.get("message", "Unknown error"))
	connection = db_result["connection"]
	cursor = connection.cursor(dictionary=True)

	# Get units
	cursor.execute("SELECT * FROM units")
	units = cursor.fetchall()
	unit_df = pd.DataFrame(units)[['unit_id', 'address', 'unit']]

	total_uploaded = 0

	for idx, row in df.iterrows():
		# Get unit_id
		try:
			unit_id = unit_df[(unit_df.address == row['Address']) & (unit_df.unit == row['Unit'])].unit_id.values[0]
		except Exception:
			continue

		# Clean up and prepare values
		row['Term'] = row.get('Term') or 0
		if not row.get('Actual Rent'):
			continue

		for col in ['Move Out', 'Expiry', 'Lease Start']:
			val = row.get(col)
			if str(val) == 'NaT':
				row[col] = None
			elif pd.notna(val):
				row[col] = val.to_pydatetime()
			else:
				row[col] = None

		try:
			row['Conc'] = float(row.get('Conc', 0) or 0)
		except Exception:
			row['Conc'] = 0

		row['Gross'] = row.get('Gross') or 0

		# Find the most recent deal for this unit
		cursor.execute(
			"SELECT * FROM deals WHERE unit_id = %s ORDER BY created_at DESC LIMIT 1",
			(unit_id,)
		)
		existing_deal = cursor.fetchone()

		# Prepare values for insert/update
		deal_values = (
			str(unit_id), '{}', '{}', row['Status'], row['Lease Start'], row['Term'], row['Expiry'],
			row['Conc'], row['Gross'], row['Actual Rent'], -1, -1, pd.Timestamp.today().to_pydatetime(), row['Move Out'], row['Lease Type']
		)

		if existing_deal:
			db_status = existing_deal.get('deal_status', '')
			new_status = row['Status'] or ''
			if db_status == "Occupied" and "Active" in new_status:
				# Insert a new deal
				cursor.execute("""
					INSERT INTO deals (unit_id, tenant_ids, guarantor_ids, deal_status, start_date, 
						term, expiry, concession, gross, actual_rent, agent_id, manager_id, created_at, move_out, lease_type)
					VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
				""", deal_values)
				total_uploaded += 1
				print(f"Inserted new deal for unit_id {unit_id} due to status transition Occupied -> Active")
			else:
				# Update the existing deal
				cursor.execute("""
					UPDATE deals SET
						tenant_ids=%s, guarantor_ids=%s, deal_status=%s, start_date=%s, term=%s, expiry=%s,
						concession=%s, gross=%s, actual_rent=%s, agent_id=%s, manager_id=%s, created_at=%s, move_out=%s, lease_type=%s
					WHERE deal_id=%s
				""", (
					'{}', '{}', row['Status'], row['Lease Start'], row['Term'], row['Expiry'],
					row['Conc'], row['Gross'], row['Actual Rent'], -1, -1, pd.Timestamp.today().to_pydatetime(), row['Move Out'], row['Lease Type'],
					existing_deal['deal_id']
				))
				print(f"Updated deal_id {existing_deal['deal_id']} for unit_id {unit_id}")
		else:
			# No deal exists, insert new
			cursor.execute("""
				INSERT INTO deals (unit_id, tenant_ids, guarantor_ids, deal_status, start_date, 
					term, expiry, concession, gross, actual_rent, agent_id, manager_id, created_at, move_out, lease_type)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
			""", deal_values)
			total_uploaded += 1
			print(f"Inserted new deal for unit_id {unit_id}")

		if total_uploaded % 100 == 1:
			print(f"\nUploaded {total_uploaded} deals so far...")

	connection.commit()
	cursor.close()
	connection.close()
	print(f"✅ Done! {total_uploaded} deals uploaded or updated successfully.")

if __name__ == "__main__":
	update_deals_from_monday()
