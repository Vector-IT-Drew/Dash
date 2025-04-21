from monday import MondayClient
import pandas as pd
import numpy as np
import re
import ast
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_monday_client():
	token = os.getenv("MONDAY_API_TOKEN")
	if not token:
		raise ValueError("MONDAY_API_TOKEN environment variable is not set")
	
	client = MondayClient(
		token=token,
		headers={'API-Version': '2023-10'}
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
