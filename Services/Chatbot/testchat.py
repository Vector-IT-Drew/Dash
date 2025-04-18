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

try:
    # Send message to external API
    api_url = "https://dash-production-b25c.up.railway.app/chat"
    print(f"Calling API: {api_url} with payload: {payload}")
    
    # Add timeout to prevent hanging
    response = api_session.post(api_url, json=payload, timeout=10)
    print(f"API response status: {response.status_code}")
    print(f"API response content: {response.text[:200]}...")  # Print first 200 chars
    
    api_data = response.json()
    # ...
except Exception as e:
    # Log detailed error
    import traceback
    print(e)