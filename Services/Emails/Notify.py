import os
import mysql.connector
from flask import Blueprint, Flask, jsonify, request
import logging
from mysql.connector import Error
import decimal
from datetime import datetime
from Services.Functions.Gmail import send_email
import json

# Send out an email from the portfolio_email address,to a selected address
notify_bp = Blueprint('Notify', __name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@notify_bp.route('/notify', methods=['GET', 'POST'])
def notify():
    # Handle both GET parameters and POST JSON data
    if request.method == 'GET':
        # Get parameters from URL query string
        sender = request.args.get('sender')
        recipients_str = request.args.get('recipients')
        cc_str = request.args.get('cc')
        subject = request.args.get('subject')
        msg = request.args.get('msg')
        
        # Parse JSON strings if needed
        try:
            recipients = json.loads(recipients_str) if recipients_str else []
            cc = json.loads(cc_str) if cc_str else []
        except:
            recipients = [recipients_str] if recipients_str else []
            cc = [cc_str] if cc_str else []
    else:
        # Get data from JSON body
        request_data = request.get_json()
        print('request_data', request_data)
        
        sender = request_data.get('sender')
        recipients = request_data.get('recipients')
        cc = request_data.get('cc')
        subject = request_data.get('subject')
        msg = request_data.get('msg')

    r = send_email(sender, recipients, cc, subject, msg, attachments=[])
    print('r', r)

    return jsonify({"status": "success", "data": r})
