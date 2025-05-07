# Services/Emails/emailwatcher.py

from flask import Blueprint, request, jsonify
import base64
import json
from google.auth.transport import requests as grequests
from google.oauth2 import id_token

emailwatcher_bp = Blueprint('emailwatcher', __name__)

@emailwatcher_bp.route('/email_watcher', methods=['POST'])
def email_watcher():
    # Verify the JWT
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return 'Unauthorized', 401
    token = auth_header.split(' ')[1]
    try:
        id_info = id_token.verify_oauth2_token(
            token,
            grequests.Request(),
            audience='sheets-helper@vector-main-app.iam.gserviceaccount.com'
        )
    except Exception as e:
        print("JWT verification failed:", e)
        return 'Unauthorized', 401

    envelope = request.get_json()
    pubsub_message = envelope.get('message', {})
    if 'data' in pubsub_message:
        data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        message_data = json.loads(data)
        print("Received Gmail notification:", message_data)
        # TODO: Process the notification (fetch new emails, etc.)
    return jsonify({'status': 'ok'})
