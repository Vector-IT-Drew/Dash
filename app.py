from flask import Flask, request, Response, jsonify
import os
import time
import uuid
import json
import logging
from functools import wraps
from dotenv import load_dotenv
from Services.Database import connect_bp, listings_bp
from Services.Logging import log_viewer_bp

load_dotenv()

app = Flask(__name__)
app.register_blueprint(connect_bp)
app.register_blueprint(listings_bp)
app.register_blueprint(log_viewer_bp)

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure custom API logger with its own dedicated file
api_logger = logging.getLogger("api")
api_logger.setLevel(logging.INFO)
api_logger.propagate = False  # Don't propagate to root logger

# Custom formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# File handler for API logs only
file_handler = logging.FileHandler("logs/api_requests.log", mode='a')
file_handler.setFormatter(formatter)
api_logger.addHandler(file_handler)

# Optional console handler for API logs
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
api_logger.addHandler(console_handler)

@app.before_request
def before_request():
    # Skip logging for admin routes, static files and favicon
    if request.path.startswith('/admin') or request.path.startswith('/static') or request.path == '/favicon.ico':
        return
    
    # Generate unique request ID
    request_id = str(uuid.uuid4())
    request.request_id = request_id
    request.start_time = time.time()
    
    # Create a simplified log for requests
    request_data = {
        "method": request.method,
        "url": request.url,
        "path": request.path,
        "client_ip": request.remote_addr,
    }
    
    # Add body if present and is JSON
    if request.is_json:
        try:
            request_data["body"] = request.get_json()
        except:
            request_data["body"] = "(invalid JSON)"
    
    # Add query parameters if present
    if request.args:
        request_data["query_params"] = dict(request.args)
    
    # Convert to JSON string and log it
    request_json = json.dumps(request_data)
    api_logger.info(f"REQUEST {request_id}: {request.method} {request.path} - {request_json}")

@app.after_request
def after_request(response):
    # Skip logging for admin routes, static files and favicon
    if request.path.startswith('/admin') or request.path.startswith('/static') or request.path == '/favicon.ico':
        return response
    
    request_id = getattr(request, 'request_id', 'unknown')
    duration = time.time() - getattr(request, 'start_time', time.time())
    
    # Create a simplified response log
    response_data = {
        "status_code": response.status_code,
        "duration_ms": round(duration * 1000, 2)
    }
    
    # Try to capture response body if it's JSON
    try:
        response_body = response.get_data().decode('utf-8')
        if response.headers.get('Content-Type', '').startswith('application/json'):
            response_data["body"] = json.loads(response_body)
        elif len(response_body) < 1000:  # Only log short text responses
            response_data["body"] = response_body
    except Exception as e:
        response_data["error"] = str(e)
    
    # Convert to JSON string and log it
    response_json = json.dumps(response_data)
    api_logger.info(f"RESPONSE {request_id}: {response.status_code} {round(duration * 1000)}ms - {response_json}")
    
    return response

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)