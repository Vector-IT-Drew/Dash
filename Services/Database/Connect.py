import os
import mysql.connector
from flask import Blueprint, Flask, jsonify, request
import logging
from mysql.connector import Error
import decimal
from datetime import datetime

# Create a Blueprint instead of a Flask app
connect_bp = Blueprint('Connect', __name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', '35.231.226.236'),
            user=os.getenv('DB_USER', 'vector-dash-user'),
            password=os.getenv('DB_PASSWORD', 'VectorIT104!'),
            database=os.getenv('DB_NAME', 'dash-database'),
            port=int(os.getenv('DB_PORT', '3306'))
        )
        
        if connection.is_connected():
            return {"status": "connected", "connection": connection}
        else:
            return {"status": "error", "message": "Failed to connect to database"}
            
    except Error as e:
        logger.error(f"Database connection error: {str(e)}")
        return {"status": "error", "message": str(e)}

@connect_bp.route('/connect', methods=['GET'])
def connect():
    try:
        result = get_db_connection()
        
        if result["status"] == "connected":
            # Return success but close the connection since we're just testing
            if "connection" in result:
                result["connection"].close()
            
            return jsonify({"status": "connected"})
        else:
            return jsonify({"status": "error", "message": result.get("message", "Unknown error")})
            
    except Exception as e:
        logger.error(f"Error in connect route: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})


@connect_bp.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"})

@connect_bp.route('/', methods=['GET'])
def index():
    return jsonify({
        "service": "Database Connection API",
        "endpoints": {
            "/health": "GET - Health check endpoint",
            "/connect": "GET - Test database connection",
            "/listings": "GET - Listings from database",
            "/get_filtered_listings": "GET - Filtered listings with parameters"
        }
    })

# For local testing only
app = Flask(__name__)
app.register_blueprint(connect_bp)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port)