import os
import mysql.connector
from flask import Blueprint, Flask, jsonify, request
import logging
from mysql.connector import Error
import decimal
from datetime import datetime
import random
import string
import json

# Create a Blueprint instead of a Flask app
connect_bp = Blueprint('Connect', __name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#returns status, connection, and credentials
def get_db_connection(session_key=None):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', '35.231.226.236'),
            user=os.getenv('DB_USER', 'vector-dash-user'),
            password=os.getenv('DB_PASSWORD', 'VectorIT104!'),
            database=os.getenv('DB_NAME', 'dash-database'),
            port=int(os.getenv('DB_PORT', '3306'))
        )
        
        if connection.is_connected():
            # Validate session key if provided
            if session_key:
                validation_result = validate_session_key_and_get_credentials(connection, session_key)
                if validation_result["status"] == "success":
                    return {"status": "connected", "connection": connection, "credentials": json.loads(validation_result["credentials"])}
                else:
                    connection.close()
                    return {"status": "error", "message": "Invalid session key"}
            else:
                return {"status": "connected", "connection": connection, "credentials": None}
        else:
            return {"status": "error", "message": "Failed to connect to database"}
            
    except Error as e:
        logger.error(f"Database connection error: {str(e)}")
        return {"status": "error", "message": str(e)}

def validate_session_key_and_get_credentials(connection, session_key):
    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT session_key, credentials FROM dashboard_credentials WHERE session_key = %s"
        cursor.execute(query, (session_key,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return {"status": "success", "credentials": result["credentials"]}
        else:
            return {"status": "failed", "message": "Session key not found"}
    except Error as e:
        logger.error(f"Error during session key validation: {str(e)}")
        return {"status": "error", "message": str(e)}

@connect_bp.route('/get_credentials', methods=['GET'])
def get_credentials():
    # Extract session key from query parameters
    session_key = request.args.get('session_key')
    if not session_key:
        return jsonify({"status": "error", "message": "Session key not provided"}), 400

    # Get database connection with session key validation
    db_result = get_db_connection(session_key=session_key)
    if db_result["status"] != "connected":
        return jsonify({"status": "error", "message": db_result.get("message", "Invalid session key")}), 401

    return jsonify({"status": "success", "credentials": db_result["credentials"]})

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
            "/get_filtered_listings": "GET - Filtered listings with parameters",
            "/get_landlord_data": "GET - Landlord data with parameters",
            "/get_unit_data": "GET - Unit data with parameters"
            
        }
    })

@connect_bp.route('/login', methods=['POST', 'GET'])
def login():
    try:
        # Handle both POST and GET requests
        if request.method == 'POST':
            data = request.json
            print("data POST:", data)
            username = data.get('username')
            password = data.get('password')
        else:  # GET request
            print("data Get:", request.args)
            username = request.args.get('username')
            password = request.args.get('password')

        if not username or not password:
            return jsonify({"status": "error", "message": "Username and password are required"}), 400

        db_result = get_db_connection()
        if db_result["status"] != "connected":
            return jsonify({"status": "error", "message": "Failed to connect to database"})

        connection = db_result["connection"]
        try:
            cursor = connection.cursor(dictionary=True)
            # Join dashboard_credentials with persons to get first_name and last_name
            query = """
                SELECT dc.*, p.first_name, p.last_name, p.person_id
                FROM dashboard_credentials dc
                LEFT JOIN persons p ON dc.person_id = p.person_id
                WHERE dc.username = %s AND dc.password = %s
            """
            cursor.execute(query, (username, password))
            user = cursor.fetchone()

            if user:
                if user['session_key']:
                    session_key = user['session_key']
                else:
                    session_key = generate_session_key()
                    update_query = "UPDATE dashboard_credentials SET session_key = %s WHERE person_id = %s"
                    cursor.execute(update_query, (session_key, user['person_id']))
                    connection.commit()
                return jsonify({
                    "status": "success",
                    "session_key": session_key,
                    "first_name": user.get('first_name', ''),
                    "last_name": user.get('last_name', ''),
                    "person_id": user.get('person_id', '')
                })
            else:
                return jsonify({"status": "error", "message": "Invalid credentials"}), 401
        except Error as e:
            logger.error(f"Error during authentication: {str(e)}")
            return jsonify({"status": "error", "message": str(e)})
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    except Exception as e:
        logger.error(f"Error in login route: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

def generate_session_key():
    return 'session_key_' + ''.join(random.choices(string.digits, k=16))

# # @connect_bp.route('/check-session', methods=['GET'])
# # def check_session():
# #     session_key = request.cookies.get('session_key')

# #     # Handle both POST and GET requests
# #     if request.method == 'POST':
# #         data = request.json
# #         print("data POST:", data)
# #         session_key = data.get('session_key')
# #     else:  # GET request
# #         print("data Get:", request.args)
# #         session_key = request.args.get('session_key')
        
# #     if not session_key:
# #         return jsonify({"status": "failed", "message": "Session key not provided"}), 400

# #     db_result = get_db_connection()
# #     if db_result["status"] != "connected":
# #         return jsonify({"status": "error", "message": "Failed to connect to database"}), 500

# #     connection = db_result["connection"]
# #     try:
# #         cursor = connection.cursor(dictionary=True)
# #         query = "SELECT session_key FROM dashboard_credentials WHERE session_key = %s"
# #         cursor.execute(query, (session_key,))
# #         result = cursor.fetchone()

# #         if result:
# #             return jsonify({"status": "success", "message": "Session key is valid"})
# #         else:
# #             return jsonify({"status": "failed", "message": "Session key not found"}), 401

# #     except Error as e:
# #         logger.error(f"Error during session check: {str(e)}")
# #         return jsonify({"status": "error", "message": str(e)}), 500
# #     finally:
# #         if connection.is_connected():
# #             cursor.close()
# #             connection.close()

# # Ensure check_session is a utility function that can be called directly
# def check_session(session_key):
#     db_result = get_db_connection()
#     if db_result["status"] != "connected":
#         return {"status": "error", "message": "Failed to connect to database"}

#     connection = db_result["connection"]
#     try:
#         cursor = connection.cursor(dictionary=True)
#         query = "SELECT session_key FROM dashboard_credentials WHERE session_key = %s"
#         cursor.execute(query, (session_key,))
#         result = cursor.fetchone()

#         if result:
#             return {"status": "success", "message": "Session key is valid"}
#         else:
#             return {"status": "failed", "message": "Session key not found"}
#     except Error as e:
#         logger.error(f"Error during session check: {str(e)}")
#         return {"status": "error", "message": str(e)}
#     finally:
#         if connection.is_connected():
#             cursor.close()
#             connection.close()

if __name__ == '__main__':

    
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port)







