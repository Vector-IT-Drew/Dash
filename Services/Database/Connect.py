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

@connect_bp.route('/get_filtered_listings', methods=['GET'])
def get_filtered_listings():
    try:
        # Get filter parameters from query string
        address = request.args.get('address')
        unit = request.args.get('unit')
        beds = request.args.get('beds')
        baths = request.args.get('baths')
        neighborhood = request.args.get('neighborhood')
        min_price = request.args.get('min_price')
        max_price = request.args.get('max_price')
        limit = request.args.get('limit', 100)  # Default limit of 100
        
        # Get database connection
        db_result = get_db_connection()
        
        if db_result["status"] != "connected":
            return jsonify({"status": "error", "message": "Database connection failed"})
        
        connection = db_result["connection"]
        cursor = connection.cursor(dictionary=True)
        
        # Start building the query to get vacant units and units with expiring deals
        query = """
            SELECT u.unit_id, u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure,
                   u.floor_num, u.unit_status, d.expiry, d.actual_rent
            FROM units u
            LEFT JOIN deals d ON u.unit_id = d.unit_id
            WHERE (u.unit_status = 'Vacant' OR 
                  (u.unit_status = 'Occupied' AND d.expiry <= DATE_ADD(CURDATE(), INTERVAL 1 MONTH)))
        """
        
        # Add filter conditions
        params = []
            
        if address:
            query += " AND u.address LIKE %s"
            params.append(f"%{address}%")
            
        if unit:
            query += " AND u.unit = %s"
            params.append(unit)
            
        if beds:
            query += " AND u.beds = %s"
            params.append(int(beds))
            
        if baths:
            query += " AND u.baths = %s"
            params.append(float(baths))
            
        if neighborhood:
            query += " AND u.address LIKE %s"
            params.append(f"%{neighborhood}%")
            
        if min_price:
            query += " AND d.actual_rent >= %s"
            params.append(float(min_price))
            
        if max_price:
            query += " AND d.actual_rent <= %s"
            params.append(float(max_price))
        
        # Add order by and limit
        query += " ORDER BY u.unit_id DESC LIMIT %s"
        params.append(int(limit))
        
        # Execute the query
        cursor.execute(query, params)
        units = cursor.fetchall()
        
        # Convert Decimal objects to float for JSON serialization
        def decimal_to_float(obj):
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            return obj
        
        # Process each unit to convert Decimal values to float and set past expiry to blank
        processed_units = []
        today = datetime.now().date()
        
        for unit in units:
            processed_unit = {}
            for key, value in unit.items():
                if key == 'expiry' and value is not None:
                    # Check if expiry is in the past
                    if isinstance(value, datetime) and value.date() < today:
                        processed_unit[key] = ""
                    else:
                        processed_unit[key] = value
                else:
                    processed_unit[key] = decimal_to_float(value)
            processed_units.append(processed_unit)
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        
        logger.info(f"Retrieved {len(units)} filtered units from database")
        return jsonify({
            "status": "success", 
            "count": len(processed_units),
            "data": processed_units
        })
    
    except Exception as e:
        logger.error(f"Error retrieving filtered units: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# For local testing only
app = Flask(__name__)
app.register_blueprint(connect_bp)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port)