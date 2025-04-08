import os
import mysql.connector
from flask import Blueprint, Flask, jsonify, request
import logging
from mysql.connector import Error
import decimal

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
        
        # Start building the query with JOIN to units table
        query = """
            SELECT l.listing_id, l.unit_id, l.listing_status, l.listing_gross,
                   u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure,
                   u.floor_num
            FROM listings l
            JOIN units u ON l.unit_id = u.unit_id
            WHERE 1=1
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
            
        if neighborhood:  # Note: neighborhood isn't in the schema, might need to extract from address
            query += " AND u.address LIKE %s"
            params.append(f"%{neighborhood}%")
            
        if min_price:
            query += " AND l.listing_gross >= %s"
            params.append(float(min_price))
            
        if max_price:
            query += " AND l.listing_gross <= %s"
            params.append(float(max_price))
        
        # Add order by and limit
        query += " ORDER BY l.listing_id DESC LIMIT %s"
        params.append(int(limit))
        
        # Execute the query
        cursor.execute(query, params)
        listings = cursor.fetchall()
        
        # Convert Decimal objects to float for JSON serialization
        def decimal_to_float(obj):
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            return obj
        
        # Process each listing to convert Decimal values to float
        processed_listings = []
        for listing in listings:
            processed_listing = {}
            for key, value in listing.items():
                processed_listing[key] = decimal_to_float(value)
            processed_listings.append(processed_listing)
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        
        logger.info(f"Retrieved {len(listings)} filtered listings from database")
        return jsonify({
            "status": "success", 
            "count": len(processed_listings),
            "data": processed_listings
        })
    
    except Exception as e:
        logger.error(f"Error retrieving filtered listings: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

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