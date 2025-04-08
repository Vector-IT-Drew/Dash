import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection

# Create a Blueprint
listings_bp = Blueprint('Listings', __name__)
logger = logging.getLogger(__name__)

@listings_bp.route('/listings', methods=['GET'])
def get_listings():
    try:
        # Get database connection from connect
        db_result = get_db_connection()
        
        if db_result["status"] != "connected":
            return {"status": "error", "message": "Database connection failed"}
        
        connection = db_result["connection"]
        cursor = connection.cursor(dictionary=True)
        
        # Execute a test query to get listings
        query = """
            SELECT l.unit_id, l.listing_status, l.listing_gross 
            FROM listings l 
            LIMIT 10
        """
        
        cursor.execute(query)
        listings = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        logger.info(f"Retrieved {len(listings)} listings from database")
        return {"status": "success", "data": listings}
    
    except Exception as e:
        logger.error(f"Error retrieving listings: {str(e)}")
        return {"status": "error", "message": str(e)}


@listings_bp.route('/get_filtered_listings', methods=['GET'])
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



# For local testing only
app = Flask(__name__)
app.register_blueprint(listings_bp)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port) 