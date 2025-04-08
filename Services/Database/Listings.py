import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection
import decimal

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
        
        # Start building the query to get vacant units and units with expiring deals
        query = """
            SELECT u.unit_id, u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure,
                   u.floor_num, u.unit_status, d.expiry, d.actual_rent
            FROM units u
            LEFT JOIN deals d ON u.unit_id = d.unit_id
            WHERE (u.unit_status = 'Vacant' OR 
                  (d.deal_status = 'Occupied' AND d.expiry <= DATE_ADD(CURDATE(), INTERVAL 1 MONTH)))
                  AND d.actual_rent IS NOT NULL AND d.actual_rent != '' AND d.actual_rent != 0
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
        
        # Process each unit to convert Decimal values to float
        processed_units = []
        for unit in units:
            processed_unit = {}
            for key, value in unit.items():
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
app.register_blueprint(listings_bp)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port) 