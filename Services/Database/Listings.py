import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection
import decimal
import json

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
        limit = request.args.get('limit', 1000)  # Default limit of 100
        available = request.args.get('available', False)  # Default limit of 100
        
        # Get database connection
        db_result = get_db_connection()
        
        if db_result["status"] != "connected":
            return jsonify({"status": "error", "message": "Database connection failed"})
        
        connection = db_result["connection"]
        cursor = connection.cursor(dictionary=True)
        
        # Start building the query to get vacant units and units with expiring deals
        query = f"""
            SELECT u.unit_id, u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure,
                    u.floor_num, u.unit_status, d.expiry, d.actual_rent, u.unit_images, a.building_name, a.neighborhood, a.borough, d.deal_status, d.move_out, u.rentable
            FROM units u
            LEFT JOIN deals d ON u.unit_id = d.unit_id
            LEFT JOIN addresses a ON u.address_id = a.address_id
            WHERE 
                d.actual_rent IS NOT NULL 
                AND d.actual_rent != '' 
                AND d.actual_rent != 0
                AND u.address IN ('525 East 72nd Street', '1113 York Avenue', '420 East 61st Street')
                AND ((d.move_out IS NOT NULL
                    AND u.rentable = True
                    AND u.unit_status = 'Occupied' 
                    AND d.move_out <= DATE_ADD(CURDATE(), INTERVAL 3 MONTH) 
                    AND d.deal_status != 'Closed' 
                    AND d.deal_status != 'Renewal Check'
                    {f'AND move_out <= DATE_ADD(CURDATE())' if available else ''}
                    
                ) OR (
                    u.rentable = True
                    AND u.unit_status = 'Vacant' 
                ) )
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
            params.append(float(beds))
            
        if baths:
            query += " AND u.baths >= %s"
            params.append(float(baths))
            
        if neighborhood:
            query += " AND a.neighborhood LIKE %s"
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
        from datetime import datetime
        today = datetime.now().date()
        
        for unit in units:
            processed_unit = {}
            for key, value in unit.items():
                if key == 'move_out' and value is not None:
                    try:
                        if value > today:
                            processed_unit[key] = value.strftime('%m/%d/%Y')
                        else:
                            processed_unit[key] = ""

                    except Exception as e : 
                        print('move_out', e)
                        # Handle date objects directly
                elif key == 'unit_images' and value is not None:
                    try:
                        # Parse the JSON string to get the list of URLs
                        image_urls = json.loads(value)
                        # Get the first URL if available, otherwise empty string
                        processed_unit[key] = image_urls[0] if image_urls and len(image_urls) > 0 else ""
                    except Exception as e:
                        logger.error(f"Error processing unit_images: {str(e)}")
                        processed_unit[key] = ""
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


@listings_bp.route('/get_listing', methods=['GET'])
def get_listing():
    try:
        unit_id = request.args.get('unit_id')

        # Get database connection
        db_result = get_db_connection()
        
        if db_result["status"] != "connected":
            return jsonify({"status": "error", "message": "Database connection failed"})
        
        connection = db_result["connection"]
        cursor = connection.cursor(dictionary=True)
        
        # Query to get all details for a specific unit
        query = """
            SELECT u.*, d.*, a.*,
                   u.unit_id, u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure,
                   u.floor_num, u.unit_status, u.unit_images,
                   d.deal_id, d.expiry, d.actual_rent, d.deal_status, 
                   a.building_name, a.neighborhood, a.borough, a.zip_code
            FROM units u
            LEFT JOIN deals d ON u.unit_id = d.unit_id
            LEFT JOIN addresses a ON u.address_id = a.address_id
            WHERE u.unit_id = %s
        """
        
        # Execute the query
        cursor.execute(query, (unit_id,))
        unit = cursor.fetchone()
        
        if not unit:
            cursor.close()
            connection.close()
            return jsonify({"status": "error", "message": "Listing not found"}), 404
        
        # Process the unit to handle special data types
        processed_unit = {}
        from datetime import datetime
        today = datetime.now().date()
        
        for key, value in unit.items():
            if key == 'expiry' and value is not None:
                try:
                    processed_unit[key] = value.strftime('%m/%d/%Y')
                except Exception as e:
                    logger.error(f"Error formatting expiry date: {str(e)}")
                    processed_unit[key] = ""
            else:
                # Convert Decimal objects to float
                if isinstance(value, decimal.Decimal):
                    processed_unit[key] = float(value)
                else:
                    processed_unit[key] = value
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        
        logger.info(f"Retrieved details for unit_id: {unit_id}")
        return jsonify({
            "status": "success",
            "data": processed_unit
        })
    
    except Exception as e:
        logger.error(f"Error retrieving listing details: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# For local testing only
app = Flask(__name__)
app.register_blueprint(listings_bp)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port) 