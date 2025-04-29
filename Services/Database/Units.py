import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection
import decimal
import json

# Create a Blueprint
units_bp = Blueprint('Units', __name__)
logger = logging.getLogger(__name__)


@units_bp.route('/get_unit_data', methods=['GET'])
def get_unit_data():
    # Retrieve session key from query parameters
    session_key = request.args.get('session_key')
    if not session_key:
        return jsonify({"status": "failed", "message": "Session key not provided"}), 400

    # Get database connection with session key validation
    db_result = get_db_connection(session_key=session_key)
    if db_result["status"] != "connected":
        return jsonify({"status": "failed", "message": db_result.get("message", "Database connection failed")}), 401

    connection = db_result["connection"]
    try:
        cursor = connection.cursor(dictionary=True)
        
        print('Get Filtered Listings Called!\n\n', request.args)
        # Get filter parameters from query string
        address = request.args.get('address')
        unit = request.args.get('unit')
        beds = request.args.get('beds')
        baths = request.args.get('baths')
        neighborhood = request.args.get('neighborhood')
        min_price = request.args.get('min_price')
        max_price = request.args.get('max_price')
        limit = request.args.get('limit', 1000)
        available = request.args.get('available', False)
        sort = request.args.get('sort', 'ORDER BY d.actual_rent DESC')

        # Start building the query to get vacant units and units with expiring deals
        query = f"""
            SELECT u.unit_id, u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure,
                u.floor_num, u.unit_status, u.unit_images, 
                a.building_name, a.neighborhood, a.borough, a.building_amenities, p.portfolio_email, a.building_image
            FROM units u
            LEFT JOIN addresses a ON u.address_id = a.address_id
            LEFT JOIN entities e ON a.entity_id = e.entity_id
            LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
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
        if beds == '0':
            query += " AND u.beds = 0"
        elif beds:
            query += " AND u.beds >= %s"
            params.append(float(beds))
            
        if baths:
            query += " AND u.baths >= %s"
            params.append(float(baths))
            
        if neighborhood:
            query += " AND a.neighborhood LIKE %s"
            params.append(f"%{neighborhood}%")
        
        if limit:
            query += " LIMIT %s"
            params.append(int(limit))
        
      
        # Execute the query
        cursor.execute(query, params)
        units = cursor.fetchall()
        
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
                        pass
                        # Handle date objects directly
                # elif key == 'unit_images' and value is not None:
                #     pass
                    # try:
                    #     # Parse the JSON string to get the list of URLs
                    #     image_urls = json.loads(value)
                    #     # Get the first URL if available, otherwise empty string
                    #     processed_unit[key] = image_urls[0] if image_urls and len(image_urls) > 0 else ""
                    # except Exception as e:
                    #     logger.error(f"Error processing unit_images: {str(e)}")
                    #     processed_unit[key] = ""
                else:
                    if isinstance(value, decimal.Decimal):
                        processed_unit[key] = float(value)
                    else:
                        processed_unit[key] = value
            processed_units.append(processed_unit)
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        
        logger.info(f"Retrieved {len(units)} filtered units from database")
        result = {
            "status": "success", 
            "count": len(processed_units),
            "data": processed_units
        }

        return result  # This will be jsonified by the route handler
    
    except Exception as e:
        logger.error(f"Error retrieving filtered units: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port) 