import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection
import decimal
import json

# Create a Blueprint
listings_bp = Blueprint('Listings', __name__)
logger = logging.getLogger(__name__)

# @listings_bp.route('/listings', methods=['GET'])
# def get_listings():
#     try:
#         # Get database connection from connect
#         db_result = get_db_connection()
        
#         if db_result["status"] != "connected":
#             return {"status": "error", "message": "Database connection failed"}
        
#         connection = db_result["connection"]
#         cursor = connection.cursor(dictionary=True)
        
#         # Execute a test query to get listings
#         query = """
#             SELECT l.unit_id, l.listing_status, l.listing_gross 
#             FROM listings l 
#             LIMIT 10
#         """
        
#         cursor.execute(query)
#         listings = cursor.fetchall()
        
#         cursor.close()
#         connection.close()
        
#         logger.info(f"Retrieved {len(listings)} listings from database")
#         return {"status": "success", "data": listings}
    
#     except Exception as e:
#         logger.error(f"Error retrieving listings: {str(e)}")
#         return {"status": "error", "message": str(e)}


@listings_bp.route('/get_filtered_listings', methods=['GET'])
def get_filtered_listings():
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
    include_all = request.args.get('include_all', False)
    
    # Call the reusable function with parameters from request
    result = get_filtered_listings_data(
        address, unit, beds, baths, neighborhood, 
        min_price, max_price, limit, available, sort, include_all
    )
    
    # Return JSON response for the API endpoint
    return jsonify(result)


def get_filtered_listings_data(
    address=None, unit=None, beds=None, baths=None, 
    neighborhood=None, min_price=None, max_price=None, 
    limit=10000, available=False, sort='ORDER BY d.actual_rent DESC', include_all=False, direct_response=False
):
    """
    Get filtered listings data that can be called from other Python files.
    
    Args:
        address (str, optional): Filter by address
        unit (str, optional): Filter by unit number
        beds (float, optional): Minimum number of bedrooms
        baths (float, optional): Minimum number of bathrooms
        neighborhood (str, optional): Filter by neighborhood
        min_price (float, optional): Minimum price
        max_price (float, optional): Maximum price
        limit (int, optional): Maximum number of results to return
        available (bool, optional): Only show currently available units
        sort (str, optional): Sort order for results
        include_all (bool, optional): Include all fields in the response
        direct_response (bool, optional): Return data directly instead of jsonify
        
    Returns:
        dict: Dictionary with status, count, and data keys
    """
    try:
        # Get database connection
        db_result = get_db_connection()
        
        if db_result["status"] != "connected":
            return {"status": "error", "message": "Database connection failed"}
        
        connection = db_result["connection"]
        cursor = connection.cursor(dictionary=True)
        
        # Start building the query to get vacant units and units with expiring deals
        query = f"""
            SELECT u.unit_id, u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure,
                u.floor_num, u.unit_status, d.expiry, d.actual_rent, u.unit_images, 
                a.building_name, a.neighborhood, a.borough, d.deal_status, d.move_out, 
                u.rentable, a.building_amenities, p.portfolio_email, a.building_image
                {f', u.*, d.*, a.*' if include_all else ''}  
            FROM units u
            LEFT JOIN deals d ON u.unit_id = d.unit_id
            LEFT JOIN addresses a ON u.address_id = a.address_id
            LEFT JOIN entities e ON a.entity_id = e.entity_id
            LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
            WHERE 
                d.actual_rent IS NOT NULL 
                AND d.actual_rent != '' 
                AND d.actual_rent != 0
                AND u.rentable = True
                AND (
                    (
                        d.move_out IS NOT NULL
                        AND u.unit_status = 'Occupied' 
                        AND d.move_out <= DATE_ADD(CURDATE(), INTERVAL 3 MONTH) 
                        AND d.deal_status != 'Closed' 
                        AND d.deal_status != 'Renewal Check'
                        {f'AND d.move_out <= CURDATE()' if available else ''}
                    ) 
                    OR (
                        u.unit_status = 'Vacant' 
                    )
                )
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
            
        if min_price:
            query += " AND d.actual_rent >= %s"
            params.append(float(min_price))
            
        if max_price:
            query += " AND d.actual_rent <= %s"
            params.append(float(max_price))
        
        # Add order by and limit
        if sort == 'price_asc':
            query += f' ORDER BY d.actual_rent ASC  '
        elif sort == 'price_desc':
            query += f' ORDER BY d.actual_rent DESC'
        elif sort == 'size_desc':
            query += f' ORDER BY u.sqft DESC'
        else:
            query += sort
        
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
        
        # Return the raw result if direct_response is True
        if direct_response:
            return result
        else:
            return result  # This will be jsonified by the route handler
    
    except Exception as e:
        logger.error(f"Error retrieving filtered units: {str(e)}")
        error_result = {"status": "error", "message": str(e)}
        
        # Return the raw error result if direct_response is True
        if direct_response:
            return error_result
        else:
            return error_result  # This will be jsonified by the route handler


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
            SELECT u.*, d.*, a.*, p.portfolio_email
            FROM units u
            LEFT JOIN deals d ON u.unit_id = d.unit_id
            LEFT JOIN addresses a ON u.address_id = a.address_id
            LEFT JOIN entities e ON a.entity_id = e.entity_id
            LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
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
            if key == 'move_out' and value is not None:
                try:
                    processed_unit[key] = value.strftime('%m/%d/%Y')
                except Exception as e:
                    logger.error(f"Error formatting move_out date: {str(e)}")
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


def get_unique_neighborhoods_and_addresses():
    """
    Get unique neighborhoods and addresses where rentable is True.
    
    Returns:
        dict: A dictionary containing unique neighborhoods and addresses
    """
    try:
        # Get database connection
        db_result = get_db_connection()
        connection = db_result["connection"]
        cursor = connection.cursor(dictionary=True)
        
        # Query to get unique neighborhoods and addresses where rentable is True
        query = """
            SELECT DISTINCT u.neighborhood, u.address
            FROM units u
            WHERE u.rentable = True
        """
        
        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Process results to extract unique neighborhoods and addresses
        unique_neighborhoods = set()
        unique_addresses = set()
        
        for row in results:
            if row['neighborhood']:
                unique_neighborhoods.add(row['neighborhood'])
            if row['address']:
                unique_addresses.add(row['address'])
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        
        # Return unique values
        return {
            "status": "success",
            "unique_neighborhoods": list(unique_neighborhoods),
            "unique_addresses": list(unique_addresses)
        }
    
    except Exception as e:
        logger.error(f"Error retrieving unique values: {str(e)}")
        return {"status": "error", "message": str(e)}


@listings_bp.route('/unique-values', methods=['GET'])
def unique_values():
    """
    Endpoint to get unique neighborhoods and addresses where rentable is True.
    
    Returns:
        JSON: A JSON response containing unique neighborhoods and addresses
    """
    unique_values = get_unique_neighborhoods_and_addresses()
    return jsonify(unique_values)


# For local testing only
app = Flask(__name__)
app.register_blueprint(listings_bp)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port) 