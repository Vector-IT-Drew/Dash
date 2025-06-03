import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection
import decimal
import json
import math

# Create a Blueprint
listings_bp = Blueprint('Listings', __name__)
logger = logging.getLogger(__name__)

def haversine(lat1, lon1, lat2, lon2):
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    # Radius of earth in miles is 3956
    miles = 3956 * c
    return miles

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
    move_out = request.args.get('move_out', None)
    rentable = request.args.get('rentable', True)
    portfolio = request.args.get('portfolio', '')
   
    sort = request.args.get('sort', None)
    include_all = request.args.get('include_all', False)
    proximity = request.args.get('proximity', False)
    
    # Convert proximity_distance to float, default to 1 if not provided
    try:
        proximity_distance = float(request.args.get('proximity_distance', 1000))
    except ValueError:
        proximity_distance = 1000  # Fallback to default if conversion fails
    
    
    # Call the reusable function with parameters from request
    result = get_filtered_listings_data(
        address=address,
        unit=unit,
        beds=beds,
        baths=baths,
        neighborhood=neighborhood,
        min_price=min_price,
        max_price=max_price,
        proximity=proximity,
        proximity_distance=proximity_distance,
        limit=limit,
        available=available,
        sort=sort,
        include_all=include_all,
        move_out=move_out,
        rentable=rentable,
        portfolio=portfolio
    )
    
    # Return JSON response for the API endpoint
    return jsonify(result)


def get_filtered_listings_data(
    address=None, unit=None, beds=None, baths=None, 
    neighborhood=None, min_price=None, max_price=None, proximity=False, 
    limit=10000, available=False, sort=None, include_all=False, 
    direct_response=False, proximity_distance=1, move_out=None, rentable=True, portfolio=None):

    print('Getting data for portfolio: ', portfolio)
    
    try:
        # Get database connection
        db_result = get_db_connection()
        
        if db_result["status"] != "connected":
            return {"status": "error", "message": "Database connection failed"}
        
        connection = db_result["connection"]
        cursor = connection.cursor(dictionary=True)
        
        # Strip extra quotes if present
        if proximity and (proximity.startswith('"') and proximity.endswith('"')):
            proximity = proximity[1:-1]
        
        if proximity:
            cursor.execute("SELECT latitude, longitude, borough FROM addresses WHERE address = %s", (proximity,))
            location = cursor.fetchone()
            lat, lon = None, None
            if location:
                lat = location.get('latitude')
                lon = location.get('longitude')
                borough = location.get('borough')
                # If lat/lon are missing, use borough defaults
                if (lat is None or lon is None) and borough:
                    nyc_boroughs = {
                        "Manhattan": {"latitude": 40.7831, "longitude": -73.9712},
                        "Brooklyn": {"latitude": 40.6782, "longitude": -73.9442},
                        "Queens": {"latitude": 40.7282, "longitude": -73.7949},
                        "Bronx": {"latitude": 40.8448, "longitude": -73.8648},
                        "Staten Island": {"latitude": 40.5795, "longitude": -74.1502}
                    }
                    borough_defaults = nyc_boroughs.get(borough)
                    if borough_defaults:
                        lat = borough_defaults["latitude"]
                        lon = borough_defaults["longitude"]
            if lat is not None and lon is not None:
                # Calculate distance for each unit
                distance_calculation = f"""
                    3956 * 2 * ASIN(SQRT(
                        POWER(SIN((a.latitude - {lat}) * pi()/180 / 2), 2) +
                        COS(a.latitude * pi()/180) * COS({lat} * pi()/180) *
                        POWER(SIN((a.longitude - {lon}) * pi()/180 / 2), 2)
                    ))
                """
                # Add proximity filter to the query
                proximity_filter = f"AND {distance_calculation} <= {proximity_distance}"
            else:
                proximity_filter = ""
                distance_calculation = "NULL"
        else:
            proximity_filter = ""
            distance_calculation = "NULL"
        
        try:
            query = f"""
                SELECT * from (
                    SELECT 
                        CASE 
                            WHEN {include_all} THEN u.*, d1.*, a.*, p.portfolio_email, p.portfolio, {distance_calculation} AS distance
                            ELSE 
                                u.unit_id, u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure,
                                u.floor_num,
                                CASE
                                    WHEN u.unit_status LIKE '%DNR%' THEN 'DNR'
                                    WHEN (
                                        (d1.move_in IS NOT NULL AND d1.move_out IS NOT NULL AND CURRENT_DATE BETWEEN d1.move_in AND d1.move_out)
                                        OR
                                        (d2.move_in IS NOT NULL AND d2.move_out IS NOT NULL AND CURRENT_DATE BETWEEN d2.move_in AND d2.move_out)
                                        OR
                                        (d1.move_in IS NOT NULL AND d1.move_out IS NULL AND CURRENT_DATE >= d1.move_in)
                                    ) THEN 'Occupied'
                                    ELSE 'Vacant'
                                END AS unit_status,
                                d1.expiry, d1.actual_rent, u.unit_images, 
                                a.building_name, a.neighborhood, a.borough, d1.deal_status, d1.move_out, 
                                u.rentable, a.building_amenities, p.portfolio_email, a.building_image, p.portfolio,
                                {distance_calculation} AS distance
                        END
                    FROM units u
                    LEFT JOIN (
                        SELECT *
                        FROM (
                            SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.unit_id ORDER BY d.created_at DESC) as rn
                            FROM deals d
                        ) ranked
                        WHERE ranked.rn = 1
                    ) d1 ON u.unit_id = d1.unit_id
                    LEFT JOIN (
                        SELECT *
                        FROM (
                            SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.unit_id ORDER BY d.created_at DESC) as rn
                            FROM deals d
                        ) ranked
                        WHERE ranked.rn = 2
                    ) d2 ON u.unit_id = d2.unit_id
                    LEFT JOIN addresses a ON u.address_id = a.address_id
                    LEFT JOIN entities e ON a.entity_id = e.entity_id
                    LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
                ) AS subquery
                WHERE 
                    subquery.actual_rent IS NOT NULL 
                    AND subquery.actual_rent != '' 
                    AND subquery.actual_rent != 0
                    AND (
                        (
                            subquery.move_out IS NOT NULL
                            AND subquery.unit_status = 'Occupied' 
                            AND subquery.move_out <= DATE_ADD(CURDATE(), INTERVAL 3 MONTH) 
                            {("AND subquery.move_out <= CURDATE()" if available else "")}
                        ) 
                        OR (
                            subquery.unit_status = 'Vacant' 
                        )
                       
                    )
                    AND subquery.rentable = True
                    {proximity_filter}
                """
        except Exception as e:
            raise

        # Add filter conditions
        params = []

        # Only add rentable filter if rentable is exactly True
        if rentable is True:
            query += " AND subquery.rentable = True"
            
        if address:
            query += " AND subquery.address LIKE %s"
            params.append(f"%{address}%")
            
        if unit:
            query += " AND subquery.unit = %s"
            params.append(unit)

        if portfolio:
            print('Getting data for portfolio: ', portfolio)
            query += " AND subquery.portfolio = %s"
            params.append(portfolio)

        if beds == '0':
            query += " AND subquery.beds = 0"
        elif beds:
            query += " AND subquery.beds = %s"
            params.append(float(beds))
            
        if baths:
            query += " AND subquery.baths = %s"
            params.append(float(baths))
            
        if neighborhood:
            query += " AND subquery.neighborhood LIKE %s"
            params.append(f"%{neighborhood}%")
            
        if min_price:
            query += " AND subquery.actual_rent >= %s"
            params.append(float(min_price))
            
        if max_price:
            query += " AND subquery.actual_rent <= %s"
            params.append(float(max_price))
        
        if move_out:
            query += " AND (subquery.move_out <= %s OR subquery.move_out IS NULL)"
            params.append(move_out)

        # Add order by and limit
        if sort == 'price_asc':
            query += ' ORDER BY subquery.actual_rent ASC '
        elif sort == 'price_desc':
            query += ' ORDER BY subquery.actual_rent DESC '
        elif sort == 'size_desc':
            query += ' ORDER BY subquery.sqft DESC '
        else:
            query += ' ORDER BY subquery.actual_rent DESC '  # Default sort

        print('query', query )
        print('params', params)
        
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
                    LEFT JOIN (
                        SELECT d1.*
                        FROM deals d1
                        INNER JOIN (
                            SELECT unit_id, MAX(created_at) as max_created
                            FROM deals
                            GROUP BY unit_id
                        ) d2 ON d1.unit_id = d2.unit_id AND d1.created_at = d2.max_created
                    ) d ON u.unit_id = d.unit_id
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
        print('Error retrieving listing details: ', str(e))
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
        
        # Adjust the query to ensure correct table and column references
        query = """
            SELECT DISTINCT a.neighborhood, u.address
            FROM units u
            LEFT JOIN deals d ON u.unit_id = d.unit_id
            LEFT JOIN addresses a ON u.address_id = a.address_id
            LEFT JOIN entities e ON a.entity_id = e.entity_id
            LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id

            WHERE u.rentable = True
        """
        
        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Process results to extract unique neighborhoods and addresses
        unique_neighborhoods = set()
        unique_addresses = set()
        
        for row in results:

            if row['neighborhood'] and len(row['neighborhood'].strip()) > 1:
                unique_neighborhoods.add(row['neighborhood'])
            if row['address'] and len(row['address'].strip()) > 1:
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