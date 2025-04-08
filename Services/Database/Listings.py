import os
from flask import Blueprint, jsonify, Flask
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

@listings_bp.route('/listings', methods=['GET'])
def retrieve_listings():
    result = get_listings()
    
    if result["status"] == "success":
        return jsonify({"status": "success", "listings": result["data"]})
    else:
        return jsonify({"status": "error", "message": result["message"]}), 500

# For local testing only
app = Flask(__name__)
app.register_blueprint(listings_bp)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port) 