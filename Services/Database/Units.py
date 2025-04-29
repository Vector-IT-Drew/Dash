import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection
import decimal
import json
from functools import wraps


# Create a Blueprint
units_bp = Blueprint('Units', __name__)
logger = logging.getLogger(__name__)

def with_db_connection(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        session_key = request.args.get('session_key')
        if not session_key:
            return jsonify({"status": "failed", "message": "Session key not provided"}), 400

        db_result = get_db_connection(session_key=session_key)
        if db_result["status"] != "connected":
            return jsonify({"status": "failed", "message": db_result.get("message", "Invalid session key")}), 401

        # Pass the connection and credentials to the endpoint function
        return f(db_result["connection"], db_result["credentials"], *args, **kwargs)
    return decorated_function


@units_bp.route('/get_unit_data', methods=['GET'])
@with_db_connection
def get_unit_data(connection, credentials):
    try:
        cursor = connection.cursor(dictionary=True)
        data_filters = credentials.get("data_filters", [])

        query = f"""
            SELECT u.unit_id, u.address, u.unit, u.beds, u.baths, u.sqft, u.exposure, u.unit_status
            FROM units u
            LEFT JOIN addresses a ON u.address_id = a.address_id
            LEFT JOIN entities e ON a.entity_id = e.entity_id
            LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
            WHERE 1=1
            
        """
        params = []

        # Apply data filters from credentials
        data_filters = credentials.get("data_filters", [])
        for column, value in data_filters:
            query += f" AND {column} = %s"
            params.append(value)

        # Execute the query
        cursor.execute(query, params)
        units = cursor.fetchall()

        # Directly return the fetched units
        return jsonify({"status": "success", "data": units})

    except Exception as e:
        logger.error(f"Error retrieving filtered units: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


@units_bp.route('/get_landlord_data', methods=['GET'])
@with_db_connection
def get_landlord_data(connection, credentials):
    try:
        cursor = connection.cursor(dictionary=True)
        data_filters = credentials.get("data_filters", [])

        query = f"""
            SELECT p.portfolio, u.address, u.unit, d.deal_status
            FROM units u
            LEFT JOIN deals d ON u.unit_id = d.unit_id
            LEFT JOIN addresses a ON u.address_id = a.address_id
            LEFT JOIN entities e ON a.entity_id = e.entity_id
            LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
            WHERE 1=1
            
        """
        params = []

        # Apply data filters from credentials
        data_filters = credentials.get("data_filters", [])
        for column, value in data_filters:
            query += f" AND {column} = %s"
            params.append(value)

        # Execute the query
        cursor.execute(query, params)
        units = cursor.fetchall()

        # Directly return the fetched units
        return jsonify({"status": "success", "data": units})

    except Exception as e:
        logger.error(f"Error retrieving filtered units: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()