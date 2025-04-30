import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection
import decimal
import json
from functools import wraps


# Create a Blueprint
data_bp = Blueprint('Data', __name__)
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

@with_db_connection
def build_column_to_table_mapping(connection, credentials):
    try:
        cursor = connection.cursor()
        query = """
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME IN ('units', 'addresses', 'entities', 'portfolios', 'deals');
        """
        cursor.execute(query, (os.getenv('DB_NAME', 'dash-database'),))
        results = cursor.fetchall()

        # Build the column-to-table mapping
        column_to_table = {column: table for table, column in results}

        cursor.close()
        return column_to_table

    except Exception as e:
        logging.error(f"Error building column-to-table mapping: {str(e)}")
        return {}

def run_data_query(connection, credentials, columns, additional_filters=None):
    cursor = connection.cursor(dictionary=True)
    query = f"""
        SELECT {', '.join(columns)}
        FROM units u
        LEFT JOIN addresses a ON u.address_id = a.address_id
        LEFT JOIN entities e ON a.entity_id = e.entity_id
        LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
        LEFT JOIN deals d ON u.unit_id = d.unit_id
        WHERE 1=1
    """
    params = []

    # Create a mapping of columns to their table aliases
    column_to_table = {col.split('.')[1]: col.split('.')[0] for col in columns if col}

    # Apply data filters from credentials
    data_filters = credentials.get("data_filters", [])
    for column, value in data_filters:
        if column and column not in ["Any", "", "undefined", "-", "0"] and column is not None and 'Any' not in value:
            query += f" AND {column} = %s"
            params.append(value)

    # Apply additional filters from request
    if additional_filters:
        for column, value in additional_filters.items():
            if column and column != "Any" and column in column_to_table:
                table_alias = column_to_table[column]
                query += f" AND {table_alias}.{column} = %s"
                params.append(value)

    cursor.execute(query, params)
    data = cursor.fetchall()

    cursor.close()
    connection.close()

    return jsonify({"status": "success", "count": len(data), "data": data})

@data_bp.route('/get_unit_data', methods=['GET'])
@with_db_connection
def get_unit_data(connection, credentials):
    try:
        columns = ["u.unit_id", "u.address", "u.unit", "u.beds", "u.baths", "u.sqft", "u.exposure", "u.unit_status"]

        # Extract additional filters from query parameters
        additional_filters = {key: value for key, value in request.args.items() if key not in ['session_key']}

        response = run_data_query(connection, credentials, columns, additional_filters)
        return response
    except Exception as e:
        logger.error(f"Error retrieving filtered units: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if connection.is_connected():
            connection.close()


@data_bp.route('/get_landlord_data', methods=['GET'])
@with_db_connection
def get_landlord_data(connection, credentials):
    try:
        columns = ["p.portfolio", "u.address", "u.unit", "d.deal_status"]
        response = run_data_query(connection, credentials, columns)
        return response
    except Exception as e:
        logger.error(f"Error retrieving filtered units: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if connection.is_connected():
            connection.close()


@data_bp.route('/get_deal_data', methods=['GET'])
@with_db_connection
def get_deal_data(connection, credentials):
    try:
        columns = ["a.address", "u.unit", "d.tenant_ids", "d.guarantor_ids", "d.deal_status", "u.unit_status",
        "d.start_date", "d.term", "d.expiry","d.move_out", "d.concession", "d.gross", "d.actual_rent",
         "d.agent_id", "d.manager_id", "d.created_at"
    ]
        response = run_data_query(connection, credentials, columns)
        return response
    except Exception as e:
        logger.error(f"Error retrieving filtered units: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if connection.is_connected():
            connection.close()

# Build the column-to-table mapping once


@data_bp.route('/get_unique_values', methods=['GET'])
@with_db_connection
def get_unique_values(connection, credentials):

    cursor = connection.cursor(dictionary=True)

    column = request.args.get('column')
    if not column:
        return jsonify({"status": "error", "message": "Column name not provided"}), 400

    column_to_table = build_column_to_table_mapping()
    table_name = column_to_table.get(column)
    if not table_name:
        return jsonify({"status": "error", "message": "Invalid column name"}), 400

    # Construct the query to get unique values
    query = f"SELECT DISTINCT {column} FROM {table_name}"

    data_filters = credentials.get("data_filters", [])
    params = []
    for col, value in data_filters:
        if col and col != "Any" and col in column_to_table:
            query += f" AND {col.split(".")[1]} = %s"
            params.append(value)

    cursor.execute(query, params)
    data = cursor.fetchall()

    cursor.close()
    connection.close()

    vals = [d[column] for d in data]

    return jsonify({"status": "success", "data": vals})


@data_bp.route('/get_leads', methods=['GET'])
@with_db_connection
def get_leads(connection, credentials):
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Define the query
        query = """
            SELECT *
            FROM persons p
            LEFT JOIN person_roles r ON p.person_id = r.person_id
            LEFT JOIN preferences pref ON pref.person_id = p.person_id
            WHERE r.role_id = 200
        """

        first_name = request.args.get('first_name')
        if first_name:
            # Use LOWER() to make the comparison case-insensitive
            query += f" AND LOWER(p.first_name) = '{first_name.lower()}'"

        
        # Execute the query
        cursor.execute(query)
        data = cursor.fetchall()
        
        # Close the cursor and connection
        cursor.close()
        connection.close()
        
        # Return the results as JSON
        return jsonify({"status": "success", "data": data})
    
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if connection.is_connected():
            connection.close()