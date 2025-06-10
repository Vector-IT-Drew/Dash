import os
from flask import Blueprint, jsonify, Flask, request
import logging
from .Connect import get_db_connection
import decimal
import json
from functools import wraps
import numpy as np
import pandas as pd


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
            query += f" AND {col.split('.')[1]} = %s"
            params.append(value)

    cursor.execute(query, params)
    data = cursor.fetchall()

    cursor.close()
    connection.close()

    vals = [d[column] for d in data]

    return jsonify({"status": "success", "data": vals})


@data_bp.route('/get_emails', methods=['GET'])
@with_db_connection
def get_emails(connection, credentials):

    try:
        cursor = connection.cursor(dictionary=True)
        person_id = request.args.get('person_id', 0)
        email_type = request.args.get('email_type', ' ')
        
        # Define the query
        query = f"""
            SELECT 
            *,
            'recipient' AS role
            FROM emails e
            WHERE JSON_CONTAINS(e.recipient_ids, JSON_ARRAY({person_id})) -- replace 2 with your person_id
            AND e.email_type = '{email_type}'

            UNION ALL

            SELECT 
            *,
            'cc' AS role
            FROM emails e
            WHERE JSON_CONTAINS(e.cc_ids, JSON_ARRAY({person_id}))
            AND e.email_type = '{email_type}'
        """
        
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


queries = {
    'all_leads': """
        SELECT *
            FROM persons p
            LEFT JOIN person_roles r ON p.person_id = r.person_id
            LEFT JOIN preferences pref ON pref.person_id = p.person_id
            WHERE r.role_id = 200
    """,
    'get_locations': """
        SELECT a.latitude, a.longitude
        FROM addresses a    
        WHERE 1=1       
    """,
    'get_client_data': """
        select * from(
            SELECT
                a.address,
                u.unit,
                u.unit_id,
                d1.lease_type,
                u.beds,
                u.baths,
                u.sqft,
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
                d1.deal_status,
                d1.gross,
                d2.gross AS previous_gross,
                d1.actual_rent,
                d2.actual_rent AS previous_actual_rent,
                d2.deal_status AS previous_deal_status,
                d2.move_out AS previous_move_out,
                d1.concession,
                d1.term,
                d1.move_in,
                d1.start_date,
                d1.move_out,
                d1.expiry,
                d1.tenant_ids,
                (
                    SELECT JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'person_id', p.person_id,
                            'first_name', p.first_name,
                            'last_name', p.last_name,
                            'email_address', p.email_address,
                            'phone_number', p.phone_number
                        )
                    )
                    FROM persons p
                    WHERE JSON_CONTAINS(d1.tenant_ids, CAST(p.person_id AS JSON))
                ) as tenant_info,
                note.note AS most_recent_note,
                note.created_at AS note_created_at,
                note.creator_id AS note_creator_id,
                CONCAT(per.first_name, ' ', per.last_name) AS creator_full_name,
                u.rentable,
                p.portfolio
            FROM units u
            LEFT JOIN addresses a ON u.address_id = a.address_id
            LEFT JOIN entities e ON a.entity_id = e.entity_id
            LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
            LEFT JOIN (
                SELECT *
                FROM (
                    SELECT
                        d.*,
                        ROW_NUMBER() OVER (PARTITION BY d.unit_id ORDER BY d.created_at DESC) as rn
                    FROM deals d
                ) ranked
                WHERE ranked.rn = 1
            ) d1 ON u.unit_id = d1.unit_id
            LEFT JOIN (
                SELECT *
                FROM (
                    SELECT
                        d.*,
                        ROW_NUMBER() OVER (PARTITION BY d.unit_id ORDER BY d.created_at DESC) as rn
                    FROM deals d
                ) ranked
                WHERE ranked.rn = 2
            ) d2 ON u.unit_id = d2.unit_id
            LEFT JOIN (
                SELECT n1.*
                FROM notes n1
                INNER JOIN (
                    SELECT target_id, MAX(created_at) AS max_created
                    FROM notes
                    WHERE target_type = 'units'
                    GROUP BY target_id
                ) n2 ON n1.target_id = n2.target_id AND n1.created_at = n2.max_created
                WHERE n1.target_type = 'units'
            ) note ON note.target_id = u.unit_id
            LEFT JOIN persons per ON note.creator_id = per.person_id
            WHERE 1=1
        ) subquery
        WHERE 1=1
    """,
    'get_notes': """
        select * from (
        SELECT n.*, a.address, 
            person.first_name AS first_name, 
            person.last_name AS last_name, 
            person.person_id,
            p.portfolio,
            e.entity
        FROM notes n
        LEFT JOIN units u ON n.target_type = 'units' AND n.target_id = u.unit_id
        LEFT JOIN addresses a ON u.address_id = a.address_id
        LEFT JOIN entities e ON a.entity_id = e.entity_id
        LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
        LEFT JOIN persons person ON n.creator_id = person.person_id
        ) subquery
        WHERE subquery.target_type = %s AND subquery.target_id = %s
    """,
    'get_unit_deals': """
       select * from (
            SELECT
                d.deal_id,
                d.unit_id,
                d.start_date,
                d.expiry,
                d.move_in,
                d.move_out,
                d.gross,
                d.actual_rent,
                d.term,
                d.concession,
                d.deal_status,
                d.tenant_ids,
                (
                    SELECT JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'person_id', p.person_id,
                            'first_name', p.first_name,
                            'last_name', p.last_name,
                            'email_address', p.email_address,
                            'phone_number', p.phone_number
                        )
                    )
                    FROM persons p
                    WHERE JSON_CONTAINS(d.tenant_ids, CAST(p.person_id AS JSON))
                ) as tenant_info,
                p.portfolio,
                e.entity,
                a.address,
                CASE 
                    WHEN d.created_at = (
                        SELECT MAX(created_at) 
                        FROM deals d2 
                        WHERE d2.unit_id = d.unit_id
                    ) THEN TRUE
                    ELSE FALSE
                END as current_deal
            FROM deals d
            LEFT JOIN units u ON d.unit_id = u.unit_id
            LEFT JOIN addresses a ON u.address_id = a.address_id
            LEFT JOIN entities e ON a.entity_id = e.entity_id
            LEFT JOIN portfolios p ON e.portfolio_id = p.portfolio_id
            
        ) subquery
        WHERE subquery.unit_id = %s
    """,
    'get_streeteasy_data': """
    select * from (
        SELECT * FROM streeteasy_units
        ) subquery
        WHERE 1=1
    """,
    'get_reports': """
        SELECT r1.*
        FROM reports r1
        INNER JOIN (
            SELECT name, created_at
            FROM (
                SELECT name, created_at,
                       ROW_NUMBER() OVER (PARTITION BY name ORDER BY created_at DESC) as rn
                FROM reports
            ) ranked
            WHERE rn <= 10
        ) r2 ON r1.name = r2.name AND r1.created_at = r2.created_at
        ORDER BY r1.name, r1.created_at DESC
    """,

}
#   d.prev_gross,
            # d.prev_payable,
            # d.price_suggestion,


# Address	Unit	Lease Type	Bed	Bath	sqft	Status	
# Prev Gross	Prev Payable	Price Suggestion	Gross	Actual Rent	Actual Rent Type	
# Conc	Term	Move Out	Lease Start	Move-In	
# Expiry	Item ID	VNY Notes	Landlord Notes
def run_query_system(connection, credentials, query_id, target_type=None, target_id=None, unit_id=None, filters=None):
    cursor = None
    
    def convert_mysql_types_to_readable(col_types):
        """Convert MySQL field type constants to readable type names"""
        mysql_type_mapping = {
            1: 'boolean',      # FIELD_TYPE_TINY (tinyint, boolean)
            3: 'integer',      # FIELD_TYPE_LONG (int, bigint)
            8: 'integer',      # FIELD_TYPE_LONGLONG (bigint)
            10: 'datetime',    # FIELD_TYPE_DATE
            12: 'datetime',    # FIELD_TYPE_DATETIME
            245: 'json',       # FIELD_TYPE_JSON
            246: 'decimal',    # FIELD_TYPE_NEWDECIMAL (decimal, numeric)
            252: 'string',     # FIELD_TYPE_BLOB (text, varchar, char)
            253: 'string',     # FIELD_TYPE_VAR_STRING (varchar)
            254: 'string',     # FIELD_TYPE_STRING (char, binary)
        }
        
        readable_types = {}
        for column, type_code in col_types.items():
            readable_types[column] = mysql_type_mapping.get(type_code, f'unknown_{type_code}')
        
        return readable_types
    
    try:
        if unit_id:
            params = [unit_id]
        elif target_type:
            params = [target_type, target_id]
        else:
            params = []

        query = queries[query_id]
        if filters is None:
            filters = {}

        # Apply data filters from credentials    -     DOnt add credential filters for soem queries
        if query_id not in ['get_reports']:
            data_filters = credentials.get("data_filters", [])
            for column, value in data_filters:
                if value and value not in ["Any", "", "undefined", "-", "0", " "] and column is not None and 'Any' not in value:
                    query += f" AND subquery.{column} = %s"
                    params.append(value)

        # Apply additional filters from request
        if filters:
            for column, value in filters.items():
                if value and value not in ["Any", "", "undefined", "-", "0", " "] and column is not None and 'Any' not in value:
                    if column == 'agg_filter':
                        #   Add filters for the report
                        if value.lower() == '4/1/25 onwards':
                            # 4/1/25 Onwards: Lease start > 4/1/25 OR Current deals move out is populated
                            query += """
                                AND (subquery.start_date > '2025-04-01' 
                                     OR subquery.move_out IS NOT NULL)
                            """
                        elif value.lower() == 'renewal horizon':
                            # Renewal Horizon: Expiry within next 120 days or the past, for the current deal
                            query += """
                                AND subquery.expiry IS NOT NULL
                                AND subquery.expiry <= DATE_ADD(CURRENT_DATE, INTERVAL 120 DAY)
                            """
                    else:
                        query += f" AND LOWER(subquery.{column}) LIKE LOWER(%s)"
                        params.append(f"%{value}%")

        # Execute the main query
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)
        data = cursor.fetchall()
        # Get column info from the cursor description after fetching data
        col_types = {}
        readable_col_types = {}
        if cursor.description:
            col_types = {desc[0]: desc[1] for desc in cursor.description}
            readable_col_types = convert_mysql_types_to_readable(col_types)

        # Check if we're in a Flask context
        try:
            from flask import has_request_context, jsonify
            if has_request_context():
                return jsonify({
                    "status": "success", 
                    "count": len(data), 
                    "data": data,
                    "col_types": readable_col_types,
                    "raw_col_types": col_types  # Keep original for debugging
                })
            else:
                # Return raw dict when called outside Flask context
                return {
                    "status": "success", 
                    "count": len(data), 
                    "data": data,
                    "col_types": readable_col_types,
                    "raw_col_types": col_types
                }
        except:
            # Fallback: return raw dict
            return {
                "status": "success", 
                "count": len(data), 
                "data": data,
                "col_types": readable_col_types,
                "raw_col_types": col_types
            }

    except Exception as e:
        print(f"Error in run_query_system: {e}")
        raise e
    finally:
        # Clean up cursors properly
        if cursor:
            try:
                cursor.close()
            except:
                pass

@data_bp.route('/run_query', methods=['GET'])
@with_db_connection
def run_query(connection, credentials):
    query_id = request.args.get('query_id')
    target_type = request.args.get('target_type', '')
    target_id = request.args.get('target_id', '')
    unit_id = request.args.get('unit_id', '')
    filters = json.loads(request.args.get('filters', '{}'))

    return run_query_system(connection, credentials, query_id, target_type, target_id, unit_id, filters)

@data_bp.route('/create_note', methods=['POST'])
def create_note():
    try:
        data = request.get_json(silent=True) or {}
        # Fallback to query params if not in JSON
        target_type = data.get('target_type')
        if target_type is None:
            target_type = request.args.get('target_type')

        target_id = data.get('target_id')
        if target_id is None:
            target_id = request.args.get('target_id')

        note = data.get('note')
        if note is None:
            note = request.args.get('note')

        creator_id = data.get('creator_id')
        if creator_id is None:
            creator_id = request.args.get('creator_id')

        tag_ids = data.get('tag_ids')
        if tag_ids is None:
            tag_ids = request.args.get('tag_ids')

        # Add this debug print here:
        print("DEBUG types:", type(target_type), type(target_id), type(note), type(creator_id))
        print("DEBUG values:", target_type, target_id, note, creator_id)

        if target_type in [None, ""] or target_id in [None, ""] or note in [None, ""] or creator_id in [None, ""]:
            print("FAILED CHECK", target_type, target_id, note, creator_id)
            return jsonify({"status": "error", "message": "target_type, target_id, note, and creator_id are required"}), 400

        db_result = get_db_connection()
        if db_result["status"] != "connected":
            return jsonify({"status": "error", "message": db_result.get("message", "Database connection failed")}), 500
        connection = db_result["connection"]
        cursor = connection.cursor()

        if tag_ids is not None:
            tag_ids_json = json.dumps(tag_ids)
        else:
            tag_ids_json = None

        query = """
            INSERT INTO notes (target_type, target_id, note, creator_id, tag_ids)
            VALUES (%s, %s, %s, %s, %s)
        """

        print(query, (target_type, target_id, note, creator_id, tag_ids_json))
        cursor.execute(query, (target_type, target_id, note, creator_id, tag_ids_json))
        connection.commit()
        note_id = cursor.lastrowid
        cursor.close()
        connection.close()
        return jsonify({"status": "success", "note_id": note_id})
    except Exception as e:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
        return jsonify({"status": "error", "message": str(e)}), 500          



def create_report_record(connection, credentials, name, status='pending'):
    """Create a new report record in the database"""
    try:
        cursor = connection.cursor(dictionary=True)
        
        query = """
        INSERT INTO reports (name, created_at, status)
        VALUES (%s, NOW(), %s)
        """
        
        cursor.execute(query, (name, status))
        connection.commit()
        
        # Get the inserted report_id
        report_id = cursor.lastrowid
        cursor.close()
        
        return {
            'status': 'success',
            'report_id': report_id,
            'message': f'Report record created with ID: {report_id}'
        }
        
    except Exception as e:
        connection.rollback()
        return {
            'status': 'error',
            'message': f'Error creating report record: {str(e)}'
        }

def update_report_record(connection, credentials, report_id, status=None, file_path=None):
    """Update an existing report record"""
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Build dynamic query based on what needs to be updated
        updates = []
        params = []
        
        if status:
            updates.append("status = %s")
            params.append(status)
            
        if file_path:
            updates.append("file_path = %s")
            params.append(file_path)
            
        if not updates:
            return {'status': 'error', 'message': 'No updates provided'}
            
        query = f"UPDATE reports SET {', '.join(updates)} WHERE report_id = %s"
        params.append(report_id)
        
        cursor.execute(query, params)
        connection.commit()
        cursor.close()
        
        return {
            'status': 'success',
            'message': f'Report {report_id} updated successfully'
        }
        
    except Exception as e:
        connection.rollback()
        return {
            'status': 'error',
            'message': f'Error updating report record: {str(e)}'
        }

@data_bp.route('/reports/create', methods=['POST'])
@with_db_connection
def create_report_endpoint(connection, credentials):
    """API endpoint to create a report record"""
    try:
        data = request.get_json()
        name = data.get('name')
        status = data.get('status', 'pending')
        
        if not name:
            return jsonify({'status': 'error', 'message': 'Report name is required'}), 400
            
        result = create_report_record(connection, credentials, name, status)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@data_bp.route('/reports/update/<int:report_id>', methods=['PUT'])
@with_db_connection
def update_report_endpoint(connection, credentials, report_id):
    """API endpoint to update a report record"""
    try:
        data = request.get_json()
        status = data.get('status')
        file_path = data.get('file_path')
        
        result = update_report_record(connection, credentials, report_id, status, file_path)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500          
