import os
import pandas as pd
from weasyprint import HTML, CSS
from jinja2 import Environment, FileSystemLoader
from Services.Reports.data_processor import get_streeteasy_data, process_all_data, get_template_data
from Services.Functions.Dropbox import save_report_to_dropbox
from datetime import datetime

# Directories
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'templates')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')

# Ensure directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMPLATE_DIR, exist_ok=True)

# Setup Jinja2 environment
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

def get_shared_css():
    """Load shared CSS"""
    css_path = os.path.join(TEMPLATE_DIR, 'shared_styles.css')
    try:
        with open(css_path, 'r', encoding='utf-8') as f:
            return f.read()
    except:
        return ""

def generate_report(report_config):
    """Generate a report based on configuration"""
    print(f"Fetching data for {report_config['name']}...")
    
    # Step 1: Create database record with pending status
    report_id = None
    connection = None
    credentials = None
    
    try:
        from Services.Database.Connect import get_db_connection
        from Services.Database.Data import create_report_record, update_report_record
        
        print("DEBUG: Attempting to connect to database...")
        db_result = get_db_connection()
        print(f"DEBUG: Database connection result: {db_result['status']}")
        
        if db_result["status"] == "connected":
            connection = db_result["connection"]
            credentials = db_result.get("credentials", {}) or {}
            print(f"DEBUG: Got connection and credentials: {type(connection)}")
            
            # Create pending report record
            print("DEBUG: Creating report record...")
            create_result = create_report_record(
                connection, 
                credentials, 
                report_config.get('title', 'Unknown Report Title'), 
                'pending'
            )
            print(f"DEBUG: Create result: {create_result}")
            
            if create_result['status'] == 'success':
                report_id = create_result['report_id']
                print(f"Created report record with ID: {report_id}")
            else:
                print(f"Failed to create report record: {create_result['message']}")
    except Exception as e:
        print(f"Error creating report record: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        # Step 2: Get raw data once
        raw_df = get_streeteasy_data().sample(500)
        
        # Step 3: Process all data once
        processed_data = process_all_data(raw_df)
        
        # Step 4: Generate pages using processed data
        all_html_content = []
        
        for i, page_config in enumerate(report_config['pages'], 1):
            template_name = page_config['template'].replace('.html', '')
            print(f"Processing page {i}: {page_config['template']}")
            
            # Get template-specific data from processed data
            template_data = get_template_data(
                template_name, 
                processed_data, 
                **page_config.get('config', {})
            )
            
            # Add common data
            template_data.update({
                'report_title': report_config.get('title', 'Unknown Report Title'),
                'generated_date': datetime.now().strftime('%B %d, %Y'),
                'page_number': i,
                'total_pages': len(report_config['pages'])
            })
            
            # Render template
            template = env.get_template(page_config['template'])
            html_content = template.render(**template_data)
            
            # Add page break after each page except the last one
            if i < len(report_config['pages']):
                html_content += '<div style="page-break-after: always;"></div>'
            
            all_html_content.append(html_content)
        
        # Combine all pages
        combined_html = '\n'.join(all_html_content)
        
        # Generate filename with timestamp using dash
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        filename = f"{report_config['name']}-{timestamp}.pdf"
        
        # Save debug HTML
        debug_path = os.path.join(OUTPUT_DIR, f"{report_config['name']}_debug.html")
        with open(debug_path, 'w', encoding='utf-8') as f:
            f.write(combined_html)
        print(f"Debug HTML saved: {debug_path}")
        
        # Generate PDF with timestamped filename
        css_content = get_shared_css()
        css = CSS(string=css_content)
        
        pdf_path = os.path.join(OUTPUT_DIR, filename)
        HTML(string=combined_html).write_pdf(pdf_path, stylesheets=[css])
        print(f"PDF generated successfully: {pdf_path}")
        
        # Upload to Dropbox
        try:
            dropbox_path = save_report_to_dropbox(pdf_path, report_config['name'])
            print(f"Report uploaded to Dropbox: {dropbox_path}")
            final_path = dropbox_path
        except Exception as e:
            print(f"Failed to upload to Dropbox: {e}")
            final_path = pdf_path  # Use local path if Dropbox fails
        
        # Step 5: Update database record with completed status and file path
        print(f"DEBUG: About to update database - report_id: {report_id}, connection: {connection is not None}")
        
        if report_id and connection:
            try:
                print(f"DEBUG: Calling update_report_record with report_id={report_id}, status='completed', path='{final_path}'")
                
                # Check if connection is still valid
                if connection.is_connected():
                    print("DEBUG: Connection is still active")
                else:
                    print("DEBUG: Connection is no longer active, reconnecting...")
                    db_result = get_db_connection()
                    if db_result["status"] == "connected":
                        connection = db_result["connection"]
                        credentials = db_result.get("credentials", {}) or {}
                        print("DEBUG: Reconnected successfully")
                    else:
                        print("DEBUG: Failed to reconnect")
                        return final_path
                
                update_result = update_report_record(
                    connection, 
                    credentials, 
                    report_id, 
                    'completed', 
                    final_path
                )
                
                print(f"DEBUG: Update result: {update_result}")
                
                if update_result['status'] == 'success':
                    print(f"✅ Updated report record {report_id} to completed")
                else:
                    print(f"❌ Failed to update report record: {update_result['message']}")
            except Exception as e:
                print(f"❌ Error updating report record: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"DEBUG: Skipping database update - report_id: {report_id}, connection exists: {connection is not None}")
        
        return final_path
        
    except Exception as e:
        # If report generation fails, update status to failed
        print(f"DEBUG: Report generation failed: {e}")
        if report_id and connection:
            try:
                print(f"DEBUG: Updating report {report_id} to failed status")
                update_report_record(connection, credentials, report_id, 'failed', None)
                print(f"Updated report record {report_id} to failed")
            except Exception as update_error:
                print(f"Failed to update report to failed status: {update_error}")
        raise e

# Report configurations
REPORT_CONFIGS = {
    'full_market_report': {
        'name': 'market_report',
        'title': 'NYC Rental Market Comp Report',
        'pages': [
            {'template': 'intro_page.html'},
            {'template': 'general_metrics.html'},
            {'template': 'weekly_price_review.html', 'config': {'bedroom_filter': [0, 1, 2]}}
        ]
    }
}

# Flask imports at the bottom to avoid circular import
try:
    from flask import request, jsonify
    from flask_cors import cross_origin
    from . import reports_bp

    @reports_bp.route('/generate_report', methods=['POST', 'OPTIONS'])
    @cross_origin(origins='*', methods=['POST', 'OPTIONS'], allow_headers=['Content-Type'])
    def generate_report_endpoint():
        """Generate a report"""
        try:
            data = request.get_json()
            
            if not data or 'report_name' not in data:
                return jsonify({'error': 'report_name required'}), 400
            
            report_name = data['report_name']
            
            if report_name not in REPORT_CONFIGS:
                return jsonify({'error': 'Invalid report name'}), 400
            
            # Generate the report
            config = REPORT_CONFIGS[report_name]
            pdf_path = generate_report(config)
            
            return jsonify({
                'success': True,
                'pdf_path': pdf_path
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500

except ImportError:
    # Flask not available when running standalone
    pass

if __name__ == "__main__":
    for report_name, config in REPORT_CONFIGS.items():
        if report_name == 'full_market_report':
            generate_report(config)