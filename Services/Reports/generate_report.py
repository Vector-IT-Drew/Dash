import os
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from jinja2 import Environment, FileSystemLoader, select_autoescape

# Don't import WeasyPrint at module level - do it when needed
WEASYPRINT = None

def get_pdf_generator():
    """Get available PDF generator, checking at runtime - prioritize pdfkit"""
    global WEASYPRINT
    if WEASYPRINT is not None:
        return WEASYPRINT
    
    # Try pdfkit first since we have wkhtmltopdf dependencies
    try:
        import pdfkit
        # Test if wkhtmltopdf is available
        config = pdfkit.configuration()
        WEASYPRINT = 'pdfkit'
        print("pdfkit loaded successfully with wkhtmltopdf")
        return WEASYPRINT
    except (ImportError, OSError) as e:
        print(f"pdfkit not available: {e}")
        
        # Fallback to WeasyPrint if pdfkit fails
        try:
            from weasyprint import HTML as WeasyHTML
            WEASYPRINT = 'weasyprint'
            print("Using WeasyPrint as fallback")
            return WEASYPRINT
        except ImportError as e2:
            print(f"WeasyPrint also not available: {e2}")
            WEASYPRINT = None
            return WEASYPRINT

from .data_processor import get_streeteasy_data, get_comparison_tables, get_ytd_ppsf_data, get_weekly_trends, calculate_general_metrics, preprocess_df, get_inventory_data

# Add the Services directory to the path so we can import modules
current_dir = os.path.dirname(os.path.abspath(__file__))
services_dir = os.path.dirname(current_dir)
if services_dir not in sys.path:
    sys.path.append(services_dir)

# Import database functions
try:
    from Database.Data import create_report_record, update_report_record
    from Database.Connect import get_db_connection
    from Functions.Dropbox import save_report_to_dropbox
except ImportError as e:
    print(f"Warning: Could not import database functions: {e}")

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

current_dir = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(current_dir, 'templates')
env = Environment(
    loader=FileSystemLoader(templates_dir),
    autoescape=select_autoescape(['html', 'xml'])
)

def generate_report(report_name, address_filters=None):
    """
    Generate a PDF report
    
    Args:
        report_name: Name of the report
        address_filters: Optional list of address filters for YTD PPSF charts
                        Format: [
                            {'name': 'Full Market Data', 'filter': {}},
                            {'name': '3 Sutton Place', 'filter': {'address': '3 Sutton Place'}},
                            {'name': '5 Sutton Place', 'filter': {'address': '5 Sutton Place'}},
                            {'name': 'Other Buildings', 'filter': {'address': 'Other'}}
                        ]
                        If None, uses default market breakdown for testing
    """
    # Step 1: Create DB record
    report_id = None
    connection = None
    try:
        db_result = get_db_connection()
        if db_result["status"] == "connected":
            connection = db_result["connection"]
            result = create_report_record(connection, None, report_name, 'generating')
            if result['status'] == 'success':
                report_id = result['report_id']
                print(f"Created report record with ID: {report_id}")
            else:
                print(f"Failed to create report record: {result['message']}")
    except Exception as e:
        print(f"Failed to connect to database: {e}")

    # Step 2: Load and preprocess data ONCE
    df = get_streeteasy_data()
    df = preprocess_df(df)
    
    # Step 3: Process data with dynamic address filters
    data = {
        'comparison_tables': get_comparison_tables(df),
        'ytd_ppsf': get_ytd_ppsf_data(df, address_filters),  # Pass through address filters
        'weekly_trends': get_weekly_trends(df),
        'general_metrics': calculate_general_metrics(df),
        'inventory_data': get_inventory_data()  # Default to 30 units with compact layout
    }

    # Step 4: Generate chart images (YTD PPSF and Weekly Trends)
    # (You can add chart image generation here if needed, or do it in data_processor)

    # Step 5: Render HTML pages
    intro_html = env.get_template('intro.html').render(
        title='NYC Rental Market Comp Report',
        subtitle='Comprehensive Market Analysis',
        date=datetime.now().strftime('%B %d, %Y')
    )
    comparison_html = env.get_template('comparison_tables.html').render(
        tables=data['comparison_tables'],
        page_title=f'Comp Report {datetime.now().strftime("%B %d, %Y")}',
        subtitle='Comped by No Fee listings with elevator, doorman, & laundry',
    )
    
    ytd_ppsf_html = env.get_template('ytd_ppsf_trends.html').render(
        **data['ytd_ppsf']
    )
    
    chart_table_html = env.get_template('chart_table.html').render(
        **data['weekly_trends']
    )
    
    # Render inventory page (5th page) with error handling
    inventory_html = ""
    try:
        print(f"Inventory data loaded: {data['inventory_data']['total_count']} units")
        print(f"Inventory data keys: {data['inventory_data'].keys()}")
        
        inventory_html = env.get_template('inventory_report.html').render(
            **data['inventory_data'],
            report_title='Inventory Report',
            date=datetime.now().strftime('%B %d, %Y')
        )
        print("Inventory HTML rendered successfully")
        print(f"Inventory HTML length: {len(inventory_html)} characters")
        
    except Exception as e:
        print(f"Error rendering inventory HTML: {e}")
        import traceback
        traceback.print_exc()
        # Create a simple fallback inventory page
        inventory_html = f"""
        <div style="text-align: center; padding: 50px;">
            <h1>Inventory Report</h1>
            <p>Error loading inventory data: {str(e)}</p>
            <p>Total units available: {data['inventory_data'].get('total_count', 'Unknown')}</p>
        </div>
        """

    # Step 6: Concatenate HTML - try without inventory first to test
    try:
        # First test without inventory
        basic_html = (intro_html + 
                    '<div style="page-break-after: always;"></div>' + 
                    comparison_html + 
                    '<div style="page-break-after: always;"></div>' + 
                    ytd_ppsf_html + 
                    '<div style="page-break-after: always;"></div>' + 
                    chart_table_html)
        
        print("Basic HTML concatenation successful")
        
        # Now add inventory
        full_html = (basic_html + 
                    '<div style="page-break-after: always;"></div>' + 
                    inventory_html)
        
        print("Full HTML with inventory concatenation successful")
        print(f"Full HTML length: {len(full_html)} characters")
        
    except Exception as e:
        print(f"Error in HTML concatenation: {e}")
        # Fall back to basic HTML without inventory
        full_html = basic_html

    html_path = os.path.join(OUTPUT_DIR, f'{report_name}_debug.html')
    with open(html_path, 'w') as f:
        f.write(full_html)
    print(f"Debug HTML saved to: {html_path}")

    # Step 7: Convert HTML to PDF with error handling
    pdf_path = os.path.join(OUTPUT_DIR, f"{report_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf")
    
    pdf_generator = get_pdf_generator()
    
    if pdf_generator is None:
        print("ERROR: No PDF generation libraries available")
        print("Please install WeasyPrint: pip install weasyprint")
        print("Or install wkhtmltopdf for pdfkit")
        print("Returning HTML file path instead")
        return html_path
    
    try:
        if pdf_generator == 'weasyprint':
            try:
                from weasyprint import HTML as WeasyHTML
                print("Using WeasyPrint for PDF conversion...")
                
                # Test: Generate inventory page standalone to check for issues
                try:
                    print("Testing inventory page standalone...")
                    inventory_test_path = os.path.join(OUTPUT_DIR, f"inventory_test_{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf")
                    WeasyHTML(string=inventory_html, base_url=OUTPUT_DIR).write_pdf(inventory_test_path)
                    inventory_size = os.path.getsize(inventory_test_path)
                    print(f"Standalone inventory PDF created: {inventory_size} bytes")
                except Exception as inv_error:
                    print(f"Standalone inventory PDF failed: {inv_error}")
                    
                # Test: Generate basic report without inventory
                try:
                    print("Testing basic report without inventory...")
                    basic_test_html = (intro_html + 
                                    '<div style="page-break-after: always;"></div>' + 
                                    comparison_html + 
                                    '<div style="page-break-after: always;"></div>' + 
                                    ytd_ppsf_html + 
                                    '<div style="page-break-after: always;"></div>' + 
                                    chart_table_html)
                    basic_test_path = os.path.join(OUTPUT_DIR, f"basic_test_{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf")
                    WeasyHTML(string=basic_test_html, base_url=OUTPUT_DIR).write_pdf(basic_test_path)
                    basic_size = os.path.getsize(basic_test_path)
                    print(f"Basic report PDF created: {basic_size} bytes")
                except Exception as basic_error:
                    print(f"Basic report PDF failed: {basic_error}")
                
                # Now try the full report
                WeasyHTML(string=full_html, base_url=OUTPUT_DIR).write_pdf(pdf_path)
                
            except Exception as weasy_error:
                print(f"WeasyPrint failed due to system dependencies: {weasy_error}")
                print("Falling back to pdfkit...")
                # Fall back to pdfkit
                try:
                    import pdfkit
                    pdfkit.from_file(html_path, pdf_path)
                except OSError as e:
                    if "wkhtmltopdf" in str(e):
                        print("ERROR: wkhtmltopdf not found and WeasyPrint system deps missing")
                        print("Returning HTML file instead")
                        return html_path
                    else:
                        raise e
                except ImportError:
                    print("No PDF generators available. Returning HTML file")
                    return html_path
        else:  # pdfkit
            print("Using pdfkit for PDF conversion...")
            try:
                import pdfkit
                
                # Configure pdfkit options for better PDF quality
                options = {
                    'page-size': 'A4',
                    'margin-top': '0.75in',
                    'margin-right': '0.75in',
                    'margin-bottom': '0.75in',
                    'margin-left': '0.75in',
                    'encoding': "UTF-8",
                    'no-outline': None,
                    'enable-local-file-access': None,
                    'print-media-type': None
                }
                
                # Use xvfb-run on Linux for headless operation if available
                config = None
                try:
                    import subprocess
                    result = subprocess.run(['which', 'xvfb-run'], capture_output=True)
                    if result.returncode == 0:
                        # xvfb-run is available, configure pdfkit to use it
                        config = pdfkit.configuration(wkhtmltopdf='xvfb-run -a wkhtmltopdf')
                        print("Using xvfb-run for headless PDF generation")
                except:
                    pass  # Use default configuration
                
                if config:
                    pdfkit.from_file(html_path, pdf_path, options=options, configuration=config)
                else:
                    pdfkit.from_file(html_path, pdf_path, options=options)
                    
            except OSError as e:
                if "wkhtmltopdf" in str(e):
                    print("ERROR: wkhtmltopdf not found. Please install it:")
                    print("Ubuntu/Debian: sudo apt-get install wkhtmltopdf")
                    print("CentOS/RHEL: sudo yum install wkhtmltopdf")
                    print("macOS: brew install wkhtmltopdf")
                    print("Or install WeasyPrint instead: pip install weasyprint")
                    print("Returning HTML file path instead")
                    return html_path
                else:
                    raise e
                    
        print(f"PDF generated successfully: {pdf_path}")
        
        # Check if PDF was actually created and has content
        if os.path.exists(pdf_path):
            file_size = os.path.getsize(pdf_path)
            print(f"PDF file size: {file_size} bytes")
            if file_size < 10000:  # Less than 10KB indicates a problem for a 5-page report
                print("WARNING: PDF file size is very small for a 5-page report with inventory")
                
                # Try generating without inventory as emergency fallback
                print("Generating fallback PDF without inventory due to small file size...")
                fallback_html = (intro_html + 
                            '<div style="page-break-after: always;"></div>' + 
                            comparison_html + 
                            '<div style="page-break-after: always;"></div>' + 
                            ytd_ppsf_html + 
                            '<div style="page-break-after: always;"></div>' + 
                            chart_table_html)
                
                fallback_path = os.path.join(OUTPUT_DIR, f"{report_name}-no-inventory-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf")
                WeasyHTML(string=fallback_html, base_url=OUTPUT_DIR).write_pdf(fallback_path)
                fallback_size = os.path.getsize(fallback_path)
                print(f"Fallback PDF without inventory: {fallback_size} bytes")
        else:
            print("ERROR: PDF file was not created")
            
    except Exception as e:
        print(f"Error generating PDF: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to generate a basic PDF without inventory as fallback
        try:
            print("Attempting fallback PDF generation without inventory...")
            basic_html = (intro_html + 
                        '<div style="page-break-after: always;"></div>' + 
                        comparison_html + 
                        '<div style="page-break-after: always;"></div>' + 
                        ytd_ppsf_html + 
                        '<div style="page-break-after: always;"></div>' + 
                        chart_table_html)
            
            fallback_path = os.path.join(OUTPUT_DIR, f"{report_name}-fallback-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf")
            if pdf_generator == 'weasyprint':
                try:
                    from weasyprint import HTML as WeasyHTML
                    WeasyHTML(string=basic_html, base_url=OUTPUT_DIR).write_pdf(fallback_path)
                except Exception as weasy_fallback_error:
                    print(f"WeasyPrint fallback also failed: {weasy_fallback_error}")
                    print("Trying pdfkit for fallback...")
                    try:
                        import pdfkit
                        basic_html_path = os.path.join(OUTPUT_DIR, f'{report_name}_basic.html')
                        with open(basic_html_path, 'w') as f:
                            f.write(basic_html)
                        pdfkit.from_file(basic_html_path, fallback_path)
                    except:
                        print("All PDF generation failed. Returning HTML file")
                        return html_path
            else:
                # Write basic HTML to file first
                basic_html_path = os.path.join(OUTPUT_DIR, f'{report_name}_basic.html')
                with open(basic_html_path, 'w') as f:
                    f.write(basic_html)
                import pdfkit
                pdfkit.from_file(basic_html_path, fallback_path)
            
            print(f"Fallback PDF generated: {fallback_path}")
            pdf_path = fallback_path
            
        except Exception as fallback_error:
            print(f"Fallback PDF generation also failed: {fallback_error}")
            return None

    # Step 8: Upload to Dropbox
    final_path = pdf_path
    try:
        dropbox_path = save_report_to_dropbox(pdf_path, report_name)
        print(f"Report uploaded to Dropbox: {dropbox_path}")
        final_path = dropbox_path
    except Exception as e:
        print(f"Failed to upload to Dropbox: {e}")
        final_path = pdf_path

    # Step 9: Update DB record
    if report_id and connection:
        try:
            update_result = update_report_record(connection, None, report_id, 'completed', final_path)
            print(f"Updated report record: {update_result}")
        except Exception as e:
            print(f"Failed to update report record: {e}")
        finally:
            if connection and connection.is_connected():
                connection.close()
    return final_path

# Add Flask endpoint
from flask import request, jsonify, send_file
from . import reports_bp

@reports_bp.route('/generate', methods=['GET', 'POST'])
def generate_report_endpoint():
    """Simple endpoint to generate reports"""
    try:
        # Get report name from query params or JSON
        if request.method == 'POST':
            data = request.get_json() or {}
            report_name = data.get('report_name', 'market_report')
        else:
            report_name = request.args.get('report_name', 'market_report')
        
        # Generate the report
        result_path = generate_report(report_name)
        
        if result_path:
            return jsonify({
                "status": "success",
                "message": "Report generated successfully",
                "file_path": result_path,
                "report_name": report_name
            })
        else:
            return jsonify({
                "status": "error", 
                "message": "Failed to generate report"
            }), 500
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@reports_bp.route('/download', methods=['GET'])
def download_report_endpoint():
    """Simple endpoint to download reports"""
    try:
        # Get filename from query params
        filename = request.args.get('filename')
        
        if not filename:
            return jsonify({
                "status": "error",
                "message": "filename parameter is required"
            }), 400
        
        # Build file path
        file_path = os.path.join(OUTPUT_DIR, filename)
        
        # Check if file exists
        if not os.path.exists(file_path):
            return jsonify({
                "status": "error",
                "message": f"File '{filename}' not found"
            }), 404
        
        # Send the file
        return send_file(file_path, as_attachment=True, download_name=filename)
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == "__main__":
    # Example of how to use the report:
    
    # Option 1: Standard market report with inventory (default)
    generate_report('Full_Market_Report_SMK')
    
    # Option 2: Custom address filters for market report
    # custom_filters = [
    #     {'name': 'Full Market Data', 'filter': {}},
    #     {'name': '3 Sutton Place', 'filter': {'address': '3 Sutton Place'}},
    #     {'name': '5 Sutton Place', 'filter': {'address': '5 Sutton Place'}},
    #     {'name': 'Other Buildings', 'filter': {'address': 'Other'}}
    # ]
    # generate_report('custom_address_report', address_filters=custom_filters)

#python3 -m Services.Reports.generate_report

