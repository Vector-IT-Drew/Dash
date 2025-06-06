import os
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from jinja2 import Environment, FileSystemLoader, select_autoescape
try:
    from weasyprint import HTML as WeasyHTML
    WEASYPRINT = True
except ImportError:
    import pdfkit
    WEASYPRINT = False
from .data_processor import get_streeteasy_data, get_comparison_tables, get_ytd_ppsf_data, get_weekly_trends, calculate_general_metrics, preprocess_df

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
                        Format: [{'name': 'Building Name', 'filter': {'address': 'Address'}}]
                        If None, all charts show market data with bedroom breakdowns
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
    
    # Step 3: Process data with optional address filters
    data = {
        'comparison_tables': get_comparison_tables(df),
        'ytd_ppsf': get_ytd_ppsf_data(df, address_filters),
        'weekly_trends': get_weekly_trends(df),
        'general_metrics': calculate_general_metrics(df)
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

    # Step 6: Concatenate HTML
    full_html = intro_html + '<div style="page-break-after: always;"></div>' + comparison_html + '<div style="page-break-after: always;"></div>' + ytd_ppsf_html + '<div style="page-break-after: always;"></div>' + chart_table_html
    html_path = os.path.join(OUTPUT_DIR, f'{report_name}_debug.html')
    with open(html_path, 'w') as f:
        f.write(full_html)

    # Step 7: Convert HTML to PDF
    pdf_path = os.path.join(OUTPUT_DIR, f"{report_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf")
    if WEASYPRINT:
        WeasyHTML(string=full_html, base_url=OUTPUT_DIR).write_pdf(pdf_path)
    else:
        pdfkit.from_file(html_path, pdf_path)
    print(f"PDF generated successfully: {pdf_path}")

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

if __name__ == "__main__":
    generate_report('full_market_report')

#python3 -m Services.Reports.generate_report

