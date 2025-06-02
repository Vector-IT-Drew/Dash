import os
import sys
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

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

# Constants
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_sample_data():
    """Get sample data for the report"""
    return {
        'total_listings': 1250,
        'avg_rent': 3450,
        'vacancy_rate': 5.2,
        'market_trend': 'Increasing',
        'neighborhood_data': [
            {'name': 'Manhattan', 'avg_rent': 4200, 'listings': 450},
            {'name': 'Brooklyn', 'avg_rent': 2800, 'listings': 380},
            {'name': 'Queens', 'avg_rent': 2400, 'listings': 290},
            {'name': 'Bronx', 'avg_rent': 2100, 'listings': 130}
        ]
    }

def create_title_page(story, styles, title):
    """Create the title page of the report"""
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=30,
        alignment=1,  # Center alignment
        textColor=colors.darkblue
    )
    
    story.append(Paragraph(title, title_style))
    story.append(Spacer(1, 0.5*inch))
    
    # Date
    date_style = ParagraphStyle(
        'DateStyle',
        parent=styles['Normal'],
        fontSize=14,
        alignment=1,
        textColor=colors.grey
    )
    
    current_date = datetime.now().strftime("%B %d, %Y")
    story.append(Paragraph(f"Generated on: {current_date}", date_style))
    story.append(Spacer(1, 1*inch))

def create_metrics_section(story, styles, data):
    """Create the general metrics section"""
    story.append(Paragraph("Market Overview", styles['Heading2']))
    story.append(Spacer(1, 0.2*inch))
    
    # Create metrics table
    metrics_data = [
        ['Metric', 'Value'],
        ['Total Listings', f"{data['total_listings']:,}"],
        ['Average Rent', f"${data['avg_rent']:,}"],
        ['Vacancy Rate', f"{data['vacancy_rate']}%"],
        ['Market Trend', data['market_trend']]
    ]
    
    metrics_table = Table(metrics_data, colWidths=[2.5*inch, 2*inch])
    metrics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    story.append(metrics_table)
    story.append(Spacer(1, 0.5*inch))

def create_neighborhood_section(story, styles, data):
    """Create the neighborhood analysis section"""
    story.append(Paragraph("Neighborhood Analysis", styles['Heading2']))
    story.append(Spacer(1, 0.2*inch))
    
    # Create neighborhood table
    neighborhood_data = [['Neighborhood', 'Average Rent', 'Listings']]
    for neighborhood in data['neighborhood_data']:
        neighborhood_data.append([
            neighborhood['name'],
            f"${neighborhood['avg_rent']:,}",
            f"{neighborhood['listings']:,}"
        ])
    
    neighborhood_table = Table(neighborhood_data, colWidths=[2*inch, 1.5*inch, 1.5*inch])
    neighborhood_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    story.append(neighborhood_table)
    story.append(Spacer(1, 0.5*inch))

def generate_report(report_config):
    """Generate a report based on configuration"""
    
    # Step 1: Create database record first
    report_id = None
    connection = None
    
    try:
        db_result = get_db_connection()
        if db_result["status"] == "connected":
            connection = db_result["connection"]
            
            # Create report record
            result = create_report_record(connection, None, report_config['name'], 'generating')
            if result['status'] == 'success':
                report_id = result['report_id']
                print(f"Created report record with ID: {report_id}")
            else:
                print(f"Failed to create report record: {result['message']}")
                
    except Exception as e:
        print(f"Failed to connect to database: {e}")
    
    # Step 2: Generate timestamped filename
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = f"{report_config['name']}-{timestamp}.pdf"
    pdf_path = os.path.join(OUTPUT_DIR, filename)
    
    # Step 3: Create the PDF document
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []
    
    # Get sample data (in a real implementation, this would come from your database)
    data = get_sample_data()
    
    # Build the report content
    create_title_page(story, styles, report_config['title'])
    create_metrics_section(story, styles, data)
    create_neighborhood_section(story, styles, data)
    
    # Build the PDF
    doc.build(story)
    print(f"PDF generated successfully: {pdf_path}")
    
    # Step 4: Upload to Dropbox
    final_path = pdf_path
    try:
        dropbox_path = save_report_to_dropbox(pdf_path, report_config['name'])
        print(f"Report uploaded to Dropbox: {dropbox_path}")
        final_path = dropbox_path
    except Exception as e:
        print(f"Failed to upload to Dropbox: {e}")
        final_path = pdf_path  # Use local path if Dropbox fails
    
    # Step 5: Update database record with completed status and file path
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