import os
import sys
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import matplotlib.pyplot as plt
import io
import base64
import pandas as pd
from .data_processor import get_streeteasy_data, calculate_general_metrics
import numpy as np

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
    from Reports.data_processor import get_streeteasy_data, calculate_general_metrics
except ImportError as e:
    print(f"Warning: Could not import database functions: {e}")

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- PAGE TEMPLATE FUNCTIONS ---
def draw_intro_page(c, title, subtitle=None):
    c.setFont("Helvetica-Bold", 32)
    c.setFillColor(colors.darkblue)
    c.drawCentredString(4.25*inch, 7*inch, title)
    c.setFont("Helvetica", 18)
    c.setFillColor(colors.grey)
    if subtitle:
        c.drawCentredString(4.25*inch, 6.3*inch, subtitle)
    c.setFont("Helvetica", 14)
    c.setFillColor(colors.black)
    c.drawCentredString(4.25*inch, 5.5*inch, f"Generated on: {datetime.now().strftime('%B %d, %Y')}")
    c.showPage()

def draw_text_page(c, heading, text):
    c.setFont("Helvetica-Bold", 22)
    c.setFillColor(colors.darkblue)
    c.drawString(1*inch, 10*inch, heading)
    c.setFont("Helvetica", 14)
    c.setFillColor(colors.black)
    text_object = c.beginText(1*inch, 9.5*inch)
    for line in text.split('\n'):
        text_object.textLine(line)
    c.drawText(text_object)
    c.showPage()

def draw_comparison_table_page(c, title, subtitle, multi_tables, table_columns):
    # Title
    c.setFont("Helvetica-Bold", 22)
    c.setFillColor(colors.darkblue)
    c.drawCentredString(4.25*inch, 10.2*inch, title)
    c.setFont("Helvetica", 13)
    c.setFillColor(colors.black)
    c.drawCentredString(4.25*inch, 9.7*inch, subtitle)

    # Table layout
    x0 = 0.7*inch
    y0 = 9.1*inch
    row_height = 0.32*inch
    col_widths = [1.0*inch, 1.0*inch, 1.0*inch, 1.0*inch, 1.0*inch]
    n_cols = len(table_columns)
    max_tables = min(4, len(multi_tables))
    table_height = 1.1*inch + row_height * 6  # header + up to 5 rows
    spacing = 0.25*inch
    for t, table in enumerate(multi_tables[:max_tables]):
        y_table = y0 - t * (table_height + spacing)
        # Section title
        c.setFont("Helvetica-Bold", 14)
        c.setFillColor(colors.darkblue)
        c.drawString(x0, y_table, table['title'])
        y_table -= 0.18*inch
        # Table background
        c.setFillColorRGB(0.92, 0.97, 1.0)
        c.roundRect(x0-0.05*inch, y_table-row_height*(len(table['rows'])+1)-0.05*inch, sum(col_widths)+0.1*inch, row_height*(len(table['rows'])+1)+0.1*inch, 7, fill=1, stroke=0)
        # Header
        c.setFillColor(colors.darkblue)
        c.rect(x0, y_table-row_height, sum(col_widths), row_height, fill=1, stroke=0)
        c.setFont("Helvetica-Bold", 11)
        c.setFillColor(colors.whitesmoke)
        for i, col in enumerate(table_columns):
            c.drawCentredString(x0 + sum(col_widths[:i]) + col_widths[i]/2, y_table-row_height+0.11*inch, str(col))
        # Data rows
        c.setFont("Helvetica", 10)
        for r, row in enumerate(table['rows'][:5]):
            y = y_table - row_height*(r+2)
            if r % 2 == 0:
                c.setFillColorRGB(0.85, 0.92, 1.0)
                c.rect(x0, y, sum(col_widths), row_height, fill=1, stroke=0)
            c.setFillColor(colors.black)
            for i, col in enumerate(table_columns):
                val = row.get(col, '')
                c.drawCentredString(x0 + sum(col_widths[:i]) + col_widths[i]/2, y + 0.11*inch, str(val))
        # Grid lines
        c.setStrokeColor(colors.black)
        for i in range(n_cols+1):
            c.line(x0 + sum(col_widths[:i]), y_table-row_height, x0 + sum(col_widths[:i]), y_table-row_height*(len(table['rows'])+1))
        for r in range(len(table['rows'])+2):
            c.line(x0, y_table-row_height*r, x0+sum(col_widths), y_table-row_height*r)
    c.showPage()

def draw_chart_page(c, heading, chart_func, chart_data):
    c.setFont("Helvetica-Bold", 22)
    c.setFillColor(colors.darkblue)
    c.drawString(1*inch, 10*inch, heading)
    # Generate chart image
    img_buf = chart_func(chart_data)
    if img_buf:
        c.drawImage(img_buf, 1*inch, 5*inch, width=6*inch, height=4*inch, preserveAspectRatio=True, mask='auto')
    c.showPage()

def draw_chart_table_page(c, heading, chart_func, chart_data, table_data, table_columns):
    c.setFont("Helvetica-Bold", 22)
    c.setFillColor(colors.darkblue)
    c.drawString(1*inch, 10*inch, heading)
    # Chart
    img_buf = chart_func(chart_data)
    if img_buf:
        c.drawImage(img_buf, 1*inch, 6.5*inch, width=3.5*inch, height=2.5*inch, preserveAspectRatio=True, mask='auto')
    # Table
    c.setFont("Helvetica-Bold", 12)
    c.setFillColor(colors.black)
    y = 6.2*inch
    col_widths = [2*inch for _ in table_columns]
    # Header
    for i, col in enumerate(table_columns):
        c.drawString(1*inch + sum(col_widths[:i]), y, str(col))
    y -= 0.25*inch
    c.setFont("Helvetica", 11)
    max_rows = 8
    for row in table_data[:max_rows]:
        for i, col in enumerate(table_columns):
            c.drawString(1*inch + sum(col_widths[:i]), y, str(row.get(col, '')))
        y -= 0.22*inch
        if y < 1*inch:
            break
    c.showPage()

# --- CHART GENERATION ---
def make_rent_chart(df):
    if df is None or df.empty:
        return None
    plt.figure(figsize=(6, 4))
    if 'date' in df.columns and 'rent' in df.columns:
        plt.plot(pd.to_datetime(df['date']), df['rent'], label='Rent', color='blue')
    elif 0 in df.columns:
        for col in [0, 1, 2, 3]:
            if col in df.columns:
                plt.plot(df.index, df[col], label=f"{col} BR")
        plt.legend()
    plt.title("Rent Trends")
    plt.xlabel("Date")
    plt.ylabel("Rent ($)")
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='PNG')
    plt.close()
    buf.seek(0)
    return buf

# --- MAIN REPORT GENERATION ---
PAGE_TEMPLATES = {
    'intro': draw_intro_page,
    'text': draw_text_page,
    'comparison_table': draw_comparison_table_page,
    'chart': draw_chart_page,
    'chart_table': draw_chart_table_page,
}

REPORT_CONFIGS = {
    'full_market_report': [
        {'type': 'intro', 'title': 'NYC Rental Market Comp Report', 'subtitle': None},
        {'type': 'comparison_table',
         'title': 'UES',
         'subtitle': 'Comped by No Fee listings with elevator, doorman, & laundry',
         'multi_tables_key': 'multi_tables',
         'table_columns': ['Market', 'Avg Price', 'Avg SqFt', 'Avg PSf', 'Count']},
        {'type': 'text', 'heading': 'Summary', 'text_key': 'summary_text'},
    ],
}

def generate_table_rows(df):
    # Group by bedrooms
    grouped = df.groupby('bedrooms')
    table_rows = []
    for bedrooms, group in grouped:
        try:
            avg_price = group['listed_price'].mean()
            avg_sqft = group['size_sqft'].mean()
            avg_ppsf = group['ppsf'].mean()
            count = len(group)
            row = {
                'Market': int(bedrooms) if pd.notnull(bedrooms) else '-',
                'Avg Price': f"${avg_price:,.0f}" if pd.notnull(avg_price) else '-',
                'Avg SqFt': f"${avg_sqft:,.0f}" if pd.notnull(avg_sqft) else '-',
                'Avg PSf': f"${avg_ppsf:,.2f}" if pd.notnull(avg_ppsf) and np.isfinite(avg_ppsf) else '-',
                'Count': int(count) if pd.notnull(count) else '-',
            }
        except Exception as e:
            row = {'Market': bedrooms, 'Avg Price': '-', 'Avg SqFt': '-', 'Avg PSf': '-', 'Count': '-'}
        table_rows.append(row)
    # Sort by Market (bedrooms)
    table_rows = sorted(table_rows, key=lambda x: (x['Market'] if isinstance(x['Market'], int) else 99))
    return table_rows

def get_report_data(filters=None):
    df = get_streeteasy_data()
    df = df.copy()
    # Convert to float, coerce errors to NaN
    df['listed_price'] = pd.to_numeric(df['listed_price'], errors='coerce')
    df['size_sqft'] = pd.to_numeric(df['size_sqft'], errors='coerce')
    df['bedrooms'] = pd.to_numeric(df['bedrooms'], errors='coerce')
    # Only use rows with valid, positive price and sqft and bedrooms
    df = df[
        (df['listed_price'].notnull()) & (df['size_sqft'].notnull()) & (df['bedrooms'].notnull()) &
        (df['listed_price'] > 0) & (df['size_sqft'] > 0)
    ]
    # Calculate PPSF
    df['ppsf'] = df['listed_price'] / df['size_sqft']
    # Remove any rows where PPSF is not finite
    df = df[np.isfinite(df['ppsf'])]
    # Apply filter if provided
    if filters:
        for k, v in filters.items():
            df = df[df[k] == v]

    all_data = df[df['is_no_fee'] == 1] if 'is_no_fee' in df.columns else df
    table1 = generate_table_rows(all_data)
    table2 = generate_table_rows(df[(df['address'] == '3 Sutton Street') & (df['source'].str.lower() == 'vector')])

    table2 = generate_table_rows(df[(df['source'].str.lower() == 'vector')])

    summary_text = "This page shows grouped market stats for all no-fee listings, and for 3 Sutton Street (Vector)."
    return {
        'multi_tables': [
            {'title': 'Market Data', 'rows': table1},
            {'title': '3 Sutton Street (Vector)', 'rows': table2},
        ],
        'summary_text': summary_text
    }

def generate_report(report_name):
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

    # Step 2: Prepare data
    data = get_report_data()

    # Step 3: Generate PDF
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = f"{report_name}-{timestamp}.pdf"
    pdf_path = os.path.join(OUTPUT_DIR, filename)
    c = canvas.Canvas(pdf_path, pagesize=letter)

    # Step 4: Render pages as per config
    config = REPORT_CONFIGS.get(report_name)
    if not config:
        raise ValueError(f"No report config found for {report_name}")
    for page in config:
        page_type = page['type']
        func = PAGE_TEMPLATES[page_type]
        if page_type == 'intro':
            func(c, page.get('title', ''), page.get('subtitle', ''))
        elif page_type == 'text':
            text = data.get(page.get('text_key', ''), '')
            func(c, page.get('heading', ''), text)
        elif page_type == 'comparison_table':
            multi_tables = data.get(page.get('multi_tables_key', ''), [])
            table_columns = page.get('table_columns', [])
            func(c, page.get('title', ''), page.get('subtitle', ''), multi_tables, table_columns)
        elif page_type == 'chart':
            chart_data = data.get(page.get('chart_data_key', ''), pd.DataFrame())
            func(c, page.get('heading', ''), page.get('chart_func'), chart_data)
        elif page_type == 'chart_table':
            chart_data = data.get(page.get('chart_data_key', ''), pd.DataFrame())
            table_data = data.get(page.get('table_data_key', ''), [])
            table_columns = page.get('table_columns', [])
            func(c, page.get('heading', ''), page.get('chart_func'), chart_data, table_data, table_columns)
    c.save()
    print(f"PDF generated successfully: {pdf_path}")

    # Step 5: Upload to Dropbox
    final_path = pdf_path
    try:
        dropbox_path = save_report_to_dropbox(pdf_path, report_name)
        print(f"Report uploaded to Dropbox: {dropbox_path}")
        final_path = dropbox_path
    except Exception as e:
        print(f"Failed to upload to Dropbox: {e}")
        final_path = pdf_path

    # Step 6: Update DB record
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
    # Example usage
    generate_report('full_market_report')

#python3 -m Services.Reports.generate_report