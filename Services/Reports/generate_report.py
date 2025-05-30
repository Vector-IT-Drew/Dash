import os
import pandas as pd
from weasyprint import HTML, CSS
from jinja2 import Environment, FileSystemLoader
from Services.Reports.data_processor import get_streeteasy_data, process_all_data, get_template_data
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
    
    # Step 1: Get raw data once
    raw_df = get_streeteasy_data().sample(500)
    
    # Step 2: Process all data once
    processed_data = process_all_data(raw_df)
    
    # Step 3: Generate pages using processed data
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
            'report_title': report_config.get('title', 'Market Report'),
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
    
    # Save debug HTML
    debug_path = os.path.join(OUTPUT_DIR, f"{report_config['name']}_debug.html")
    with open(debug_path, 'w', encoding='utf-8') as f:
        f.write(combined_html)
    print(f"Debug HTML saved: {debug_path}")
    
    # Generate PDF
    css_content = get_shared_css()
    css = CSS(string=css_content)
    
    pdf_path = os.path.join(OUTPUT_DIR, f"{report_config['name']}.pdf")
    HTML(string=combined_html).write_pdf(pdf_path, stylesheets=[css])
    print(f"PDF generated successfully: {pdf_path}")

# Report configurations
REPORT_CONFIGS = {
    'full_market_report': {
        'name': 'market_report',
        'title': 'NYC Rental Market Report',
        'pages': [
            {'template': 'intro_page.html'},
            {'template': 'general_metrics.html'},
            {'template': 'weekly_price_review.html', 'config': {'bedroom_filter': [0, 1, 2]}}
        ]
    },
    'simple_report': {
        'name': 'price_review_only',
        'title': 'Weekly Price Review',
        'pages': [
            {'template': 'weekly_price_review.html', 'config': {'bedroom_filter': [0, 1, 2]}}
        ]
    }
}

if __name__ == "__main__":
    for report_name, config in REPORT_CONFIGS.items():
        generate_report(config)
