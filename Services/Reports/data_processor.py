import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import json
import base64
import io
from collections import defaultdict
from Services.Database.Connect import get_db_connection
from Services.Database.Data import run_query_system

def get_streeteasy_data():
    """Fetch StreetEasy data from database"""
    try:
        db_result = get_db_connection()
        
        if db_result["status"] != "connected":
            raise Exception("Database connection failed")
        
        connection = db_result["connection"]
        credentials = db_result.get("credentials", {}) or {}
        
        result = run_query_system(
            connection=connection,
            credentials=credentials,
            query_id='get_streeteasy_data',
            target_type=None,
            target_id=None,
            unit_id=None,
            filters={}
        )
        
        # Handle response
        if hasattr(result, 'get_json'):
            response_data = result.get_json()
        elif isinstance(result, dict):
            response_data = result
        else:
            response_data = result
        
        if response_data and response_data.get('status') == 'success':
            data = response_data.get('data', [])
            return pd.DataFrame(data)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error fetching StreetEasy data: {e}")
        return pd.DataFrame()

def calculate_general_metrics(df):
    """Calculate general metrics from StreetEasy data"""
    total_listings = len(df)
    avg_rent = df['rent'].mean() if 'rent' in df.columns else 0
    avg_days_on_market = df['days_on_market'].mean() if 'days_on_market' in df.columns else 0
    
    return {
        'total_listings': f"{total_listings:,}",
        'avg_rent': f"${avg_rent:,.0f}",
        'avg_days_on_market': f"{avg_days_on_market:.0f}",
        'summary_text': f"Analysis of {total_listings:,} rental listings shows an average rent of ${avg_rent:,.0f} with properties spending an average of {avg_days_on_market:.0f} days on the market."
    }

def process_streeteasy_rent_history(df):
    """Process StreetEasy data to extract rent history trends"""
    if df.empty:
        print("RENT DEBUG: Input DataFrame is empty")
        return pd.DataFrame()
    
    print(f"RENT DEBUG: Processing {len(df)} rows")
    print(f"RENT DEBUG: Columns available: {list(df.columns)}")
    
    # Check if price_history column exists (not rent_history)
    if 'price_history' not in df.columns:
        print("RENT DEBUG: No 'price_history' column found")
        return pd.DataFrame()
    
    records = []
    processed_count = 0
    
    for idx, row in df.iterrows():
        try:
            bedrooms = row.get('bedrooms', 0)
            if pd.isna(bedrooms):
                continue
                
            bedrooms = int(bedrooms)
            history = row.get('price_history', '[]')  # Changed from rent_history to price_history
            
            if isinstance(history, str):
                try:
                    history = json.loads(history)
                except json.JSONDecodeError:
                    continue
                
            if not isinstance(history, list) or not history:
                continue
                
            processed_count += 1
            if processed_count <= 5:  # Debug first 5 rows
                print(f"RENT DEBUG: Row {idx} - bedrooms: {bedrooms}, history length: {len(history)}")
                
            # Sort by date
            history = sorted(history, key=lambda x: x.get('date'))
            
            for i, entry in enumerate(history):
                try:
                    price = float(entry.get('price', 0))
                    if price <= 0:
                        continue
                        
                    start_date = pd.to_datetime(entry.get('date')).date()
                    
                    # Determine end date
                    if i + 1 < len(history):
                        end_date = pd.to_datetime(history[i + 1].get('date')).date() - timedelta(days=1)
                    else:
                        end_date = datetime.now().date()
                    
                    records.append({
                        'unit_id': row.get('unit_id'),
                        'bedrooms': bedrooms,
                        'start_date': start_date,
                        'end_date': end_date,
                        'price': price
                    })
                except Exception as e:
                    if processed_count <= 5:
                        print(f"RENT DEBUG: Error processing entry {i} for row {idx}: {e}")
                    continue
        except Exception as e:
            if processed_count <= 5:
                print(f"RENT DEBUG: Error processing row {idx}: {e}")
            continue
    
    print(f"RENT DEBUG: Processed {processed_count} rows with valid history")
    print(f"RENT DEBUG: Created {len(records)} price records")
    
    if not records:
        print("RENT DEBUG: No records created, returning empty DataFrame")
        return pd.DataFrame()
    
    # Convert to DataFrame
    price_df = pd.DataFrame(records)
    print(f"RENT DEBUG: Price DataFrame shape: {price_df.shape}")
    print(f"RENT DEBUG: Unique bedrooms: {sorted(price_df['bedrooms'].unique())}")
    
    # Create date range (last 14 months)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=425)  # ~14 months
    date_range = pd.date_range(start_date, end_date, freq='D')
    print(f"RENT DEBUG: Date range: {start_date} to {end_date}, {len(date_range)} days")
    
    # Aggregate by date
    daily_totals = defaultdict(lambda: defaultdict(float))
    daily_counts = defaultdict(lambda: defaultdict(int))
    
    for _, row in price_df.iterrows():
        period_dates = pd.date_range(
            max(row['start_date'], start_date),
            min(row['end_date'], end_date),
            freq='D'
        )
        
        bedrooms = row['bedrooms']
        price = row['price']
        
        for date in period_dates:
            date_key = date.date()
            daily_totals[date_key][bedrooms] += price
            daily_counts[date_key][bedrooms] += 1
    
    # Build result DataFrame
    all_bedrooms = sorted(set(price_df['bedrooms']))
    print(f"RENT DEBUG: All bedrooms found: {all_bedrooms}")
    result_records = []
    
    for date in date_range:
        date_key = date.date()
        record = {'date': date}
        
        for bedrooms in all_bedrooms:
            total = daily_totals[date_key][bedrooms]
            count = daily_counts[date_key][bedrooms]
            avg_price = total / count if count > 0 else np.nan
            record[bedrooms] = avg_price
            
        result_records.append(record)
    
    result_df = pd.DataFrame(result_records)
    result_df['date'] = pd.to_datetime(result_df['date'])
    result_df = result_df.set_index('date').sort_index()
    
    # Forward fill NaN values
    result_df = result_df.ffill()
    
    print(f"RENT DEBUG: Final result DataFrame shape: {result_df.shape}")
    print(f"RENT DEBUG: Final columns: {list(result_df.columns)}")
    
    return result_df

def create_price_chart(rent_df, bedroom_filter=[0, 1, 2, 3], title="Weekly Rent Price Trends"):
    """Create a matplotlib chart for price trends by bedroom count"""
    if rent_df.empty:
        return "<div>No data available for chart</div>"
    
    # Resample to weekly data for cleaner visualization
    weekly_df = rent_df.resample('W').mean()
    
    # Filter data by bedroom count
    available_bedrooms = [col for col in weekly_df.columns if isinstance(col, (int, float)) and col in bedroom_filter]
    
    if not available_bedrooms:
        return "<div>No data available for selected bedroom counts</div>"
    
    # Create matplotlib figure
    plt.style.use('default')
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Color palette for different bedroom counts
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    
    traces_added = 0
    for i, bedrooms in enumerate(sorted(available_bedrooms)):
        bedroom_data = weekly_df[bedrooms].dropna()
        
        if bedroom_data.empty:
            continue
            
        bedroom_label = f"Studio" if bedrooms == 0 else f"{int(bedrooms)} BR"
        
        ax.plot(bedroom_data.index, bedroom_data.values, 
                color=colors[i % len(colors)], 
                linewidth=2, 
                marker='o', 
                markersize=4,
                label=bedroom_label)
        traces_added += 1
    
    if traces_added == 0:
        return "<div>No valid data found for chart generation</div>"
    
    # Customize the chart
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Average Rent ($)', fontsize=12)
    
    # Format y-axis to show currency
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    # Format x-axis dates - show month names and years
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    
    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Add legend
    ax.legend(loc='upper right', frameon=True, fancybox=True, shadow=True, fontsize=10)
    
    # Adjust layout
    plt.tight_layout()
    
    try:
        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        img_buffer.seek(0)
        
        # Convert to base64
        img_base64 = base64.b64encode(img_buffer.getvalue()).decode()
        
        # Close the plot to free memory
        plt.close(fig)
        
        # Create simple HTML with embedded image
        chart_html = f'''<img src="data:image/png;base64,{img_base64}" alt="{title}" style="width: 100%; height: auto; max-width: 100%; display: block;">'''
        
        return chart_html
        
    except Exception as e:
        plt.close(fig)
        return f"<div>Chart error: {str(e)}</div>"

def process_all_data(raw_df):
    """Process all data once and return processed datasets"""
    print(f"PROCESS DEBUG: raw_df shape: {raw_df.shape if not raw_df.empty else 'empty'}")
    processed_data = {}
    
    # Process rent history data once
    rent_df = process_streeteasy_rent_history(raw_df)
    print(f"PROCESS DEBUG: rent_df shape after processing: {rent_df.shape if not rent_df.empty else 'empty'}")
    processed_data['rent_data'] = rent_df
    
    # Calculate general metrics
    processed_data['general_metrics'] = calculate_general_metrics(raw_df)
    
    return processed_data

def get_template_data(template_name, processed_data, **kwargs):
    """Get data for specific template using pre-processed data"""
    template_configs = {
        'intro_page': {
            'data_keys': [],
            'extra_data': {}
        },
        'general_metrics': {
            'data_keys': ['general_metrics'],
            'extra_data': {
                'metrics_title': 'Current Market Metrics',
                'summary_title': 'Market Overview'
            }
        },
        'weekly_price_review': {
            'data_keys': ['rent_data'],
            'extra_data': {
                'chart_title': 'Weekly Rent Price Trends',
                'bedroom_filter': kwargs.get('bedroom_filter', [0, 1, 2])
            }
        }
    }
    
    config = template_configs.get(template_name, {'data_keys': [], 'extra_data': {}})
    result_data = {}
    
    # Get required data
    for data_key in config['data_keys']:
        if data_key == 'general_metrics':
            result_data.update(processed_data['general_metrics'])
        elif data_key == 'rent_data':
            # Create price chart from rent data
            rent_df = processed_data['rent_data']
            print(f"CHART DEBUG: rent_df shape in get_template_data: {rent_df.shape if not rent_df.empty else 'empty'}")
            bedroom_filter = config['extra_data'].get('bedroom_filter', [0, 1, 2])
            chart_html = create_price_chart(rent_df, bedroom_filter, config['extra_data'].get('chart_title', 'Price Trends'))
            result_data['price_chart'] = chart_html
    
    # Add extra template-specific data
    result_data.update(config['extra_data'])
    
    return result_data
