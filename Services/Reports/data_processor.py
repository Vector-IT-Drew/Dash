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
from calendar import month_abbr
import os

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

def get_comparison_tables(df, filters=None, filter_titles=None):
    def generate_table_rows(df):
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
                    'Avg SqFt': f"{avg_sqft:,.0f}" if pd.notnull(avg_sqft) else '-',
                    'Avg PSf': f"${avg_ppsf:,.2f}" if pd.notnull(avg_ppsf) and np.isfinite(avg_ppsf) else '-',
                    'Count': int(count) if pd.notnull(count) else '-',
                }
            except Exception:
                row = {'Market': bedrooms, 'Avg Price': '-', 'Avg SqFt': '-', 'Avg PSf': '-', 'Count': '-'}
            table_rows.append(row)
        table_rows = sorted(table_rows, key=lambda x: (x['Market'] if isinstance(x['Market'], int) else 99))
        return table_rows
    def add_variance_columns(filtered_rows, market_rows):
        market_map = {row['Market']: row for row in market_rows}
        for row in filtered_rows:
            m = market_map.get(row['Market'])
            if not m or m['Avg Price'] in ('-', 0) or row['Avg Price'] in ('-', 0):
                row['Price Variance'] = '-'
                row['Avg SqFt Var'] = '-'
                row['Avg PSf Var'] = '-'
                continue
            def pct(var, base):
                try:
                    return f"{((var-base)/base)*100:+.2f}%"
                except Exception:
                    return '-'
            def to_num(s): return float(str(s).replace('$','').replace(',','')) if s not in ('-', None) else 0
            row['Price Variance'] = pct(to_num(row['Avg Price']), to_num(m['Avg Price']))
            row['Avg SqFt Var'] = pct(to_num(row['Avg SqFt']), to_num(m['Avg SqFt']))
            row['Avg PSf Var'] = pct(to_num(row['Avg PSf']), to_num(m['Avg PSf']))
        return filtered_rows
    # Market table (no filter, no variance)
    market_rows = generate_table_rows(df)
    tables = [{
        'title': 'Market Data',
        'columns': ['Market', 'Avg Price', 'Avg SqFt', 'Avg PSf', 'Count'],
        'rows': market_rows
    }]
    # Subset tables (with variance)
    if filters is None:
        filters = [{} for _ in range(3)]
    if filter_titles is None:
        filter_titles = [f'Filter {i+1}' for i in range(3)]
    for i, f in enumerate(filters):
        subset = df.copy()
        for k, v in f.items():
            subset = subset[subset[k] == v]
        rows = generate_table_rows(subset)
        rows = add_variance_columns(rows, market_rows)
        tables.append({
            'title': filter_titles[i],
            'columns': ['Market', 'Avg Price', 'Avg SqFt', 'Avg PSf', 'Count', 'Price Variance', 'Avg SqFt Var', 'Avg PSf Var'],
            'rows': rows
        })
    return tables

def get_weekly_trends(df, title="Weekly Rent Price Trends", bedroom_filter=None):
    if bedroom_filter is None:
        bedroom_filter = [0, 1, 2, 3]
        
    rent_df = process_streeteasy_rent_history(df)
    if 'date' not in rent_df.columns:
        rent_df = rent_df.reset_index()
    rent_df = rent_df.sort_values('date')
    rent_df = rent_df.set_index('date').resample('W').mean().reset_index()
    rent_df = rent_df.iloc[-12:]  # Last 12 weeks
    rent_df = rent_df.reset_index(drop=True)
    
    bed_labels = {0: 'Studio', 1: '1 Bed', 2: '2 Bed', 3: '3 Bed', 4: '4 Bed'}
    week_cols = [d.strftime('%b-%d') for d in rent_df['date']]
    
    # Create table data
    table_rows = []
    available_bedrooms = [bed for bed in bedroom_filter if bed in rent_df.columns]
    
    for bed in available_bedrooms:
        row = {'Bed': bed_labels.get(bed, str(bed))}
        vals = [rent_df[bed].iloc[i] for i in range(len(rent_df))]
        for i, v in enumerate(vals):
            row[week_cols[i]] = f"${v:,.0f}" if pd.notnull(v) else '-'
        
        # Calculate WoW change
        if len(vals) > 1 and pd.notnull(vals[-2]) and pd.notnull(vals[-1]) and vals[-2] != 0:
            wow = (vals[-1] - vals[-2]) / vals[-2] * 100
            row['WoW'] = f"{wow:+.2f}%"
        else:
            row['WoW'] = '-'
        table_rows.append(row)
    
    # --- Generate clean line chart ---
    plt.style.use('default')
    fig, ax = plt.subplots(figsize=(12, 4.5))  # Reduced height to fit better with table
    
    # Color palette - clean blues like in the image
    colors = ['#7FB3D3', '#5B9BD5', '#4472C4', '#2F528F']
    
    # Plot lines for each bedroom type
    for i, bed in enumerate(available_bedrooms):
        if bed in rent_df.columns:
            # Get data and remove NaN values
            x_data = rent_df['date']
            y_data = rent_df[bed]
            
            # Plot the line
            ax.plot(x_data, y_data, 
                   color=colors[i % len(colors)], 
                   linewidth=3, 
                   marker='o', 
                   markersize=6,
                   label=bed_labels.get(bed, str(bed)),
                   markerfacecolor=colors[i % len(colors)],
                   markeredgecolor='white',
                   markeredgewidth=1)
            
            # Add value labels above each point
            for x, y in zip(x_data, y_data):
                if pd.notnull(y):
                    ax.annotate(f"${y:,.0f}", 
                               (x, y), 
                               textcoords="offset points", 
                               xytext=(0, 6), 
                               ha='center', 
                               fontsize=8, 
                               color=colors[i % len(colors)],
                               fontweight='bold')
    
    # Customize the chart
    ax.set_xlabel('')  # Remove x-axis label for cleaner look
    ax.set_ylabel('')  # Remove y-axis label for cleaner look
    
    # Format y-axis to show currency
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax.tick_params(axis='y', labelsize=10, colors='#666666')
    
    # Format x-axis dates
    ax.set_xticks(rent_df['date'])
    ax.set_xticklabels([d.strftime('%b-%d') for d in rent_df['date']], 
                       fontsize=10, 
                       color='#666666',
                       rotation=0)
    
    # Add subtle grid
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5, color='#E5E5E5')
    ax.set_axisbelow(True)
    
    # Clean up spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#E5E5E5')
    ax.spines['bottom'].set_color('#E5E5E5')
    
    # Set background color
    ax.set_facecolor('white')
    fig.patch.set_facecolor('white')
    
    # Add legend - positioned to the right, more compact
    legend = ax.legend(loc='center left', 
                      bbox_to_anchor=(1.01, 0.5),
                      frameon=False, 
                      fontsize=10,
                      labelcolor='#333333')
    
    # Adjust layout to accommodate legend - more compact
    plt.subplots_adjust(right=0.88)
    plt.tight_layout()
    
    # Save chart
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
    chart_path = os.path.join(OUTPUT_DIR, 'weekly_trends_chart.png')
    fig.savefig(chart_path, format='png', bbox_inches='tight', facecolor='white', dpi=150)
    plt.close(fig)
    
    rel_chart_path = os.path.relpath(chart_path, OUTPUT_DIR)
    
    return {
        'table_rows': table_rows,
        'week_cols': week_cols,
        'table_columns': ['Bed'] + week_cols + ['WoW'],
        'chart_path': rel_chart_path,
        'chart_title': title,
        'bedroom_filter': available_bedrooms
    }

def get_ytd_ppsf_data(df, address_filters=None):
    """
    Generate YTD PPSF data for 4 charts
    If address_filters is None, show full market data for all 4 charts
    If address_filters provided, use those specific filters
    """
    this_year = datetime.now().year
    last_year = this_year - 1
    months = [month_abbr[m] for m in range(1, datetime.now().month+1)]
    
    # Define datasets - if no filters provided, show full market data for all 4 charts
    if not address_filters:
        # Default: Show full market data for all 4 charts
        datasets = [
            {'name': 'Full Market Data', 'filter': {}},
            {'name': 'Full Market Data', 'filter': {}},
            {'name': 'Full Market Data', 'filter': {}},
            {'name': 'Full Market Data', 'filter': {}}
        ]
    else:
        # Use provided address filters
        datasets = address_filters[:4]  # Limit to 4 charts
    
    charts_data = []
    
    for i, dataset in enumerate(datasets):
        chart_info = {
            'title': dataset['name'],
            'chart_path': '',
            'table_rows': [],
            'months': months
        }
        
        # Apply filter to dataframe
        filtered_df = df.copy()
        
        for filter_key, filter_value in dataset['filter'].items():
            if filter_key == 'address':
                filtered_df = filtered_df[filtered_df['address'].str.contains(filter_value, na=False, case=False)]
            else:
                # For any other filter, apply it directly
                filtered_df = filtered_df[filtered_df[filter_key] == filter_value]
        
        if filtered_df.empty:
            chart_info['chart_path'] = None
            charts_data.append(chart_info)
            continue
        
        # Generate chart data for this dataset - aggregate all bedrooms together
        chart_data = {}
        table_rows = []
        
        # Current year data (all bedrooms combined)
        current_year_data = []
        prior_year_data = []
        
        for month in range(1, datetime.now().month + 1):
            # Current year
            current_mask = (filtered_df['year'] == this_year) & (filtered_df['month'] == month)
            current_data = filtered_df.loc[current_mask, 'ppsf']
            current_ppsf = current_data.mean()
            current_year_data.append(current_ppsf if pd.notnull(current_ppsf) else np.nan)
            
            # Prior year
            prior_mask = (filtered_df['year'] == last_year) & (filtered_df['month'] == month)
            prior_data = filtered_df.loc[prior_mask, 'ppsf']
            prior_ppsf = prior_data.mean()
            prior_year_data.append(prior_ppsf if pd.notnull(prior_ppsf) else np.nan)
        
        # Store chart data
        chart_data['all_current'] = current_year_data
        chart_data['all_prior'] = prior_year_data
        
        # Create table rows
        # Current year row
        current_row = {'category': 'Current Year', 'year': str(this_year)}
        for j, month in enumerate(months):
            val = current_year_data[j]
            current_row[month] = f"${val:,.2f}" if pd.notnull(val) else '-'
        table_rows.append(current_row)
        
        # Prior year row
        prior_row = {'category': 'Prior Year', 'year': str(last_year)}
        for j, month in enumerate(months):
            val = prior_year_data[j]
            prior_row[month] = f"${val:,.2f}" if pd.notnull(val) else '-'
        table_rows.append(prior_row)
        
        # Variance row
        variance_row = {'category': 'Variance', 'year': '% Change'}
        for j, month in enumerate(months):
            curr = current_year_data[j]
            prior = prior_year_data[j]
            if pd.notnull(curr) and pd.notnull(prior) and prior != 0:
                variance = ((curr - prior) / prior) * 100
                variance_row[month] = f"{variance:+.1f}%"
            else:
                variance_row[month] = '-'
        table_rows.append(variance_row)
        
        # Generate chart image
        chart_path = generate_ppsf_chart_simple(chart_data, months, f"{dataset['name']}_{i+1}", this_year, last_year)
        chart_info['chart_path'] = chart_path
        chart_info['table_rows'] = table_rows
        
        charts_data.append(chart_info)
    
    return {
        'charts': charts_data,
        'months': months
    }

def generate_ppsf_chart_simple(chart_data, months, title, current_year, prior_year):
    """Generate a simple line chart for PPSF data comparing current vs prior year"""
    if not chart_data:
        return None
        
    plt.style.use('default')
    fig, ax = plt.subplots(figsize=(6, 3))  # Smaller size for grid layout
    
    # Create month positions for x-axis
    x_positions = range(len(months))
    
    # Plot current year line (solid blue)
    current_data = chart_data.get('all_current', [])
    current_clean = [val if pd.notnull(val) else None for val in current_data]
    ax.plot(x_positions, current_clean, 
           color='#4472C4', 
           linewidth=3, 
           marker='o', 
           markersize=4,
           label=f'{current_year}',
           linestyle='-')
    
    # Plot prior year line (dashed light blue)
    prior_data = chart_data.get('all_prior', [])
    prior_clean = [val if pd.notnull(val) else None for val in prior_data]
    ax.plot(x_positions, prior_clean, 
           color='#A5C6EA', 
           linewidth=3, 
           marker='s', 
           markersize=4,
           label=f'{prior_year}',
           linestyle='--')
    
    # Customize chart
    ax.set_xlabel('')
    ax.set_ylabel('')
    
    # Format y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax.tick_params(axis='y', labelsize=8, colors='#666666')
    
    # Format x-axis
    ax.set_xticks(x_positions)
    ax.set_xticklabels(months, fontsize=8, color='#666666')
    
    # Styling
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5, color='#E5E5E5')
    ax.set_axisbelow(True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#E5E5E5')
    ax.spines['bottom'].set_color('#E5E5E5')
    ax.set_facecolor('white')
    fig.patch.set_facecolor('white')
    
    # Compact legend
    ax.legend(fontsize=8, frameon=False, loc='upper left')
    
    plt.tight_layout()
    
    # Save chart
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
    safe_title = title.replace(' ', '_').replace('.', '').replace(',', '')
    chart_path = os.path.join(OUTPUT_DIR, f'ppsf_chart_{safe_title}.png')
    fig.savefig(chart_path, format='png', bbox_inches='tight', facecolor='white', dpi=120)
    plt.close(fig)
    
    return os.path.relpath(chart_path, OUTPUT_DIR)

def preprocess_df(df):
    df = df.copy()
    # Convert columns to numeric
    for col in ['listed_price', 'size_sqft', 'bedrooms', 'net_rent']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    # Only use rows with valid, positive price and sqft and bedrooms
    if 'listed_price' in df.columns and 'size_sqft' in df.columns:
        df = df[(df['listed_price'] > 0) & (df['size_sqft'] > 0)]
    if 'bedrooms' in df.columns:
        df = df[df['bedrooms'].notnull()]
    # Calculate PPSF and NPSF
    if 'listed_price' in df.columns and 'size_sqft' in df.columns:
        df['ppsf'] = df['listed_price'] / df['size_sqft']
    if 'net_rent' in df.columns and 'size_sqft' in df.columns:
        df['npsf'] = df['net_rent'] / df['size_sqft']
    else:
        df['npsf'] = np.nan
    # Add year/month for YTD
    if 'date_listed' in df.columns:
        df['year'] = pd.to_datetime(df['date_listed'], errors='coerce').dt.year
        df['month'] = pd.to_datetime(df['date_listed'], errors='coerce').dt.month
    else:
        df['year'] = datetime.now().year
        df['month'] = datetime.now().month
    return df

def process_all_data(df):
    df = preprocess_df(df)
    return {
        'comparison_tables': get_comparison_tables(df),
        'ytd_ppsf': get_ytd_ppsf_data(df),
        'weekly_trends': get_weekly_trends(df),
        'general_metrics': calculate_general_metrics(df),
    }

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
