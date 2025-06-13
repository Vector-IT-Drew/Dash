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

def create_comp_data(df):
    
    comp_data = df.copy()

    comp_data = comp_data[comp_data['bedrooms'] != '']
   
    # Filter: bedrooms <= 4
    if 'bedrooms' in comp_data.columns:
        before_count = len(comp_data)
        comp_data = comp_data[comp_data['bedrooms'].astype(float) <= 4]
        print(f"COMP DEBUG: After bedrooms <= 4 filter: {len(comp_data)} rows (removed {before_count - len(comp_data)})")
    else:
        print("COMP DEBUG: Warning - 'bedrooms' column not found")
    
    # # Filter 1: is_no_fee = 1
    # if 'is_no_fee' in comp_data.columns:
    #     before_count = len(comp_data)
    #     comp_data = comp_data[comp_data['is_no_fee'].astype(int) == 1]

    #     print(f"COMP DEBUG: After no_fee filter: {len(comp_data)} rows (removed {before_count - len(comp_data)})")
    # else:
    #     print("COMP DEBUG: Warning - 'is_no_fee' column not found")
    
    # # Filter 2: Area must be GreenPoint or East Williamsburg
    # print('area names:', comp_data['areaName'].unique())
    # if 'areaName' in comp_data.columns:
    #     before_count = len(comp_data)
    #     comp_data = comp_data[comp_data['areaName'].isin(['Greenpoint', 'East Williamsburg'])]
    #     print(f"COMP DEBUG: After area filter: {len(comp_data)} rows (removed {before_count - len(comp_data)})")
    # else:
    #     print("COMP DEBUG: Warning - 'areaName' column not found")

    # print('area names:', comp_data['areaName'].unique())
    
    # # Filter 3: building_amenities includes required amenities
    # if 'amenities' in comp_data.columns:
    #     before_count = len(comp_data)
        
    #     # Convert building_amenities to string and check for required amenities
    #     def has_required_amenities(amenities_str):
    #         if pd.isna(amenities_str):
    #             return False
    #         amenities_str = str(amenities_str).lower()
            
    #         # Check for excluded amenities
    #         has_doorman = 'doorman' in amenities_str and 'virtual_doorman' not in amenities_str
    #         has_elevator = 'elevator' in amenities_str
            
    #         # Return True if does NOT have excluded amenities (virtual_doorman is optional)
    #         return (
    #             (not has_doorman and
    #             not has_elevator)
    #         )
    #     comp_data = comp_data[comp_data['amenities'].apply(has_required_amenities)]
    #     print(f"COMP DEBUG: After amenities filter: {len(comp_data)} rows (removed {before_count - len(comp_data)})")
    # else:
    #     print("COMP DEBUG: Warning - 'building_amenities' column not found")
    # print(f"COMP DEBUG: Final comp_data: {len(comp_data)} rows")

    # Print amenities for a specific unit if it appears
    if 'address' in comp_data.columns and 'unit' in comp_data.columns and 'amenities' in comp_data.columns:
        mask = (comp_data['address'] == '166-20 90th Avenue') & (comp_data['unit'] == '710')
        if mask.any():
            print(f"DEBUG: Amenities for 166-20 90th Avenue #710: {comp_data.loc[mask, 'amenities'].values}")

    return comp_data

def calculate_general_metrics(comp_data):
    """Calculate general metrics from comp data"""
    total_listings = len(comp_data)
    
    # Handle the new column naming from flat query structure
    rent_col = 'listed_price'  # Now we have the direct column name
    dom_col = 'days_on_market'  # Now we have the direct column name

    # Try to convert to numeric and catch errors
    avg_rent = 0
    avg_days_on_market = 0
    if rent_col in comp_data.columns:
        try:
            comp_data[rent_col] = pd.to_numeric(comp_data[rent_col], errors='raise')
            avg_rent = comp_data[rent_col].mean()
        except Exception as e:
            print(f"ERROR converting {rent_col} to numeric: {e}")
            for idx, val in enumerate(comp_data[rent_col]):
                try:
                    float(val)
                except Exception as e2:
                    print(f"BAD VALUE in {rent_col} at index {idx}: {val} -- {e2}")
    if dom_col in comp_data.columns:
        try:
            comp_data[dom_col] = pd.to_numeric(comp_data[dom_col], errors='raise')
            avg_days_on_market = comp_data[dom_col].mean()
        except Exception as e:
            print(f"ERROR converting {dom_col} to numeric: {e}")
            for idx, val in enumerate(comp_data[dom_col]):
                try:
                    float(val)
                except Exception as e2:
                    print(f"BAD VALUE in {dom_col} at index {idx}: {val} -- {e2}")

    return {
        'total_listings': f"{total_listings:,}",
        'avg_rent': f"${avg_rent:,.0f}",
        'avg_days_on_market': f"{avg_days_on_market:.0f}",
        'summary_text': f"Analysis of {total_listings:,} comp rental listings (no fee, laundry, doorman, super) shows an average rent of ${avg_rent:,.0f} with properties spending an average of {avg_days_on_market:.0f} days on the market."
    }

def process_streeteasy_rent_history(comp_data):
   
    # Check required columns for flat structure
    if not all(col in comp_data.columns for col in ['listed_price', 'created_at', 'bedrooms']):
        print("RENT DEBUG: Missing required columns: listed_price, created_at, or bedrooms")
        return pd.DataFrame()
    
    # Convert created_at to datetime if it's not already
    comp_data = comp_data.copy()
    comp_data['created_at'] = pd.to_datetime(comp_data['created_at'])
    
    # Filter out invalid data
    valid_data = comp_data[
        (comp_data['listed_price'].notna()) & 
        (comp_data['listed_price'] > 0) &
        (comp_data['bedrooms'].notna()) &
        (comp_data['created_at'].notna())
    ].copy()
    
    if valid_data.empty:
        print("RENT DEBUG: No valid data after filtering")
        return pd.DataFrame()
   
    # Create date range (last 14 months)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=425)  # ~14 months
    date_range = pd.date_range(start_date, end_date, freq='D')
    print(f"RENT DEBUG: Target date range: {start_date} to {end_date}, {len(date_range)} days")
    
    # Filter data to date range and convert to date for grouping
    valid_data['date'] = valid_data['created_at'].dt.date
    valid_data = valid_data[
        (valid_data['date'] >= start_date) & 
        (valid_data['date'] <= end_date)
    ]
    
    if valid_data.empty:
        print("RENT DEBUG: No data in target date range")
        return pd.DataFrame()
    
    # Group by date and bedroom, taking average price for each day/bedroom combination
    daily_averages = valid_data.groupby(['date', 'bedrooms'])['listed_price'].mean().reset_index()
    
    print(f"RENT DEBUG: Daily averages: {len(daily_averages)} records")
    
    # Pivot to get bedrooms as columns
    pivot_data = daily_averages.pivot(index='date', columns='bedrooms', values='listed_price')
    
    # Reindex to include all dates in range and forward fill missing values
    pivot_data = pivot_data.reindex(pd.date_range(start_date, end_date, freq='D').date)
    pivot_data = pivot_data.ffill()
    
    # Convert index back to datetime
    pivot_data.index = pd.to_datetime(pivot_data.index)
    
    print(f"RENT DEBUG: Final pivot data shape: {pivot_data.shape}")
    print(f"RENT DEBUG: Final columns: {list(pivot_data.columns)}")
    
    return pivot_data

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
    fig, ax = plt.subplots(figsize=(20, 6))  # Increased width to 20 for 100% page width
    
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

def get_comparison_tables(comp_data, custom_filters=None):
    """
    Generate comparison tables with dynamic filtering
    
    Args:
        comp_data: Base filtered dataset 
        custom_filters: List of filter definitions, each with 'title' and 'filter_func'
                       If None, creates default amenities-based filters
    """
    def generate_table_rows(df):
        if 'bedrooms' not in df.columns or df.empty:
            return []
        grouped = df.groupby('bedrooms')
        table_rows = []
        for bedrooms, group in grouped:
            try:
                # Handle both old and new column names
                price_col = 'current_listed_price' if 'current_listed_price' in group.columns else 'listed_price'
                avg_price = group[price_col].mean()
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

    # Create default amenities-based filters if none provided
    if custom_filters is None:
        # Debug: Print all unique amenities in alphabetical order
        amenities_col = 'amenities' if 'amenities' in comp_data.columns else 'building_amenities'
        if amenities_col in comp_data.columns:
            all_amenities = set()
            for amenities_str in comp_data[amenities_col].dropna():
                if isinstance(amenities_str, str):
                    amenities_list = [amenity.strip().lower() for amenity in amenities_str.split(',')]
                    all_amenities.update(amenities_list)
            sorted_amenities = sorted(all_amenities)
            print(f"DEBUG: All unique amenities (alphabetical): {', '.join(sorted_amenities)}")
        else:
            print(f"DEBUG: {amenities_col} column not found in comp_data")
        
        def has_outdoor_no_laundry_unit(amenities_str):
            """Has balcony or terrace, but NOT washer_dryer"""
            if pd.isna(amenities_str):
                return False
            amenities_str = str(amenities_str).lower()
            has_outdoor = 'balcony' in amenities_str or 'terrace' in amenities_str
            has_laundry_unit = 'washer_dryer' in amenities_str
            return has_outdoor and not has_laundry_unit
        
        def has_laundry_unit_no_outdoor(amenities_str):
            """Has washer_dryer, but NOT balcony or terrace"""
            if pd.isna(amenities_str):
                return False
            amenities_str = str(amenities_str).lower()
            has_outdoor = 'balcony' in amenities_str or 'terrace' in amenities_str
            has_laundry_unit = 'washer_dryer' in amenities_str
            return has_laundry_unit and not has_outdoor
        
        def has_both_outdoor_and_laundry_unit(amenities_str):
            """Has both (balcony or terrace) AND washer_dryer"""
            if pd.isna(amenities_str):
                return False 
            amenities_str = str(amenities_str).lower()
            has_outdoor = 'balcony' in amenities_str or 'terrace' in amenities_str
            has_laundry_unit = 'washer_dryer' in amenities_str
            return has_outdoor and has_laundry_unit
        
        # Determine which amenities column to use
        amenities_col = 'amenities' if 'amenities' in comp_data.columns else 'building_amenities'
        
        custom_filters = [
            {
                'title': 'Comp Data (No Fee + Building Amenities)', 
                'filter_func': lambda df: df  # No additional filtering - use comp_data as is
            },
            {
                'title': 'Outdoor Space w/o Laundry in Unit',
                'filter_func': lambda df: df[df[amenities_col].apply(has_outdoor_no_laundry_unit)] if amenities_col in df.columns else df.iloc[0:0]
            },
            {
                'title': 'Laundry in Unit w/o Outdoor Space', 
                'filter_func': lambda df: df[df[amenities_col].apply(has_laundry_unit_no_outdoor)] if amenities_col in df.columns else df.iloc[0:0]
            },
            {
                'title': 'Outdoor Space + Laundry in Unit',
                'filter_func': lambda df: df[df[amenities_col].apply(has_both_outdoor_and_laundry_unit)] if amenities_col in df.columns else df.iloc[0:0]
            }
        ]

    # Generate the first table (baseline - usually comp_data with no additional filtering)
    baseline_filter = custom_filters[0]
    baseline_data = baseline_filter['filter_func'](comp_data)
    market_rows = generate_table_rows(baseline_data)
    
    tables = [{
        'title': baseline_filter['title'],
        'columns': ['Market', 'Avg Price', 'Avg SqFt', 'Avg PSf', 'Count'],
        'rows': market_rows
    }]
    
    # Generate remaining tables with variance columns
    for filter_def in custom_filters[1:]:
        try:
            filtered_data = filter_def['filter_func'](comp_data)
            print(f"COMP DEBUG: {filter_def['title']} filtered to {len(filtered_data)} rows")
            
            rows = generate_table_rows(filtered_data)
            rows = add_variance_columns(rows, market_rows)
            
            tables.append({
                'title': filter_def['title'],
                'columns': ['Market', 'Avg Price', 'Avg SqFt', 'Avg PSf', 'Count', 'Price Variance', 'Avg SqFt Var', 'Avg PSf Var'],
                'rows': rows
            })
        except Exception as e:
            print(f"Error processing filter '{filter_def['title']}': {e}")
            # Add empty table on error
            tables.append({
                'title': f"{filter_def['title']} (Error)",
                'columns': ['Market', 'Avg Price', 'Avg SqFt', 'Avg PSf', 'Count', 'Price Variance', 'Avg SqFt Var', 'Avg PSf Var'],
                'rows': []
            })
    
    return tables

def get_weekly_trends(comp_data, title="Weekly Rent Price Trends", bedroom_filter=None):
    if bedroom_filter is None:
        bedroom_filter = [0, 1, 2, 3]
        
    rent_df = process_streeteasy_rent_history(comp_data)
    if 'date' not in rent_df.columns:
        rent_df = rent_df.reset_index()
    rent_df = rent_df.sort_values('date')
    rent_df = rent_df.set_index('date').resample('W').mean().reset_index()
    rent_df = rent_df.iloc[-12:]  # Last 12 weeks
    rent_df = rent_df.reset_index(drop=True)
    
    bed_labels = {0: 'Studio', 1: '1 Bed', 2: '2 Bed', 3: '3 Bed', 4: '4 Bed'}
    week_cols = [d.strftime('%b-%d') for d in rent_df['date']]

    # Create table data: WoW % change for each week (except first)
    table_rows = []
    available_bedrooms = [bed for bed in bedroom_filter if bed in rent_df.columns]
    for bed in available_bedrooms:
        row = {'Bed': bed_labels.get(bed, str(bed))}
        vals = [rent_df[bed].iloc[i] for i in range(len(rent_df))]
        # Calculate WoW % change for each week (first week is '-')
        for i in range(len(vals)):
            if i == 0 or pd.isnull(vals[i-1]) or pd.isnull(vals[i]) or vals[i-1] == 0:
                row[week_cols[i]] = '-'
            else:
                pct = (vals[i] - vals[i-1]) / vals[i-1] * 100
                row[week_cols[i]] = pct
        # Add average WoW % change for this bed type
        pct_changes = [row[w] for w in week_cols if isinstance(row[w], (int, float))]
        if pct_changes:
            avg_wow = sum(pct_changes) / len(pct_changes)
            row['Avg WoW'] = avg_wow
        else:
            row['Avg WoW'] = '-'
        table_rows.append(row)

    # Prepare color coding info for the template (positive/negative/neutral)
    color_map = {}
    for row in table_rows:
        for w in week_cols:
            val = row[w]
            if isinstance(val, (int, float)):
                if val > 2:
                    color_map[(row['Bed'], w)] = 'wow-pos-strong'
                elif val > 0:
                    color_map[(row['Bed'], w)] = 'wow-pos'
                elif val < -2:
                    color_map[(row['Bed'], w)] = 'wow-neg-strong'
                elif val < 0:
                    color_map[(row['Bed'], w)] = 'wow-neg'
                else:
                    color_map[(row['Bed'], w)] = 'wow-neutral'
            else:
                color_map[(row['Bed'], w)] = 'wow-na'

    # --- Generate clean line chart (unchanged) ---
    plt.style.use('default')
    fig, ax = plt.subplots(figsize=(20, 6))
    colors = ['#7FB3D3', '#5B9BD5', '#4472C4', '#2F528F']
    for i, bed in enumerate(available_bedrooms):
        if bed in rent_df.columns:
            x_data = rent_df['date']
            y_data = rent_df[bed]
            ax.plot(x_data, y_data, 
                   color=colors[i % len(colors)], 
                   linewidth=3, 
                   marker='o', 
                   markersize=6,
                   label=bed_labels.get(bed, str(bed)),
                   markerfacecolor=colors[i % len(colors)],
                   markeredgecolor='white',
                   markeredgewidth=1)
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
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax.tick_params(axis='y', labelsize=10, colors='#666666')
    ax.set_xticks(rent_df['date'])
    ax.set_xticklabels([d.strftime('%b-%d') for d in rent_df['date']], 
                       fontsize=10, 
                       color='#666666',
                       rotation=0)
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5, color='#E5E5E5')
    ax.set_axisbelow(True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#E5E5E5')
    ax.spines['bottom'].set_color('#E5E5E5')
    ax.set_facecolor('white')
    fig.patch.set_facecolor('white')
    legend = ax.legend(loc='center left', 
                      bbox_to_anchor=(1.01, 0.5),
                      frameon=False, 
                      fontsize=10,
                      labelcolor='#333333')
    plt.subplots_adjust(right=0.88)
    plt.tight_layout()
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
    chart_path = os.path.join(OUTPUT_DIR, 'weekly_trends_chart.png')
    fig.savefig(chart_path, format='png', bbox_inches='tight', facecolor='white', dpi=150)
    plt.close(fig)

    return {
        'table_rows': table_rows,
        'week_cols': week_cols,
        'table_columns': ['Bed'] + week_cols + ['Avg WoW'],
        'chart_path': 'weekly_trends_chart.png',
        'chart_title': title,
        'bedroom_filter': available_bedrooms,
        'color_map': color_map
    }

def get_ytd_ppsf_data(comp_data, custom_filters=None):
    """
    Generate YTD PPSF data for 4 charts using historical price data from comp_data
    
    Args:
        comp_data: Filtered StreetEasy data (no fee + required amenities)
        custom_filters: List of filter definitions, each with 'title' and 'filter_func'
                       If None, uses same default amenities-based filters as comparison tables
    """
    this_year = datetime.now().year
    last_year = this_year - 1
    months = [month_abbr[m] for m in range(1, datetime.now().month+1)]
    
    # Create default amenities-based filters if none provided (same as comparison tables)
    if custom_filters is None:
        def has_outdoor_no_laundry_unit(amenities_str):
            """Has balcony or terrace, but NOT washer_dryer"""
            if pd.isna(amenities_str):
                return False
            amenities_str = str(amenities_str).lower()
            has_outdoor = 'balcony' in amenities_str or 'terrace' in amenities_str
            has_laundry_unit = 'washer_dryer' in amenities_str
            return has_outdoor and not has_laundry_unit
        
        def has_laundry_unit_no_outdoor(amenities_str):
            """Has washer_dryer, but NOT balcony or terrace"""
            if pd.isna(amenities_str):
                return False
            amenities_str = str(amenities_str).lower()
            has_outdoor = 'balcony' in amenities_str or 'terrace' in amenities_str
            has_laundry_unit = 'washer_dryer' in amenities_str
            return has_laundry_unit and not has_outdoor
        
        def has_both_outdoor_and_laundry_unit(amenities_str):
            """Has both (balcony or terrace) AND washer_dryer"""
            if pd.isna(amenities_str):
                return False
            amenities_str = str(amenities_str).lower()
            has_outdoor = 'balcony' in amenities_str or 'terrace' in amenities_str
            has_laundry_unit = 'washer_dryer' in amenities_str
            return has_outdoor and has_laundry_unit
        
        # Determine which amenities column to use
        amenities_col = 'amenities' if 'amenities' in comp_data.columns else 'building_amenities'
        
        custom_filters = [
            {
                'title': 'Comp Data (No Fee + Building Amenities)', 
                'filter_func': lambda df: df  # No additional filtering - use comp_data as is
            },
            {
                'title': 'Outdoor Space w/o Laundry in Unit',
                'filter_func': lambda df: df[df[amenities_col].apply(has_outdoor_no_laundry_unit)] if amenities_col in df.columns else df.iloc[0:0]
            },
            {
                'title': 'Laundry in Unit w/o Outdoor Space', 
                'filter_func': lambda df: df[df[amenities_col].apply(has_laundry_unit_no_outdoor)] if amenities_col in df.columns else df.iloc[0:0]
            },
            {
                'title': 'Outdoor Space + Laundry in Unit',
                'filter_func': lambda df: df[df[amenities_col].apply(has_both_outdoor_and_laundry_unit)] if amenities_col in df.columns else df.iloc[0:0]
            }
        ]

    charts_data = []
    
    for i, filter_def in enumerate(custom_filters):
        chart_info = {
            'title': filter_def['title'],
            'chart_path': '',
            'table_rows': [],
            'months': months
        }
        
        # Apply the filter to get filtered dataset FIRST
        try:
            filtered_comp_data = filter_def['filter_func'](comp_data)
            print(f"YTD PPSF DEBUG: {filter_def['title']} filtered to {len(filtered_comp_data)} rows")
            
            if filtered_comp_data.empty:
                chart_info['chart_path'] = None
                charts_data.append(chart_info)
                continue
            
            # NOW process historical rent data for this specific filtered dataset
            historical_df = process_streeteasy_rent_history(filtered_comp_data)
            if historical_df.empty:
                chart_info['chart_path'] = None
                charts_data.append(chart_info)
                continue
            
            # Reset index to make date a column
            if 'date' not in historical_df.columns:
                historical_df = historical_df.reset_index()
            
            # Add year/month columns to historical data
            historical_df['year'] = historical_df['date'].dt.year
            historical_df['month'] = historical_df['date'].dt.month
            
            # Convert historical rent data to PPSF data for this specific filter
            ppsf_records = []
            for idx, row in historical_df.iterrows():
                date = row['date']
                year = row['year']
                month = row['month']
                
                for bed_col in historical_df.columns:
                    if isinstance(bed_col, (int, float)) and bed_col in [0, 1, 2, 3, 4]:  # bedroom columns
                        price = row[bed_col]
                        if pd.notnull(price) and price > 0:
                            # Use average sqft by bedroom type from current filtered listings
                            bed_data = filtered_comp_data[filtered_comp_data['bedrooms'] == bed_col]
                            if not bed_data.empty:
                                avg_sqft = bed_data['size_sqft'].mean()
                                if pd.notnull(avg_sqft) and avg_sqft > 0:
                                    ppsf = price / avg_sqft
                                    ppsf_records.append({
                                        'date': date,
                                        'year': year,
                                        'month': month,
                                        'bedrooms': bed_col,
                                        'price': price,
                                        'sqft': avg_sqft,
                                        'ppsf': ppsf
                                    })
            
            if not ppsf_records:
                chart_info['chart_path'] = None
                charts_data.append(chart_info)
                continue
            
            ppsf_df = pd.DataFrame(ppsf_records)
            
        except Exception as e:
            print(f"Error processing filter '{filter_def['title']}': {e}")
            chart_info['chart_path'] = None
            charts_data.append(chart_info)
            continue
        
        # Generate chart data - aggregate all bedrooms for each dataset
        chart_data = {}
        table_rows = []
        
        # Current year and prior year data
        current_year_data = []
        prior_year_data = []
        
        for month in range(1, datetime.now().month + 1):
            # Current year - properly average all PPSF values for the month
            current_data = ppsf_df[(ppsf_df['year'] == this_year) & (ppsf_df['month'] == month)]
            current_ppsf = current_data['ppsf'].mean()
            current_year_data.append(current_ppsf if pd.notnull(current_ppsf) else np.nan)
            
            # Prior year - properly average all PPSF values for the month  
            prior_data = ppsf_df[(ppsf_df['year'] == last_year) & (ppsf_df['month'] == month)]
            prior_ppsf = prior_data['ppsf'].mean()
            prior_year_data.append(prior_ppsf if pd.notnull(prior_ppsf) else np.nan)
        
        # Store chart data
        chart_data['all_current'] = current_year_data
        chart_data['all_prior'] = prior_year_data
        
        # Create table rows with proper formatting
        # Current year row
        current_row = {'category': 'Current Year', 'year': str(this_year)}
        for j, month in enumerate(months):
            val = current_year_data[j]
            current_row[month] = f"${val:.2f}" if pd.notnull(val) else '-'
        table_rows.append(current_row)
        
        # Prior year row
        prior_row = {'category': 'Prior Year', 'year': str(last_year)}
        for j, month in enumerate(months):
            val = prior_year_data[j]
            prior_row[month] = f"${val:.2f}" if pd.notnull(val) else '-'
        table_rows.append(prior_row)
        
        # Variance row with proper percentage calculation
        variance_row = {'category': 'Variance', 'year': 'Variance'}
        for j, month in enumerate(months):
            curr = current_year_data[j]
            prior = prior_year_data[j]
            if pd.notnull(curr) and pd.notnull(prior) and prior != 0:
                variance = ((curr - prior) / prior) * 100
                variance_row[month] = f"{variance:+.1f}%"
            else:
                variance_row[month] = '-'
        table_rows.append(variance_row)
        
        # Generate chart image with value labels
        chart_path = generate_ppsf_chart_with_labels(chart_data, months, f"{filter_def['title']}_{i+1}", this_year, last_year)
        chart_info['chart_path'] = chart_path
        chart_info['table_rows'] = table_rows
        
        charts_data.append(chart_info)
    
    return {
        'charts': charts_data,
        'months': months
    }

def generate_ppsf_chart_with_labels(chart_data, months, title, current_year, prior_year):
    """Generate a modern, clean line chart for PPSF data with value labels like the weekly chart"""
    if not chart_data:
        return None
        
    # Use clean, modern styling
    plt.style.use('default')
    # Wider aspect ratio to fit the quadrant layout better
    fig, ax = plt.subplots(figsize=(6, 2.5), facecolor='white')
    
    # Create month positions for x-axis
    x_positions = list(range(len(months)))
    
    # Get and clean data - remove NaN values completely
    current_data = chart_data.get('all_current', [])
    prior_data = chart_data.get('all_prior', [])
    
    # Filter out NaN values and create valid data points
    current_valid = [(i, val) for i, val in enumerate(current_data) if pd.notnull(val) and val > 0]
    prior_valid = [(i, val) for i, val in enumerate(prior_data) if pd.notnull(val) and val > 0]
    
    # Modern color palette - clean blues like the weekly chart
    current_color = '#2563eb'  # Modern blue
    prior_color = '#f59e0b'    # Modern amber
    
    # If no valid data, create a placeholder chart
    if not current_valid and not prior_valid:
        ax.text(0.5, 0.5, 'No Data Available', 
                horizontalalignment='center', verticalalignment='center',
                transform=ax.transAxes, fontsize=14, color='#6b7280',
                fontweight='500')
        ax.set_xlim(0, len(months)-1)
        ax.set_ylim(0, 10)
    else:
        # Plot current year line if we have data
        if current_valid:
            current_x = [point[0] for point in current_valid]
            current_y = [point[1] for point in current_valid]
            ax.plot(current_x, current_y, 
                   color=current_color, 
                   linewidth=3, 
                   marker='o', 
                   markersize=6,
                   label=f'{current_year}',
                   linestyle='-',
                   markerfacecolor=current_color,
                   markeredgecolor='white',
                   markeredgewidth=2,
                   alpha=0.9)
            
            # Add value labels above each point - like the weekly chart
            for x, y in zip(current_x, current_y):
                ax.annotate(f"${y:.1f}", 
                           (x, y), 
                           textcoords="offset points", 
                           xytext=(0, 8), 
                           ha='center', 
                           fontsize=8, 
                           color=current_color,
                           fontweight='bold')
        
        # Plot prior year line if we have data
        if prior_valid:
            prior_x = [point[0] for point in prior_valid]
            prior_y = [point[1] for point in prior_valid]
            ax.plot(prior_x, prior_y, 
                   color=prior_color, 
                   linewidth=3, 
                   marker='s', 
                   markersize=6,
                   label=f'{prior_year}',
                   linestyle='--',
                   markerfacecolor=prior_color,
                   markeredgecolor='white',
                   markeredgewidth=2,
                   alpha=0.8)
            
            # Add value labels above each point - like the weekly chart
            for x, y in zip(prior_x, prior_y):
                ax.annotate(f"${y:.1f}", 
                           (x, y), 
                           textcoords="offset points", 
                           xytext=(0, 8), 
                           ha='center', 
                           fontsize=8, 
                           color=prior_color,
                           fontweight='bold')
    
    # Modern styling - clean like the weekly chart
    ax.set_xlabel('')
    ax.set_ylabel('')
    
    # Format y-axis with decimals - more modern formatting
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:.1f}'))
    ax.tick_params(axis='y', labelsize=9, colors='#374151', labelcolor='#374151')
    
    # Format x-axis
    ax.set_xticks(x_positions)
    ax.set_xticklabels(months, fontsize=9, color='#374151')
    
    # Modern grid styling - subtle like the weekly chart
    ax.grid(True, alpha=0.2, linestyle='-', linewidth=1, color='#e5e7eb')
    ax.set_axisbelow(True)
    
    # Clean, minimal spines - like the weekly chart
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e5e7eb')
    ax.spines['bottom'].set_color('#e5e7eb')
    ax.spines['left'].set_linewidth(1)
    ax.spines['bottom'].set_linewidth(1)
    
    # Set clean background
    ax.set_facecolor('#fafafa')
    fig.patch.set_facecolor('white')
    
    # Add modern legend if we have data - positioned like the weekly chart
    if current_valid or prior_valid:
        legend = ax.legend(fontsize=9, 
                          frameon=True, 
                          loc='upper left',
                          fancybox=True,
                          shadow=False,
                          framealpha=0.9,
                          edgecolor='#e5e7eb',
                          facecolor='white')
        legend.get_frame().set_linewidth(1)
    
    # Set reasonable y-axis limits with padding
    all_values = [point[1] for point in current_valid + prior_valid]
    if all_values:
        min_val = min(all_values)
        max_val = max(all_values)
        padding = (max_val - min_val) * 0.15  # More generous padding for labels
        ax.set_ylim(max(0, min_val - padding), max_val + padding)
    
    # Tight layout for better fit - minimal padding for max stretch
    plt.tight_layout(pad=0.2)
    
    # Save chart with high quality
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
    # Properly sanitize filename - remove/replace problematic characters
    safe_title = title.replace(' ', '_').replace('.', '').replace(',', '').replace('&', 'and').replace('/', '_').replace('\\', '_').replace('(', '').replace(')', '').replace('+', 'plus')
    chart_path = os.path.join(OUTPUT_DIR, f'ppsf_chart_{safe_title}.png')
    fig.savefig(chart_path, format='png', bbox_inches='tight', 
               facecolor='white', dpi=150, edgecolor='none', pad_inches=0.05)
    plt.close(fig)
    
    # Return relative path for WeasyPrint
    return f'ppsf_chart_{safe_title}.png'

def preprocess_df(comp_data):
    comp_data = comp_data.copy()
    
    # Map new column names from grouped query to expected names for compatibility
    column_mapping = {
        'current_listed_price': 'listed_price',
        'current_days_on_market': 'days_on_market',
        'current_status': 'status'
    }
    
    # Rename columns if they exist
    for old_col, new_col in column_mapping.items():
        if old_col in comp_data.columns:
            comp_data[new_col] = comp_data[old_col]
    
    # Convert columns to numeric
    for col in ['listed_price', 'size_sqft', 'bedrooms', 'net_rent', 'current_listed_price']:
        if col in comp_data.columns:
            comp_data[col] = pd.to_numeric(comp_data[col], errors='coerce')
    
    # Only use rows with valid, positive price and sqft and bedrooms
    price_col = 'current_listed_price' if 'current_listed_price' in comp_data.columns else 'listed_price'
    if price_col in comp_data.columns and 'size_sqft' in comp_data.columns:
        comp_data = comp_data[(comp_data[price_col] > 0) & (comp_data['size_sqft'] > 0)]
    
    if 'bedrooms' in comp_data.columns:
        comp_data = comp_data[comp_data['bedrooms'].notnull()]
    
    # Calculate PPSF and NPSF
    if price_col in comp_data.columns and 'size_sqft' in comp_data.columns:
        comp_data['ppsf'] = comp_data[price_col] / comp_data['size_sqft']
        # Also create a 'listed_price' column if it doesn't exist for backward compatibility
        if 'listed_price' not in comp_data.columns:
            comp_data['listed_price'] = comp_data[price_col]
    
    if 'net_rent' in comp_data.columns and 'size_sqft' in comp_data.columns:
        comp_data['npsf'] = comp_data['net_rent'] / comp_data['size_sqft']
    else:
        comp_data['npsf'] = np.nan
    
    # Add year/month for YTD - check multiple possible date columns
    date_col = None
    if 'listed_at' in comp_data.columns:
        date_col = 'listed_at'
    elif 'date_listed' in comp_data.columns:
        date_col = 'date_listed'
    elif 'last_run_date' in comp_data.columns:
        date_col = 'last_run_date'
    
    if date_col:
        comp_data['year'] = pd.to_datetime(comp_data[date_col], errors='coerce').dt.year
        comp_data['month'] = pd.to_datetime(comp_data[date_col], errors='coerce').dt.month
    else:
        comp_data['year'] = datetime.now().year
        comp_data['month'] = datetime.now().month
    
    return comp_data

def process_all_data(df):
    # Create comp data first
    comp_data = create_comp_data(df)
    comp_data = preprocess_df(comp_data)
    
    return {
        'comparison_tables': get_comparison_tables(comp_data),
        'ytd_ppsf': get_ytd_ppsf_data(comp_data),
        'weekly_trends': get_weekly_trends(comp_data),
        'general_metrics': calculate_general_metrics(comp_data),
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
                'bedroom_filter': kwargs.get('bedroom_filter', [0, 1, 2, 3])
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

def get_inventory_data(limit_units=''):
    """Fetch client data for inventory report - units with future move-out dates
    
    Args:
        limit_units: Optional integer to limit number of units returned (default 30)
    """
    try:
        db_result = get_db_connection()
        
        if db_result["status"] != "connected":
            raise Exception("Database connection failed")
        
        connection = db_result["connection"]
        credentials = db_result.get("credentials", {}) or {}
        
        result = run_query_system(
            connection=connection,
            credentials=credentials,
            query_id='get_client_data',
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
            df = pd.DataFrame(data)
            
            if df.empty:
                return {'units': [], 'total_count': 0}
            
            # Filter for units with future move-out dates only
            if 'move_out' in df.columns:
                # Convert move_out to datetime and filter for future dates
                # Handle both formatted dates (MM/dd/yy) and other formats
                def parse_move_out_date(date_str):
                    if pd.isna(date_str) or date_str == '-' or date_str == '':
                        return pd.NaT
                    try:
                        # Try to parse MM/dd/yy format first
                        return pd.to_datetime(date_str, format='%m/%d/%y', errors='coerce')
                    except:
                        # Fallback to general parsing
                        return pd.to_datetime(date_str, errors='coerce')
                
                df['move_out_parsed'] = df['move_out'].apply(parse_move_out_date)
                current_date = pd.Timestamp.now()
                
                # Only show units where move_out is in the future (unit will become available)
                future_moveouts = df[
                    df['move_out_parsed'].notna() & 
                    (df['move_out_parsed'] > current_date)
                ].copy()
                
                if future_moveouts.empty:
                    return {'units': [], 'total_count': 0}
                
              
                # Format the data for display
                formatted_units = []
                for _, row in future_moveouts.iterrows():
                    # Format dates - keep original format if it's already formatted correctly
                    def format_date(date_val):
                        if pd.isna(date_val) or date_val == '-' or date_val == '':
                            return "-"
                        # If it's already in MM/dd/yy format, keep it
                        if isinstance(date_val, str) and '/' in date_val:
                            return date_val
                        try:
                            if isinstance(date_val, str):
                                date_obj = pd.to_datetime(date_val)
                            else:
                                date_obj = date_val
                            return date_obj.strftime('%m/%d/%y')
                        except:
                            return str(date_val) if date_val != '-' else "-"
                    
                    # Handle currency and other values that may already be formatted
                    def safe_format_value(val, default='-'):
                        if pd.isna(val) or val == '' or val == '-':
                            return default
                        return str(val)
                    
                    # Handle numeric values with proper conversion
                    def safe_numeric(val, default_val=0):
                        if pd.isna(val) or val == '' or val == '-':
                            return default_val
                        try:
                            return int(float(val))
                        except:
                            return default_val
                    
                    def safe_float(val, default_val=0.0):
                        if pd.isna(val) or val == '' or val == '-':
                            return default_val
                        try:
                            return float(val)
                        except:
                            return default_val
                    
                    # Calculate days until vacant
                    days_until_vacant = 0
                    if pd.notna(row.get('move_out_parsed')):
                        days_until_vacant = (row['move_out_parsed'] - current_date).days
                    elif pd.notna(row.get('days_until_vacant')):
                        try:
                            days_until_vacant = int(row['days_until_vacant'])
                        except:
                            days_until_vacant = 0
                    
                    unit_data = {
                        'address': safe_format_value(row.get('address')),
                        'unit': safe_format_value(row.get('unit')),
                        'beds': safe_numeric(row.get('beds')),
                        'baths': safe_float(row.get('baths')),
                        'sqft': safe_numeric(row.get('sqft')),
                        'unit_status': safe_format_value(row.get('unit_status')),
                        'deal_status': safe_format_value(row.get('deal_status')),
                        'gross': safe_format_value(row.get('gross')),
                        'actual_rent': safe_format_value(row.get('actual_rent')),
                        'concession': safe_format_value(row.get('concession')),
                        'term': safe_format_value(row.get('term')),
                        'move_in': format_date(row.get('move_in')),
                        'move_out': format_date(row.get('move_out')),
                        'tenant_names': safe_format_value(row.get('tenant_names'), ''),
                        'most_recent_note': safe_format_value(row.get('most_recent_note')),
                        'days_until_vacant': days_until_vacant
                    }
                    formatted_units.append(unit_data)
                
                # Sort by move_out date (soonest first)
                formatted_units.sort(key=lambda x: x['days_until_vacant'])
                
             
                return {
                    'units': formatted_units,
                    'total_count': len(formatted_units)
                }
            else:
                return {'units': [], 'total_count': 0}
                
        else:
            return {'units': [], 'total_count': 0}
            
    except Exception as e:
        print(f"Error fetching inventory data: {e}")
        return {'units': [], 'total_count': 0}
