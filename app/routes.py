from datetime import datetime, timedelta
import pandas as pd
from flask import Flask,request,render_template
from app.fetch_data import fetch_data, get_available_csv_files, get_available_timeframes
from app.preprocess_data import preprocess_data
from app.patterns import (
    is_hammer,
    is_doji,
    is_rising_window,
    is_evening_star,
    is_three_white_soldiers
)
from app.visualize import visualize_patterns
from app.pattern_scanner import scan_all_patterns

app=Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    # Get available CSV files and timeframes for the dropdowns
    csv_files = get_available_csv_files()
    timeframes = get_available_timeframes()
    
    if request.method == "POST":
        csv_file = request.form.get("csv_file")
        start_date = request.form.get("start_date")
        end_date = request.form.get("end_date")
        timeframe = request.form.get("timeframe", "1min")
        action = request.form.get("action")

        today = datetime.today().date()
        if datetime.strptime(end_date, "%Y-%m-%d").date() > today:
            return render_template("index.html", error="End date cannot be in the future.", 
                                 csv_files=csv_files, timeframes=timeframes)
        if datetime.strptime(start_date, "%Y-%m-%d").date() > today:
            return render_template("index.html", error="Start date cannot be in the future.", 
                                 csv_files=csv_files, timeframes=timeframes)

        df = fetch_data(csv_file, start_date, end_date, timeframe)
        
        if df.empty:
            return render_template("index.html", error="No data available for the selected file or date range.", 
                                 csv_files=csv_files, timeframes=timeframes)
            
        df = preprocess_data(df)

        if df.empty:
            return render_template("index.html", error="No valid data available after preprocessing.", 
                                 csv_files=csv_files, timeframes=timeframes)

        # If action is "show_table", just show the data table
        if action == "show_table":
            # Prepare table data (limit to first 100 rows for display)
            table_data = df.head(100).to_dict('records')
            return render_template("index.html", 
                                 csv_files=csv_files, 
                                 timeframes=timeframes,
                                 table_data=table_data,
                                 selected_csv=csv_file,
                                 selected_timeframe=timeframe,
                                 selected_start_date=start_date,
                                 selected_end_date=end_date,
                                 total_rows=len(df))

        # If action is "analyze", perform pattern analysis and show chart
        if action == "analyze":
            # Single candle patterns
            df['Hammer'] = df.apply(is_hammer, axis=1)
            df['Doji'] = df.apply(is_doji, axis=1)

            # Two-candle patterns
            df['RisingWindow'] = df.apply(
                lambda row: is_rising_window(row, df.iloc[df.index.get_loc(row.name) - 1]) if row.name > 0 else False,
                axis=1
            )

            # Three-candle patterns
            df['EveningStar'] = df.apply(
                lambda row: is_evening_star(
                    df.iloc[df.index.get_loc(row.name) - 2],
                    df.iloc[df.index.get_loc(row.name) - 1],
                    row
                ) if row.name > 1 else False,
                axis=1
            )

            df['ThreeWhiteSoldiers'] = df.apply(
                lambda row: is_three_white_soldiers(
                    df.iloc[df.index.get_loc(row.name) - 2],
                    df.iloc[df.index.get_loc(row.name) - 1],
                    row
                ) if row.name > 1 else False,
                axis=1
            )

            chart = visualize_patterns(df)
            return render_template("index.html", 
                                 chart=chart.to_html(), 
                                 csv_files=csv_files, 
                                 timeframes=timeframes,
                                 selected_csv=csv_file,
                                 selected_timeframe=timeframe,
                                 selected_start_date=start_date,
                                 selected_end_date=end_date)

    return render_template("index.html", csv_files=csv_files, timeframes=timeframes)

@app.route("/pattern-scanner", methods=["GET", "POST"])
def pattern_scanner():
    """
    Route for pattern scanner that shows all patterns across all files and timeframes
    """
    if request.method == "POST":
        start_date = request.form.get("start_date")
        end_date = request.form.get("end_date")
        
        # Get filter parameters
        script_filter = request.form.get("script_filter", "")
        pattern_filter = request.form.get("pattern_filter", "")
        timeframe_filter = request.form.get("timeframe_filter", "")
        
        today = datetime.today().date()
        if datetime.strptime(end_date, "%Y-%m-%d").date() > today:
            return render_template("pattern_scanner.html", error="End date cannot be in the future.")
        if datetime.strptime(start_date, "%Y-%m-%d").date() > today:
            return render_template("pattern_scanner.html", error="Start date cannot be in the future.")
        
        # Scan all patterns
        try:
            all_patterns = scan_all_patterns(start_date, end_date)
            
            # Apply filters
            filtered_patterns = all_patterns
            if script_filter:
                filtered_patterns = [p for p in filtered_patterns if script_filter.lower() in p['Script'].lower()]
            if pattern_filter:
                filtered_patterns = [p for p in filtered_patterns if pattern_filter.lower() in p['Pattern'].lower()]
            if timeframe_filter:
                filtered_patterns = [p for p in filtered_patterns if timeframe_filter.lower() in p['Timeframe'].lower()]
            
            # Get unique values for filter dropdowns
            unique_scripts = sorted(list(set([p['Script'] for p in all_patterns])))
            unique_patterns = sorted(list(set([p['Pattern'] for p in all_patterns])))
            unique_timeframes = sorted(list(set([p['Timeframe'] for p in all_patterns])))
            
            return render_template("pattern_scanner.html", 
                                 patterns=filtered_patterns,
                                 unique_scripts=unique_scripts,
                                 unique_patterns=unique_patterns,
                                 unique_timeframes=unique_timeframes,
                                 selected_start_date=start_date,
                                 selected_end_date=end_date,
                                 script_filter=script_filter,
                                 pattern_filter=pattern_filter,
                                 timeframe_filter=timeframe_filter,
                                 total_patterns=len(filtered_patterns))
        except Exception as e:
            return render_template("pattern_scanner.html", error=f"Error scanning patterns: {str(e)}")
    
    return render_template("pattern_scanner.html")

@app.route("/show-pattern")
def show_pattern():
    """
    Route to show a specific pattern on a chart
    """
    # Get parameters from URL
    csv_file = request.args.get("csv_file")
    timeframe = request.args.get("timeframe")
    pattern_time = request.args.get("pattern_time")
    pattern_type = request.args.get("pattern_type")
    
    if not all([csv_file, timeframe, pattern_time, pattern_type]):
        return render_template("pattern_detail.html", error="Missing required parameters")
    
    try:
        # Convert pattern time to date for data fetching
        pattern_datetime = datetime.strptime(pattern_time, "%Y-%m-%d %H:%M:%S")
        
        # Fetch data around the pattern occurrence (±7 days for broader context)
        start_date = (pattern_datetime - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (pattern_datetime + timedelta(days=7)).strftime("%Y-%m-%d")
        
        # Convert timeframe display back to code
        timeframe_map = {
            '1 Minute': '1min',
            '5 Minutes': '5min', 
            '15 Minutes': '15min',
            '30 Minutes': '30min',
            '1 Hour': '1hour'
        }
        timeframe_code = timeframe_map.get(timeframe, '1min')
        
        # Fetch and process data
        df = fetch_data(csv_file, start_date, end_date, timeframe_code)
        if df.empty:
            return render_template("pattern_detail.html", error="No data available for the specified period")
        
        df = preprocess_data(df)
        if df.empty:
            return render_template("pattern_detail.html", error="No valid data after preprocessing")
        
        # Find the pattern occurrence time in the dataframe
        # The preprocess_data function creates a 'Date' column, not 'Datetime'
        df['Date'] = pd.to_datetime(df['Date'])
        pattern_idx = df[df['Date'] <= pattern_datetime].index
        
        if len(pattern_idx) > 0:
            # Get the index closest to pattern time
            closest_idx = pattern_idx[-1]
            
            # Calculate start and end indices for 20 candles window
            start_idx = max(0, closest_idx - 10)  # 10 candles before
            end_idx = min(len(df), closest_idx + 11)  # 10 candles after + the pattern candle
            
            # Limit to last 20 candles around the pattern
            df = df.iloc[start_idx:end_idx].copy()
        else:
            # If pattern time not found, take last 20 candles
            df = df.tail(20).copy()
        
        # Apply only the specific pattern for focused view
        df = apply_specific_pattern(df, pattern_type)
        
        # Create chart focused on the specific pattern
        chart = visualize_patterns(df)
        
        # Get company name
        company_name = csv_file.replace('.csv', '').replace('-', ' ').title()
        
        return render_template("pattern_detail.html",
                             chart=chart.to_html(),
                             company_name=company_name,
                             pattern_type=pattern_type,
                             pattern_time=pattern_time,
                             timeframe=timeframe,
                             csv_file=csv_file)
        
    except Exception as e:
        return render_template("pattern_detail.html", error=f"Error displaying pattern: {str(e)}")

def apply_specific_pattern(df, pattern_type):
    """
    Apply only the specific pattern detection to the dataframe
    """
    # Initialize all pattern columns to False
    df['Hammer'] = False
    df['Doji'] = False
    df['RisingWindow'] = False
    df['EveningStar'] = False
    df['ThreeWhiteSoldiers'] = False
    
    # Apply only the selected pattern
    if pattern_type == 'Hammer':
        df['Hammer'] = df.apply(is_hammer, axis=1)
    elif pattern_type == 'Doji':
        df['Doji'] = df.apply(is_doji, axis=1)
    elif pattern_type == 'Rising Window':
        df['RisingWindow'] = df.apply(
            lambda row: is_rising_window(row, df.iloc[df.index.get_loc(row.name) - 1]) if row.name > 0 else False,
            axis=1
        )
    elif pattern_type == 'Evening Star':
        df['EveningStar'] = df.apply(
            lambda row: is_evening_star(
                df.iloc[df.index.get_loc(row.name) - 2],
                df.iloc[df.index.get_loc(row.name) - 1],
                row
            ) if row.name > 1 else False,
            axis=1
        )
    elif pattern_type == 'Three White Soldiers':
        df['ThreeWhiteSoldiers'] = df.apply(
            lambda row: is_three_white_soldiers(
                df.iloc[df.index.get_loc(row.name) - 2],
                df.iloc[df.index.get_loc(row.name) - 1],
                row
            ) if row.name > 1 else False,
            axis=1
        )
    
    return df

def apply_all_patterns(df):
    """
    Apply all pattern detection functions to the dataframe
    """
    # Single candle patterns
    df['Hammer'] = df.apply(is_hammer, axis=1)
    df['Doji'] = df.apply(is_doji, axis=1)

    # Two-candle patterns
    df['RisingWindow'] = df.apply(
        lambda row: is_rising_window(row, df.iloc[df.index.get_loc(row.name) - 1]) if row.name > 0 else False,
        axis=1
    )

    # Three-candle patterns
    df['EveningStar'] = df.apply(
        lambda row: is_evening_star(
            df.iloc[df.index.get_loc(row.name) - 2],
            df.iloc[df.index.get_loc(row.name) - 1],
            row
        ) if row.name > 1 else False,
        axis=1
    )

    df['ThreeWhiteSoldiers'] = df.apply(
        lambda row: is_three_white_soldiers(
            df.iloc[df.index.get_loc(row.name) - 2],
            df.iloc[df.index.get_loc(row.name) - 1],
            row
        ) if row.name > 1 else False,
        axis=1
    )
    
    return df
