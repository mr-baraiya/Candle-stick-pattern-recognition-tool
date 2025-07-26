# Function to check candle is hammer or not
def is_hammer(row):
    try:
        # Convert to float to avoid Series ambiguity
        open_price = float(row['Open'])
        close_price = float(row['Close'])
        high_price = float(row['High'])
        low_price = float(row['Low'])
        
        body = abs(open_price - close_price)
        if close_price > open_price:
            lower_wick = open_price - low_price
            upper_wick = high_price - close_price
        else:
            lower_wick = close_price - low_price
            upper_wick = high_price - open_price
        return lower_wick > 2 * body and body > upper_wick
    except Exception:
        return False
    
# Function to check candle is doji or not
def is_doji(row):
    try:
        # Convert to float to avoid Series ambiguity
        open_price = float(row['Open'])
        close_price = float(row['Close'])
        high_price = float(row['High'])
        low_price = float(row['Low'])
        
        return abs(close_price - open_price) / (high_price - low_price) < 0.1
    except ZeroDivisionError:
        return False
    except Exception:
        return False
    
#Function to check candle is rising window or not
def is_rising_window(curr_row, prev_row):
    try:
        if curr_row is None or prev_row is None:
            return False
        
        # Convert to float to avoid Series ambiguity
        prev_close = float(prev_row['Close'])
        prev_open = float(prev_row['Open'])
        curr_close = float(curr_row['Close'])
        curr_open = float(curr_row['Open'])
        
        return (
            prev_close > prev_open and  # Previous candle is bullish
            curr_close > curr_open and  # Current candle is bullish
            curr_open > prev_close  # Gap up
        )
    except Exception as e:
        print(f"Error in is_rising_window: {e}")
        return False

#Function to check candle is evening star or not
def is_evening_star(prev_row, mid_row, curr_row):
    try:
        if prev_row is None or mid_row is None or curr_row is None:
            return False
        
        # Convert to float to avoid Series ambiguity
        prev_close = float(prev_row['Close'])
        prev_open = float(prev_row['Open'])
        mid_close = float(mid_row['Close'])
        mid_open = float(mid_row['Open'])
        curr_close = float(curr_row['Close'])
        curr_open = float(curr_row['Open'])
        
        return (
            prev_close > prev_open and  # Previous candle is bullish
            abs(mid_close - mid_open) < (prev_close - prev_open) * 0.5 and  # Middle candle is small
            curr_open > mid_close and  # Gap up from middle candle
            curr_close < prev_open and  # Current close below previous open
            curr_close < curr_open  # Current candle is bearish
        )
    except Exception as e:
        print(f"Error in is_evening_star: {e}")
        return False

#Function to check candle is three white soldiers or not
def is_three_white_soldiers(row1, row2, row3):
    try:
        def is_bullish(row):
            return row['Close'] > row['Open']

        return (
            is_bullish(row1) and is_bullish(row2) and is_bullish(row3) and
            row2['Open'] > row1['Open'] and row2['Close'] > row1['Close'] and
            row3['Open'] > row2['Open'] and row3['Close'] > row2['Close']
        )
    except Exception as e:
        print(f"Error in is_three_white_soldiers: {e}")
        return False
