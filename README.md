# Candlestick Pattern Recognition Tool

This project is a web-based tool for detecting and visualizing candlestick patterns in financial time series data. It is designed to help traders and analysts identify key reversal and continuation patterns in stock price charts, using uploaded or provided OHLCV (Open, High, Low, Close, Volume) data.

## Features

- **Automatic Pattern Detection:** Identifies popular candlestick patterns such as Hammer, Doji, Rising Window (Gap Up), Evening Star, and Three White Soldiers using algorithmic methods.
- **Single Symbol Analysis:** Analyze candlestick patterns for individual companies over selected timeframes.
- **Pattern Scanner:** Scan across multiple companies and timeframes to discover significant patterns.
- **Interactive Visualizations:** Uses Plotly and Bootstrap for interactive, responsive charting and results display.
- **Detailed Pattern Information:** Provides explanations, visuals, and statistics for each detected pattern, including occurrence time and signal type.
- **User-Friendly Interface:** Built with Bootstrap and Font Awesome for a modern, accessible user experience.

## How It Works

1. **Upload Data:** Provide your own CSV file(s) containing historical price data. The tool preprocesses this data to ensure compatibility (supports columns in both lowercase and uppercase).
2. **Pattern Analysis:** The backend applies a series of pattern recognition functions to each candlestick or group of candlesticks, marking occurrences in the data.
3. **Visualization:** Detected patterns are summarized in tables and visualized on interactive candlestick charts for easy review.
4. **Pattern Details:** Selecting a pattern occurrence provides an in-depth explanation and zoomed chart for focused analysis.

## Detected Patterns

- **Hammer:** Bullish reversal pattern at the bottom of a downtrend.
- **Doji:** Neutral/Indecision pattern indicating possible trend reversal.
- **Rising Window:** Bullish continuation signaled by a price gap up.
- **Evening Star:** Bearish reversal formed by a three-candle sequence.
- **Three White Soldiers:** Bullish reversal with three consecutive long bullish candles.

## Technologies Used

- Python (Pandas, Plotly)
- Flask (for the web application)
- HTML/CSS (Bootstrap, Font Awesome, Bootstrap Icons)
- Jinja2 templating

## Team Members

- **Haresh Zapadiya**  
  Worked on data-related tasks, backend development, and provided Python support.

- **Deven Machchhar**  
  Focused on research, development, and testing.

- **Dhruvrajsinh Zala**  
  Possesses strong knowledge of trading and contributed as a frontend designer.

- **Vishal Baraiya** (Team Leader)  
  Led the backend development, managed code merging and bug fixing, and effectively utilized AI tools.
  
## Getting Started

1. Clone this repository:
   ```bash
   git clone https://github.com/mr-baraiya/Candle-stick-pattern-recognition-tool.git
   cd Candle-stick-pattern-recognition-tool
   ```
2. Install dependencies (recommend using a virtual environment):
   ```bash
   pip install -r requirements.txt
   ```
3. Run the application:
   ```bash
   python app.py
   ```
4. Open your browser at `http://localhost:5000` and start analyzing candlestick patterns!

## Folder Structure

```
app/
 ├─ patterns.py          # Pattern detection logic
 ├─ preprocess_data.py   # Data cleaning and preprocessing
 ├─ pattern_scanner.py   # Scanning logic for multiple patterns
 ├─ visualize.py         # Visualization functions
 ├─ routes.py            # Flask routes and web logic
 └─ templates/           # HTML templates for UI
```

## License

MIT License

---

*This tool is intended for educational and informational purposes only. Always backtest and confirm patterns with additional analysis before making trading decisions.*
