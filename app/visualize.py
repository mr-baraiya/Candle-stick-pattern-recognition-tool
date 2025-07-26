import plotly.graph_objects as go

def visualize_patterns(df):
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df['Date'],
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close'],
        name="Candlestick"
    ))

    # Hammer
    hammer_df = df[df['Hammer']]
    fig.add_trace(go.Scatter(
        x=hammer_df['Date'],
        y=hammer_df['Low'], 
        mode='markers',
        name='Hammer',
        marker=dict(color='blue', size=10, symbol='triangle-up')
    ))

    # Doji
    doji_df = df[df['Doji']]
    fig.add_trace(go.Scatter(
        x=doji_df['Date'],
        y=doji_df['Close'],  
        mode='markers',
        name='Doji',
        marker=dict(color='orange', size=10, symbol='circle')
    ))

    # Rising Window
    rising_window_df = df[df['RisingWindow']]
    fig.add_trace(go.Scatter(
        x=rising_window_df['Date'],
        y=rising_window_df['Low'],
        mode='markers',
        name='Rising Window',
        marker=dict(color='lightgreen', size=10, symbol='arrow-up')
    ))

    # Evening Star
    evening_star_df = df[df['EveningStar']]
    fig.add_trace(go.Scatter(
        x=evening_star_df['Date'],
        y=evening_star_df['High'],
        mode='markers',
        name='Evening Star',
        marker=dict(color='red', size=10, symbol='x')
    ))

    # Three White Soldiers
    soldiers_df = df[df['ThreeWhiteSoldiers']]
    fig.add_trace(go.Scatter(
        x=soldiers_df['Date'],
        y=soldiers_df['Close'],
        mode='markers',
        name='Three White Soldiers',
        marker=dict(color='white', size=10, symbol='star')
    ))

    fig.update_layout(
        title="Interactive Candlestick Chart with Patterns",
        xaxis_title="Date",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=600,
    )

    return fig
