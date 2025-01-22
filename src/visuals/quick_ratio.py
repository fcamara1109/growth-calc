import streamlit as st
import pandas as pd
import plotly.graph_objects as go

def plot_quick_ratio(df, time_unit="month"):
    if df.empty:
        st.info("No data available for visualization. Please upload some data first.")
        return
    
    time_column = time_unit
    
    # Show metrics in a styled container before the chart
    st.markdown("""
        <style>
        .metric-container {
            background-color: #1E1E1E;
            padding: 15px 20px;
            border-radius: 10px;
            margin: 0 10px;
            text-align: center;
            border: 1px solid #333;
        }
        .metric-label {
            font-size: 0.8rem;
            color: #888;
            margin-bottom: 5px;
        }
        .metric-value {
            font-size: 1.5rem;
            font-weight: bold;
            color: #fff;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Create metrics row with styled containers
    col1, col2, col3, col4, col5 = st.columns([1, 3, 3, 3, 1])
    
    with col2:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-label">Current</div>
                <div class="metric-value">{df['quick_ratio'].iloc[-1]:.2f}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-label">Average</div>
                <div class="metric-value">{df['quick_ratio'].mean():.2f}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
            <div class="metric-container">
                <div class="metric-label">Best</div>
                <div class="metric-value">{df['quick_ratio'].max():.2f}</div>
            </div>
        """, unsafe_allow_html=True)
    
    # Create figure with secondary y-axis
    fig = go.Figure()
    
    # Add quick ratio line
    fig.add_trace(
        go.Scatter(
            x=df[time_column],
            y=df['quick_ratio'],
            name='Quick Ratio',
            line=dict(color='#2E86C1', width=2),
            mode='lines+markers',
            hovertemplate='Quick Ratio: %{y:.2f}<extra></extra>'
        )
    )
    
    # Add benchmark lines
    benchmarks = {
        'Baseline (1.0)': {'value': 1.0, 'color': '#95A5A6', 'dash': 'dash'},
        'Consumer (2.0)': {'value': 2.0, 'color': '#27AE60', 'dash': 'dash'},
        'SaaS (4.0)': {'value': 4.0, 'color': '#E67E22', 'dash': 'dash'}
    }
    
    for name, info in benchmarks.items():
        fig.add_hline(
            y=info['value'],
            line=dict(color=info['color'], width=2, dash=info['dash']),
            annotation_text=name,
            annotation_position="right",
            annotation=dict(font_size=10)
        )
    
    # Update layout
    fig.update_layout(
        xaxis_title="",
        yaxis_title="",
        hovermode='x unified',
        showlegend=False,
        margin=dict(t=20),
        plot_bgcolor='#242424',
        paper_bgcolor='#242424',
        yaxis=dict(
            gridcolor='rgba(128,128,128,0.1)',
            zerolinecolor='rgba(128,128,128,0.1)',
            linecolor='#393424'
        ),
        xaxis=dict(
            gridcolor='rgba(128,128,128,0.1)',
            zerolinecolor='rgba(128,128,128,0.1)',
            linecolor='#393424'
        )
    )
    
    st.plotly_chart(fig, use_container_width=True) 