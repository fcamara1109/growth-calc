import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

def plot_ltv_cohorts(data, period="month"):
    if data.empty:
        st.info("No data available for visualization. Please upload some data first.")
        return
    
    df = pd.DataFrame(data)
    
    if df.empty:
        st.info("No data available for the selected date range.")
        return
    
    # Add color scale range controls
    col1, col2 = st.columns(2)
    with col1:
        suggested_min = max(0, df['ltv'].min() * 0.9)  # 10% lower than min value, but not below 0
        min_value = st.number_input(
            "Heatmap Min. LTV ($)", 
            value=float(suggested_min),
            min_value=0.0,
            step=10.0,
            key="ltv_min"
        )
    with col2:
        suggested_max = df['ltv'].max() * 1.1  # 10% higher than max value
        max_value = st.number_input(
            "Heatmap Max. LTV ($)", 
            value=float(suggested_max),
            min_value=0.0,
            step=10.0,
            key="ltv_max"
        )
    
    # Use appropriate column names based on period
    if period == "month":
        first_period = 'first_month'
        periods_since = 'months_since_first'
        max_periods = 24
    elif period == "week":
        first_period = 'first_week'
        periods_since = 'weeks_since_first'
        max_periods = 52
    else:  # daily
        first_period = 'first_dt'
        periods_since = 'days_since_first'
        max_periods = 90
    
    # Pivot the data for the heatmap
    pivot_df = df.pivot(
        index=first_period,
        columns=periods_since,
        values='ltv'
    ).sort_index(ascending=True)
    
    # Limit to appropriate number of periods
    pivot_df = pivot_df.loc[:, pivot_df.columns <= max_periods]
    
    # Create text array with proper formatting
    text_array = []
    for row in pivot_df.values:
        text_row = []
        for val in row:
            if np.isnan(val):
                text_row.append("")
            else:
                text_row.append(f"${val:.2f}")
        text_array.append(text_row)
    text_array = np.array(text_array)
    
    # Adjust text size and format based on period
    text_size = 10 if period == "month" else 8
    date_format = '%Y-%m' if period == "month" else '%Y-%m-%d'
    
    # Update layout with explicit font settings
    period_title = {
        "month": "Months",
        "week": "Weeks",
        "day": "Days"
    }[period]
    
    # Format dates without time
    y_dates = pd.to_datetime(pivot_df.index).strftime('%Y-%m-%d')
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=y_dates,  # Use formatted dates
        colorscale='RdYlBu',
        text=text_array,
        texttemplate="%{text}",
        textfont={"size": text_size, "family": "JetBrains Mono"},
        hoverongaps=False,
        hovertemplate=f'Cohort: %{{y}}<br>{period_title}: %{{x}}<br>LTV: $%{{z:.2f}}<extra></extra>',
        zmin=min_value,
        zmax=max_value
    ))
    
    fig.update_layout(
        title=f'{period_title} Cohort LTV Analysis',
        xaxis_title=f'{period_title} Since First Purchase',
        yaxis_title='Cohort',
        height=600,
        yaxis={
            'autorange': 'reversed',
            'categoryorder': 'category ascending',
            'tickfont': {'size': text_size}  # Adjust y-axis label size
        },
        xaxis={
            'tickfont': {'size': text_size}  # Adjust x-axis label size
        },
        font={'family': 'JetBrains Mono'},
        uniformtext={'minsize': text_size, 'mode': 'show'}
    )
    
    # Force font for all text elements
    fig.update_xaxes(tickfont={'family': 'JetBrains Mono'})
    fig.update_yaxes(tickfont={'family': 'JetBrains Mono'})
    
    # Add styling config
    layout_config = {
        'font': {
            'family': 'JetBrains Mono',
            'color': '#a6ebc9'
        },
        'plot_bgcolor': '#242424',  # Inner plot background
        'paper_bgcolor': '#242424',  # Outer chart background
        'xaxis': {
            'gridcolor': '#393424',
            'linecolor': '#393424',
            'title_font': {'family': 'JetBrains Mono'},
            'tickfont': {'family': 'JetBrains Mono'}
        },
        'yaxis': {
            'gridcolor': '#393424',
            'linecolor': '#393424',
            'title_font': {'family': 'JetBrains Mono'},
            'tickfont': {'family': 'JetBrains Mono'}
        },
        'title_font': {'family': 'JetBrains Mono'}
    }
    
    fig.update_layout(**layout_config)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show raw data in expandable section
    with st.expander("Show Raw Data"):
        st.subheader("LTV Values ($)")
        st.dataframe(pivot_df)
        
        # Create pivot table for cumulative revenue
        cum_revenue_df = df.pivot(
            index=first_period,
            columns=periods_since,
            values='cum_amt'
        ).sort_index(ascending=True)
        
        st.subheader("Cumulative Revenue Over Time ($)")
        st.dataframe(cum_revenue_df)
        
        # Create pivot table for cohort sizes
        cohort_sizes_df = df.pivot(
            index=first_period,
            columns=periods_since,
            values='cohort_num_users'
        ).sort_index(ascending=True)
        
        st.subheader("Cohort Sizes (Number of Users)")
        st.dataframe(cohort_sizes_df) 