import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

def plot_ltv_cohorts(data):
    if not data:
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
    
    # Pivot the data for the heatmap
    pivot_df = df.pivot(
        index='first_month',
        columns='months_since_first',
        values='ltv'
    ).sort_index(ascending=True)
    
    # Limit to 24 months
    pivot_df = pivot_df.loc[:, pivot_df.columns <= 24]
    
    # Create text array with proper formatting
    text_array = np.where(
        np.isnan(pivot_df.values),
        "",
        "$" + np.round(pivot_df.values, 2).astype(str)
    )
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pd.to_datetime(pivot_df.index).strftime('%Y-%m'),
        colorscale='RdYlBu',
        text=text_array,
        texttemplate="%{text}",
        textfont={"size": 10},
        hoverongaps=False,
        hovertemplate='Cohort: %{y}<br>Month: %{x}<br>LTV: ${z:.2f}<extra></extra>',
        zmin=min_value,
        zmax=max_value
    ))
    
    fig.update_layout(
        title='Monthly Cohort LTV Analysis',
        xaxis_title='Months Since First Purchase',
        yaxis_title='Cohort',
        height=600,
        yaxis={
            'autorange': 'reversed',
            'categoryorder': 'category ascending'
        }
    )
    
    layout_config = {
        'font': {
            'family': 'JetBrains Mono',
            'color': '#a6ebc9'
        },
        'xaxis': {
            'gridcolor': '#393424',
            'linecolor': '#393424'
        },
        'yaxis': {
            'gridcolor': '#393424',
            'linecolor': '#393424'
        }
    }
    
    fig.update_layout(**layout_config)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show raw data in expandable section
    with st.expander("Show Raw Data"):
        st.subheader("LTV Values ($)")
        st.dataframe(pivot_df)
        
        # Create pivot table for cumulative revenue
        cum_revenue_df = df.pivot(
            index='first_month',
            columns='months_since_first',
            values='cum_amt'
        ).sort_index(ascending=True)
        
        st.subheader("Cumulative Revenue Over Time ($)")
        st.dataframe(cum_revenue_df)
        
        # Create pivot table for cohort sizes
        cohort_sizes_df = df.pivot(
            index='first_month',
            columns='months_since_first',
            values='cohort_num_users'
        ).sort_index(ascending=True)
        
        st.subheader("Cohort Sizes (Number of Users)")
        st.dataframe(cohort_sizes_df) 