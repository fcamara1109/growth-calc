import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

def plot_weekly_retention_cohorts(data):
    if not data:
        st.info("No data available for visualization. Please upload some data first.")
        return
    
    df = pd.DataFrame(data)
    
    if df.empty:
        st.info("No data available for the selected date range.")
        return
    
    # Convert retention rate to percentage
    df['retention_rate'] = df['retention_rate'] * 100
    
    # Add color scale range controls
    col1, col2 = st.columns(2)
    with col1:
        min_value = st.number_input(
            "Heatmap Min. Retention (%)", 
            value=0.0,
            min_value=0.0,
            max_value=100.0,
            step=5.0,
            key="weekly_retention_min"
        )
    with col2:
        max_value = st.number_input(
            "Heatmap Max. Retention (%)", 
            value=100.0,
            min_value=0.0,
            max_value=100.0,
            step=5.0,
            key="weekly_retention_max"
        )
    
    # Pivot the data for the heatmap
    pivot_df = df.pivot(
        index='first_week',
        columns='weeks_since_first',
        values='retention_rate'
    ).sort_index(ascending=True)
    
    # Limit to 52 weeks
    pivot_df = pivot_df.loc[:, pivot_df.columns <= 52]
    
    # Create text array with proper formatting
    text_array = np.where(
        np.isnan(pivot_df.values),
        "",
        np.round(pivot_df.values, 1).astype(str) + "%"
    )
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pd.to_datetime(pivot_df.index).strftime('%Y-%m-%d'),
        colorscale='RdYlBu',
        text=text_array,
        texttemplate="%{text}",
        textfont={"size": 10},
        hoverongaps=False,
        hovertemplate='Cohort: %{y}<br>Week: %{x}<br>Retention: %{z:.1f}%<extra></extra>',
        zmin=min_value,
        zmax=max_value
    ))
    
    fig.update_layout(
        title='Weekly Cohort Retention Rates',
        xaxis_title='Weeks Since First Purchase',
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
        st.subheader("Retention Rates (%)")
        st.dataframe(pivot_df)
        
        # Create pivot table for cohort sizes
        cohort_sizes_df = df.pivot(
            index='first_week',
            columns='weeks_since_first',
            values='cohort_num_users'
        ).sort_index(ascending=True)
        
        st.subheader("Cohort Sizes (Number of Users at Start)")
        st.dataframe(cohort_sizes_df)
        
        # Create pivot table for active users
        active_users_df = df.pivot(
            index='first_week',
            columns='weeks_since_first',
            values='users'
        ).sort_index(ascending=True)
        
        st.subheader("Active Users Over Time")
        st.dataframe(active_users_df) 