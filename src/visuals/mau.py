import streamlit as st
import pandas as pd
import plotly.express as px

def plot_mau(data):
    if data.empty:
        st.info("No data available for visualization. Please upload some data first.")
        return
    
    df = pd.DataFrame(data)
    
    # Create figure with all MAU components
    fig = px.line(
        df,
        x='month',
        y=['mau', 'new', 'retained', 'resurrected', 'churned'],
        markers=True
    )
    
    # Customize layout
    fig.update_layout(
        hovermode='x unified',
        legend_title="Type",
        xaxis_title="",
        yaxis_title="",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=12)
        ),
        margin=dict(t=60),
        plot_bgcolor='#242424',
        paper_bgcolor='#242424'
    )
    
    # Update line colors, names and hover template
    fig.for_each_trace(lambda t: t.update(
        name={
            'mau': 'Total MAU',
            'new': 'New',
            'retained': 'Retained',
            'resurrected': 'Resurrected',
            'churned': 'Churned'
        }[t.name],
        line_color={
            'mau': '#2E86C1',
            'new': '#27AE60',
            'retained': '#F1C40F',
            'resurrected': '#E67E22',
            'churned': '#E74C3C'
        }[t.name],
        line=dict(
            width=6 if t.name == 'mau' else 2
        ),
        hovertemplate='%{fullData.name}: %{y:,.0f}<extra></extra>'
    ))
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Add raw data section in an expander
    with st.expander("Show Raw Data"):
        # Format the date column
        df['month'] = pd.to_datetime(df['month']).dt.strftime('%Y-%m')
        
        # Format numeric columns
        numeric_cols = ['mau', 'new', 'retained', 'resurrected', 'churned']
        df[numeric_cols] = df[numeric_cols].round(0)
        
        # Drop session_id column
        df = df.drop(columns=['session_id'])
        
        # Display the dataframe
        st.dataframe(
            df,
            column_config={
                "month": "Month",
                "mau": st.column_config.NumberColumn(
                    "Total MAU",
                    format="%d"
                ),
                "new": st.column_config.NumberColumn(
                    "New",
                    format="%d"
                ),
                "retained": st.column_config.NumberColumn(
                    "Retained",
                    format="%d"
                ),
                "resurrected": st.column_config.NumberColumn(
                    "Resurrected",
                    format="%d"
                ),
                "churned": st.column_config.NumberColumn(
                    "Churned",
                    format="%d"
                )
            },
            hide_index=True
        ) 