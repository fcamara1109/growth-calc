import streamlit as st
import pandas as pd
import plotly.express as px

def plot_wrr(data):
    if data.empty:
        st.info("No data available for visualization. Please upload some data first.")
        return
    
    df = pd.DataFrame(data)
    
    # Create figure with all WRR components
    fig = px.line(
        df,
        x='week',
        y=['rev', 'retained', 'new', 'expansion', 'resurrected', 'contraction', 'churned'],
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
    
    # Update line colors and names
    fig.for_each_trace(lambda t: t.update(
        name={
            'rev': 'Total WRR',
            'retained': 'Retained',
            'new': 'New',
            'expansion': 'Expansion',
            'resurrected': 'Resurrected',
            'contraction': 'Contraction',
            'churned': 'Churned'
        }[t.name],
        line_color={
            'rev': '#2E86C1',      # Blue
            'retained': '#27AE60',  # Green
            'new': '#F1C40F',      # Yellow
            'expansion': '#E67E22', # Orange
            'resurrected': '#8E44AD', # Purple
            'contraction': '#E74C3C', # Red
            'churned': '#C0392B'    # Dark Red
        }[t.name],
        line=dict(
            width=6 if t.name == 'rev' else 2
        ),
        hovertemplate='%{fullData.name}: $%{y:,.2f}<extra></extra>'
    ))
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Add raw data section in an expander
    with st.expander("Show Raw Data"):
        # Format the date column
        df['week'] = pd.to_datetime(df['week']).dt.strftime('%Y-%m-%d')
        
        # Format numeric columns
        numeric_cols = ['rev', 'retained', 'new', 'expansion', 'resurrected', 'contraction', 'churned']
        df[numeric_cols] = df[numeric_cols].round(2)
        
        # Drop session_id column
        df = df.drop(columns=['session_id'])
        
        # Display the dataframe with currency formatting
        st.dataframe(
            df,
            column_config={
                "week": "Week",
                "rev": st.column_config.NumberColumn(
                    "Total WRR",
                    format="$%.2f"
                ),
                "retained": st.column_config.NumberColumn(
                    "Retained",
                    format="$%.2f"
                ),
                "new": st.column_config.NumberColumn(
                    "New",
                    format="$%.2f"
                ),
                "expansion": st.column_config.NumberColumn(
                    "Expansion",
                    format="$%.2f"
                ),
                "resurrected": st.column_config.NumberColumn(
                    "Resurrected",
                    format="$%.2f"
                ),
                "contraction": st.column_config.NumberColumn(
                    "Contraction",
                    format="$%.2f"
                ),
                "churned": st.column_config.NumberColumn(
                    "Churned",
                    format="$%.2f"
                )
            },
            hide_index=True
        ) 