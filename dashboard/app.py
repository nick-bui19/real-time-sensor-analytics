import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime, timedelta
import subprocess
import json

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'traffic_data',
    'user': 'nickbui',
    'password': 'dummy',
    'port': '5432'
}

@st.cache_data(ttl=30)
def get_realtime_data():
    """Fetch latest 5-minute real-time traffic data"""
    try:
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        
        query = """
        SELECT start_time, end_time, location, 
               avg_traffic_count, max_traffic_count, 
               min_traffic_count, data_points
        FROM realtime_traffic_summary 
        WHERE start_time >= NOW() - INTERVAL '2 hours'
        ORDER BY start_time DESC, location
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_hourly_data():
    """Fetch hourly aggregated traffic data"""
    try:
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        
        query = """
        SELECT start_time, end_time, location, avg_traffic_count
        FROM hourly_traffic_summary 
        WHERE start_time >= NOW() - INTERVAL '24 hours'
        ORDER BY start_time DESC, location
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return pd.DataFrame()

def get_kafka_metrics():
    """Get Kafka topic metrics"""
    try:
        result = subprocess.run([
            'kafka-topics', '--bootstrap-server', 'localhost:9092', 
            '--describe', '--topic', 'traffic-topic'
        ], capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0:
            return {"status": "Connected", "details": result.stdout.strip()}
        else:
            return {"status": "Error", "details": result.stderr.strip()}
    except Exception as e:
        return {"status": "Unavailable", "details": str(e)}

def main():
    st.set_page_config(
        page_title="Real-Time Traffic Analytics Dashboard",
        page_icon="🚦",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Dashboard header
    st.title("🚦 Real-Time Traffic Analytics Dashboard")
    st.markdown("Live monitoring of traffic patterns across intersections")
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=True)
    
    if auto_refresh:
        time.sleep(1)
        st.rerun()
    
    # Fetch data
    realtime_df = get_realtime_data()
    hourly_df = get_hourly_data()
    
    if realtime_df.empty and hourly_df.empty:
        st.warning("No data available. Please ensure the data pipeline is running.")
        return
    
    # Main dashboard layout
    col1, col2, col3 = st.columns(3)
    
    # Real-time metrics cards
    if not realtime_df.empty:
        latest_data = realtime_df.groupby('location').first().reset_index()
        
        with col1:
            st.metric(
                "🚗 Average Traffic (5min)", 
                f"{latest_data['avg_traffic_count'].mean():.1f}",
                delta=f"{latest_data['avg_traffic_count'].std():.1f}"
            )
        
        with col2:
            st.metric(
                "📊 Active Intersections", 
                len(latest_data),
                delta="All Online" if len(latest_data) == 3 else "Some Offline"
            )
        
        with col3:
            st.metric(
                "🔄 Data Points (5min)", 
                int(latest_data['data_points'].sum()),
                delta=f"Last update: {latest_data['start_time'].max().strftime('%H:%M:%S')}"
            )
    
    st.divider()
    
    # Real-time traffic visualization
    st.subheader("🚦 Live Traffic Counts (5-minute windows)")
    
    if not realtime_df.empty:
        # Real-time line chart
        fig_realtime = px.line(
            realtime_df, 
            x='start_time', 
            y='avg_traffic_count',
            color='location',
            title="Real-Time Traffic Flow",
            labels={'avg_traffic_count': 'Cars per Minute', 'start_time': 'Time'},
            height=400
        )
        
        fig_realtime.update_layout(
            xaxis_title="Time",
            yaxis_title="Average Cars per Minute",
            legend_title="Intersection"
        )
        
        st.plotly_chart(fig_realtime, use_container_width=True)
        
        # Real-time data table
        with st.expander("📋 Real-Time Data Details"):
            st.dataframe(
                realtime_df[['start_time', 'location', 'avg_traffic_count', 'max_traffic_count', 'data_points']],
                use_container_width=True
            )
    
    # Historical trends
    st.subheader("📈 Historical Trends (Hourly Averages)")
    
    if not hourly_df.empty:
        # Hourly trends chart
        fig_hourly = px.line(
            hourly_df,
            x='start_time',
            y='avg_traffic_count',
            color='location',
            title="24-Hour Traffic Trends",
            labels={'avg_traffic_count': 'Average Cars per Minute', 'start_time': 'Hour'},
            height=400
        )
        
        fig_hourly.update_layout(
            xaxis_title="Time (Hourly)",
            yaxis_title="Average Cars per Minute",
            legend_title="Intersection"
        )
        
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    # Heat map section
    st.subheader("🗺️ Traffic Heat Map")
    
    if not realtime_df.empty:
        # Create heat map data
        heatmap_data = realtime_df.pivot_table(
            index='location',
            columns=realtime_df['start_time'].dt.strftime('%H:%M'),
            values='avg_traffic_count',
            fill_value=0
        )
        
        if not heatmap_data.empty:
            fig_heatmap = px.imshow(
                heatmap_data,
                title="Traffic Intensity Heat Map",
                labels={'x': 'Time (5-min intervals)', 'y': 'Intersection', 'color': 'Cars/min'},
                height=300
            )
            
            st.plotly_chart(fig_heatmap, use_container_width=True)
    
    # System health section
    st.subheader("⚡ System Health")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Kafka Status**")
        kafka_metrics = get_kafka_metrics()
        if kafka_metrics["status"] == "Connected":
            st.success("✅ Kafka Connected")
        else:
            st.error(f"❌ Kafka {kafka_metrics['status']}")
            
        with st.expander("Kafka Details"):
            st.text(kafka_metrics["details"])
    
    with col2:
        st.write("**Database Status**")
        if not realtime_df.empty or not hourly_df.empty:
            st.success("✅ Database Connected")
            st.info(f"Real-time records: {len(realtime_df)}")
            st.info(f"Hourly records: {len(hourly_df)}")
        else:
            st.error("❌ Database Connection Issues")
    
    # Footer
    st.divider()
    st.markdown("Dashboard auto-refreshes every 30 seconds | Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    main()