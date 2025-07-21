import psycopg2
import random
from datetime import datetime, timedelta

# Database connection
conn = psycopg2.connect(
    host='localhost',
    database='traffic_data',
    user='nickbui',
    password='dummy'
)

cursor = conn.cursor()

# Generate test data for the last 2 hours
locations = ['intersection_A', 'intersection_B', 'intersection_C']
now = datetime.now()

print("Populating test real-time data...")

for i in range(24):  # 24 five-minute intervals = 2 hours
    start_time = now - timedelta(minutes=5 * (24 - i))
    end_time = start_time + timedelta(minutes=5)
    
    for location in locations:
        # Simulate realistic traffic patterns
        base_traffic = 50
        if 7 <= start_time.hour <= 9 or 17 <= start_time.hour <= 19:  # Rush hour
            base_traffic = 80
        elif 22 <= start_time.hour or start_time.hour <= 5:  # Night
            base_traffic = 20
            
        avg_count = base_traffic + random.randint(-20, 20)
        max_count = avg_count + random.randint(10, 30)
        min_count = max(avg_count - random.randint(10, 20), 0)
        data_points = random.randint(50, 100)
        
        cursor.execute("""
            INSERT INTO realtime_traffic_summary 
            (start_time, end_time, location, avg_traffic_count, 
             max_traffic_count, min_traffic_count, data_points)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (start_time, end_time, location, avg_count, max_count, min_count, data_points))

conn.commit()
cursor.close()
conn.close()

print("Test data populated successfully!")
print("You can now view the dashboard at http://localhost:8501")