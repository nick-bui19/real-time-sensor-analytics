#!/bin/bash

# Real-Time Traffic Analytics Dashboard Startup Script

echo "🚦 Starting Real-Time Traffic Analytics Dashboard..."
echo "=================================================="

# Set working directory
cd "$(dirname "$0")"

# Activate virtual environment
echo "📦 Activating virtual environment..."
source venv/bin/activate

# Check if Docker is running
echo "🐳 Checking Docker status..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    echo "   Opening Docker for you..."
    open -a Docker
    echo "   Waiting for Docker to start (30 seconds)..."
    sleep 30
fi

# Start Kafka infrastructure
echo "🔧 Starting Kafka infrastructure..."
cd kafka-setup
docker-compose up -d
sleep 10

# Check Kafka status
if docker ps | grep -q kafka; then
    echo "✅ Kafka is running"
else
    echo "⚠️  Kafka may not be fully ready yet"
fi

cd ..

# Check PostgreSQL connection
echo "🗄️  Checking PostgreSQL connection..."
if psql -U nickbui -d traffic_data -c "SELECT 1;" > /dev/null 2>&1; then
    echo "✅ PostgreSQL connection successful"
else
    echo "❌ PostgreSQL connection failed. Please check if PostgreSQL is running."
    exit 1
fi

# Start sensor simulator in background
echo "🎯 Starting sensor simulator..."
python sensor-simulator/traffic_simulator.py &
SIMULATOR_PID=$!
echo "   Simulator PID: $SIMULATOR_PID"

# Start Spark streaming in background
echo "⚡ Starting Spark streaming processor..."
bash run_spark.sh &
SPARK_PID=$!
echo "   Spark PID: $SPARK_PID"

# Wait a bit for data to start flowing
echo "⏳ Waiting for data pipeline to initialize (10 seconds)..."
sleep 10

# Launch dashboard
echo "📊 Launching Streamlit dashboard..."
echo "   Dashboard will be available at: http://localhost:8501"
echo "   Press Ctrl+C to stop all services"
echo ""

# Start dashboard (this blocks)
streamlit run dashboard/app.py --server.port 8501 --server.address localhost

# Cleanup on exit
echo ""
echo "🛑 Shutting down services..."
kill $SIMULATOR_PID 2>/dev/null
kill $SPARK_PID 2>/dev/null
cd kafka-setup
docker-compose down
echo "✅ All services stopped"