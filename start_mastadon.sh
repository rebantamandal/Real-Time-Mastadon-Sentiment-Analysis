#!/bin/bash
# Start Mastodon Streaming → Kafka → Spark → Visualization pipeline
pkill -f kafka
pkill -f zookeeper
pkill -f spark

# Clean old logs
rm -rf /tmp/zookeeper
rm -rf /tmp/kafka-logs

# === CONFIGURATION ===
KAFKA_DIR=$HOME/kafka   # path where you extracted Kafka
SPARK_DIR=$HOME/spark  # path to Spark
PROJECT_DIR=$HOME/mastadon_server  # your project folder
TOPIC="mastodon-raw"

echo "🚀 Starting Mastodon Streaming Pipeline..."

# === START ZOOKEEPER ===
echo "📡 Starting Zookeeper..."
gnome-terminal -- bash -c "$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties; exec bash"

sleep 5

# === START KAFKA BROKER ===
echo "📡 Starting Kafka Broker..."
gnome-terminal -- bash -c "$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties; exec bash"

sleep 5

# === CREATE TOPIC (if not exists) ===
echo "📡 Creating Kafka topic '$TOPIC'..."
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic $TOPIC --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# === START PRODUCER ===
echo "📰 Starting Mastodon Producer..."
gnome-terminal -- bash -c "cd $PROJECT_DIR/venv && source bin/activate && cd $PROJECT_DIR && python3 producer.py; exec bash"

# === START SPARK STREAMING CONSUMER ===
echo "🔥 Starting Spark Consumer..."
gnome-terminal -- bash -c "cd $PROJECT_DIR/venv && source bin/activate && cd $PROJECT_DIR && spark-submit --master local[*] consumer.py; exec bash"

# === START VISUALIZATION (Streamlit/Dash) ===
echo "📊 Starting Visualization..."
gnome-terminal -- bash -c "cd $PROJECT_DIR/venv && source bin/activate && cd $PROJECT_DIR && streamlit run dashboard.py; exec bash"

echo "✅ All services started!"

