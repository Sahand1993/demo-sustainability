#!/bin/bash

echo "Starting Kafka Architecture Pipeline..."

set -e

while getopts "k:c:s:m:d:h:n:f:j:" opt; do
  case $opt in
    k) KAFKA_HOME="$OPTARG" ;;
    c) KAFKA_CONFIG="$OPTARG" ;;
    d) DATA_DIR="$OPTARG" ;;
    h) HOME_DIR="$OPTARG" ;;
    n) EXPERIMENT_NAME="$OPTARG" ;;
    f) FLINK_HOME="$OPTARG" ;;   # NEW
    j) FLINK_JOB="$OPTARG" ;;    # NEW (jar path)
    *) echo "Usage: ..." ; exit 1 ;;
  esac
done

echo "KAFKA_HOME: $KAFKA_HOME"
echo "KAFKA_CONFIG: $KAFKA_CONFIG"
echo "METRICS_SCRIPT: $METRICS_SCRIPT"
echo "METRICS_DIR: $METRICS_DIR"
echo "DATA_DIR: $DATA_DIR"
echo "HOME_DIR: $HOME_DIR"
echo "EXPERIMENT_NAME: $EXPERIMENT_NAME"
echo "FLINK_HOME: $FLINK_HOME"
echo "FLINK_JOB: $FLINK_JOB"

# Configuration
EXPERIMENT_NAME_COMPLETE="$EXPERIMENT_NAME-$(date +'%Y%m%d_%H%M%S')"
GRACE_PERIOD=30

# 0. Set up Python environment
echo "========================================="
echo "0. Setting up Python environment..."

source $HOME_DIR/.venv/bin/activate
uv sync

# Clean up any previous Kafka logs
rm -rf /tmp/kraft-combined-logs

# 1. Start Kafka cluster
echo "========================================="
echo "1. Starting Kafka cluster..."
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
echo "Kafka Cluster ID: $KAFKA_CLUSTER_ID"
$KAFKA_HOME/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c $KAFKA_CONFIG
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_CONFIG

# Verify Kafka is running
echo "Checking Kafka cluster..."
curl -s http://localhost:9092 || echo "WARNING: Kafka not accessible"

echo "========================================="
echo "2. Starting Flink cluster..."

$FLINK_HOME/bin/start-cluster.sh

# Wait for JobManager REST API
echo "Waiting for Flink REST API..."
for i in {1..10}; do
  if curl -s http://localhost:8081 >/dev/null; then
    echo "Flink is up!"
    break
  fi
  sleep 2
done

sleep 10

echo "========================================="
echo "3. Submitting Flink job..."

FLINK_OUTPUT=$($FLINK_HOME/bin/flink run -d "$FLINK_JOB" \
  "$FLINK_TOPIC" \
  "localhost:9092" 2>&1)

echo "$FLINK_OUTPUT"

FLINK_JOB_ID=$(echo "$FLINK_OUTPUT" | grep -oE '[a-f0-9]{32}' | head -1)

echo "Flink Job ID: $FLINK_JOB_ID"

# 4. Run kafka producer to stream TPCH data
echo "========================================="
echo "2. Starting kafka producer..."
PRODUCER_START=$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")
sleep 5
echo "Producer start time: $PRODUCER_START after 15s sleep"

uv run tpch_streamer_async.py -d $DATA_DIR



sleep 10

PRODUCER_END=$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")
echo "Producer finished at: $PRODUCER_END after 15s sleep"
echo "Waiting ${GRACE_PERIOD}s"
sleep $GRACE_PERIOD


# 5. Collect metrics for each job
echo "========================================="
echo "5. Collecting metrics for each job..."

# Record end time (after grace period)
# Capture Kafka PID
KAFKA_BROKER_PID=$(pgrep -f "kafka.Kafka" | head -1)
echo "Kafka Broker PID: $KAFKA_BROKER_PID" # need to check if it is only one

echo "  Start: $PRODUCER_START"
echo "  End:   $PRODUCER_END"
echo "  Broker PID:   $KAFKA_BROKER_PID"

#python3 "$METRICS_SCRIPT" "$EXPERIMENT_NAME_COMPLETE" \
#    --start "$PRODUCER_START" \
#    --end "$PRODUCER_END" \
#    --output-dir "$METRICS_DIR"

# if [ -n "$KAFKA_BROKER_PID" ]; then
#     python3 "$METRICS_SCRIPT" "$EXPERIMENT_NAME_COMPLETE" \
#         --start "$PRODUCER_START" \
#         --end "$PRODUCER_END" \
#         --pid "$KAFKA_BROKER_PID" \
#         --output-dir "$METRICS_DIR"
# else
#     echo "  WARNING: No PID found, collecting without PID filter"
#     python3 "$METRICS_SCRIPT" "$EXPERIMENT_NAME_COMPLETE" \
#         --start "$PRODUCER_START" \
#         --end "$PRODUCER_END" \
#         --output-dir "$METRICS_DIR"
# fi


echo ""
echo "========================================="
echo "Pipeline deployed!"
echo "========================================="
echo "  - Grafana:    http://localhost:3000 (admin/admin)"
echo "  - Prometheus: http://localhost:9090"
echo "  - Kafka Broker:   http://localhost:9092"
echo "  - Scaphandre: http://localhost:8080/metrics"
echo ""
echo "Metrics: $METRICS_DIR"
echo "========================================="

sleep 100

echo "========================================="
echo "Stopping Flink job..."

if [ -n "$FLINK_JOB_ID" ]; then
  $FLINK_HOME/bin/flink cancel "$FLINK_JOB_ID"
fi

echo "Stopping Flink cluster..."
$FLINK_HOME/bin/stop-cluster.sh