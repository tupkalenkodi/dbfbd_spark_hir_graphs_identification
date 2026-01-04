#!/bin/bash

# Usage: ./run_classification.sh <n>
# Example: ./run_classification.sh 12

# Check if n is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <n>"
    echo "Example: $0 12"
    exit 1
fi

N=$1

echo "Building JAR..."
sbt clean assembly
cp target/scala-2.12/graph-classifier.jar /srv/spark-jars/

# Set master container name
MASTER=$(docker ps --filter "name=spark-master" --format "{{.ID}}")

echo "Running classification for n=$N..."

# Optimized configuration for your cluster:
# Linux worker:   6 cores, 6G memory
# Windows worker: 2 cores, 8G memory
# Total:          8 cores, 14G memory


# Maximum resource utilization (aggressive)
docker exec $MASTER /opt/spark/bin/spark-submit \
  --master spark://192.168.0.107:7077 \
  --deploy-mode client \
  --class "main.ClassificationSparkProcess" \
  --driver-memory 2G \
  --executor-memory 6G \
  --total-executor-cores 8 \
  --executor-cores 2 \
  --num-executors 4 \
  --conf spark.default.parallelism=24 \
  --conf spark.sql.shuffle.partitions=24 \
  --conf spark.memory.fraction=0.9 \
  --conf spark.memory.storageFraction=0.2 \
  --conf spark.network.timeout=800s \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  /jars/graph-classifier.jar $N  # Pass n as argument

echo "Classification for n=$N completed!"
