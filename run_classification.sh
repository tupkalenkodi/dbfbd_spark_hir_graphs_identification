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

WORKERS=${2:-0}

echo "Building JAR..."
sbt clean assembly
cp target/scala-2.12/graph-classifier.jar /srv/spark-jars/

# Set master container name
MASTER=$(docker ps --filter "name=spark-master" --format "{{.ID}}")

if [ -z "$MASTER" ]; then
    echo "ERROR: Spark master container not found!"
    echo "Make sure your Docker Swarm cluster is running:"
    echo "  docker stack ps spark"
    exit 1
fi

echo "Running classification for n=$N..."

# Optimized configuration for your cluster:
# Linux worker:   6 cores, 6G memory
# Windows worker: 2 cores, 8G memory
# Total:          8 cores, 14G memory

# MINIMAL CONFIGURATION FOR OPTIMAL CORE USAGE
# MINIMAL CONFIGURATION FOR OPTIMAL CORE USAGE
docker exec $MASTER /opt/spark/bin/spark-submit \
  --master spark://192.168.0.107:7077 \
  --deploy-mode client \
  --class "main.ClassificationSparkProcess" \
  --driver-memory 2G \
  --executor-memory 1500M \
  --executor-cores 2 \
  --conf spark.default.parallelism=16 \
  /jars/graph-classifier.jar $N $WORKERS

echo "Classification for n=$N completed!"
