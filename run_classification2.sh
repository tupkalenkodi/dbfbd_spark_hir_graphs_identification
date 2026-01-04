#!/bin/bash

# Usage: ./run_classification.sh <n>
# Example: ./run_classish $N

# Set master container name
MASTER=$(docker ps --filter "name=spark-master" --format "{{.ID}}")

if [ -z "$MASTER" ]; then
    echo "ERROR: Spark master container not found!"
    echo "Make sure your Docker Swarm cluster is running:"
    echo "  docker stack ps spark"
    exit 1
fi

echo "Running classification for n=$N..."
echo "Cluster Configuration:"
echo "  - Linux worker: 6 cores, 6G memory"
echo "  - Windows worker: 2 cores, 8G memory"
echo ""

# FORCE UNEVEN ALLOCATION - Use Linux's 6 cores, Windows's 2 cores
docker exec $MASTER /opt/spark/bin/spark-submit \
  --master spark://192.168.0.107:7077 \
  --deploy-mode client \
  --class "main.ClassificationSparkProcess" \
  \
  `# Memory Configuration` \
  --driver-memory 2G \
  `# Different memory for different workers` \
  --executor-memory 5G `# For Linux workers` \
  \
  `# Allocate executors per worker capacity` \
  `# Strategy: Request MORE executors than needed, let Spark schedule` \
  --total-executor-cores 8 `# We want 8 total cores` \
  --executor-cores 1 `# 1 core per executor for fine-grained control` \
  \
  `# Parallelism` \
  --conf spark.default.parallelism=16 \
  --conf spark.sql.shuffle.partitions=16 \
  \
  `# Memory Management` \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  \
  `# Scheduling - allow uneven distribution` \
  --conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s \
  --conf spark.scheduler.minRegisteredResourcesRatio=1.0 `# Wait for all resources` \
  --conf spark.locality.wait=0s `# Don't wait for data locality` \
  \
  `# Disable fair scheduling` \
  --conf spark.scheduler.mode=FIFO \
  \
  `# Network & Other Settings` \
  --conf spark.network.timeout=300s \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  \
  /jars/graph-classifier.jar $N

echo "Classification for n=$N completed!"
