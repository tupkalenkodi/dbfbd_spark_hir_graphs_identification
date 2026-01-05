#!/bin/bash

# Usage: ./run_identification.sh <n> <label>
# Example: ./run_identification.sh 11 small

# 1. Validation
if [ $# -lt 1 ]; then
    echo "Usage: $0 <n> [target_label]"
    echo "Example: $0 11 small"
    exit 1
fi

N=$1
# Default to "small" if no second argument is provided
LABEL=${2:-"small"}

WORKERS=${3:-2}

echo "Building JAR..."
sbt clean assembly
cp target/scala-2.12/graph-classifier.jar /srv/spark-jars/

# 2. Get Master ID
MASTER=$(docker ps --filter "name=spark-master" --format "{{.ID}}")

if [ -z "$MASTER" ]; then
    echo "ERROR: Spark master container not found!"
    exit 1
fi

echo "Running identification for N=$N using target graph: $LABEL..."

# 3. Execution
# Note: We pass $N and $LABEL to the JAR
docker exec $MASTER /opt/spark/bin/spark-submit \
  --master spark://192.168.0.107:7077 \
  --deploy-mode client \
  --class "main.IdentificationSparkProcess" \
  --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12,org.jgrapht:jgrapht-core:1.5.1 \
  --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
  --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
  --driver-memory 4G \
  --executor-memory 1500M \
  /jars/graph-classifier.jar $N $LABEL $WORKERS

echo "Identification for N=$N with label '$LABEL' completed!"
