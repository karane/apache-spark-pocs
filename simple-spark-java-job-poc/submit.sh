#!/usr/bin/env bash
set -e

echo ">>> Submitting Simple Java Spark job to Spark cluster..."

docker run --rm \
  --network simple-spark-java-job-poc_default \
  -v "$(pwd)/app/target:/app" \
  apache/spark:3.5.0 \
  /opt/spark/bin/spark-submit \
    --class org.example.SimpleJob \
    --master spark://spark-master:7077 \
    /app/spark-simple-java-job-1.0-SNAPSHOT.jar
