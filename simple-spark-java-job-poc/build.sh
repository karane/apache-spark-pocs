#!/usr/bin/env bash
set -e

echo ">>> Building Simple Java Spark job..."

docker run --rm \
  -v "$(pwd)/app:/app" \
  -v "$HOME/.m2:/root/.m2" \
  -w /app \
  maven:3.9-eclipse-temurin-11 \
  mvn -f /app/pom.xml clean package

echo ">>> Build complete. JAR located at app/target/"
