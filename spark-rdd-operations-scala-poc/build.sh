#!/usr/bin/env bash
set -e

echo ">>> Building RDD Operations POC (Scala)..."

docker run --rm \
  -v "$(pwd)/app:/app" \
  -v "$HOME/.sbt:/root/.sbt" \
  -v "$HOME/.cache/coursier:/root/.cache/coursier" \
  -w /app \
  sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.2_2.12.21 \
  sbt -no-colors clean package

echo ">>> Build complete. JAR located at app/target/scala-2.12/"
