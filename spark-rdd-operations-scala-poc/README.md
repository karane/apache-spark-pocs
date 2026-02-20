# Spark RDD Operations POC (Scala)

Deep dive into Apache Spark RDD transformations, actions, and partitioning strategies using Scala.

## Tech Stack

- **Scala 2.12.18** / **Spark 3.5.0**
- **sbt 1.12.2** (runs in Docker — no local install needed)
- **Docker & Docker Compose**

## Prerequisites

- Docker & Docker Compose

## Usage

```bash
# 1. Start the Spark cluster
docker compose up -d

# 2. Build the JAR
./build.sh

# 3. Run a single job
./submit.sh Ex01_CreatingRddsJob

# 4. Run all jobs in sequence
./submit.sh all

# 5. Tear down
docker compose down
```

## Viewing Output

By default `./submit.sh` prints Spark logs (INFO/WARN) mixed with job output. To see only your job's output:

```bash
# Drop Spark logs (they go to stderr), keep only println output
./submit.sh Ex01_CreatingRddsJob 2>/dev/null
./submit.sh all 2>/dev/null
```

You can also check output via the **Spark Web UI** while the cluster is running:

- **Master UI** — http://localhost:8080 — registered workers, running/completed apps
- **Worker UI** — http://localhost:8081 — executor logs per task

To view a completed job's stdout: Master UI → Completed Applications → app ID → Executors → `stdout`.

## Sample Data

- `app/data/words.txt` — Text file for word count and text transformations
- `app/data/transactions.csv` — Sales data for aggregation and analytics
- `app/data/logs.txt` — Application logs for parsing and analysis
