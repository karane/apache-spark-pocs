# Simple Java Spark Job POC

Introduction to Apache Spark with Java — creating RDDs, applying transformations, and running actions on a local Spark cluster.

## How to Run

```bash
# 1. Start the Spark cluster
docker compose up -d

# 2. Build the JAR
./build.sh

# 3. Submit the job
./submit.sh

# 4. View Spark UI
# Master: http://localhost:8080
# Worker: http://localhost:8081

# 5. Tear down
docker compose down
```

## Viewing Output

By default `./submit.sh` prints Spark logs (INFO/WARN) mixed with job output. To see only your job's output:

```bash
# Drop Spark logs (they go to stderr), keep only println output
./submit.sh 2>/dev/null
```

You can also check output via the **Spark Web UI** while the cluster is running:

- **Master UI** — http://localhost:8080 — registered workers, running/completed apps
- **Worker UI** — http://localhost:8081 — executor logs per task

To view a completed job's stdout: Master UI → Completed Applications → app ID → Executors → `stdout`.
