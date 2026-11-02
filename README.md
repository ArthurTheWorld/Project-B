# CDC Lakehouse Project

**Tagline:** Real‑time change data capture from Postgres to analytics tables.

## Overview
This project implements a CDC pipeline that:
- Captures Postgres changes with **Debezium**.
- Publishes events to **Kafka**.
- Spark consumes Kafka and writes to **Delta Lake**.
- **dbt** transforms the raw Delta data into analytics tables.
- Metabase serves the final dashboards.

## Architecture
```mermaid
graph LR
    PG[PostgreSQL] -->|Debezium| KAF[Kafka]
    KAF -->|Spark| DL[Delta Lake]
    DL -->|dbt| AT[Analytics Tables]
    AT -->|Metabase| DASH[Metabase Dashboard]
```

## Prerequisites
- Docker Desktop
- At least 5 GB RAM
- Git

## Setup
```bash
# Clone the repository
git clone <repo‑url>
cd cdc-lakehouse

# Build and start containers
docker compose up -d --build

# Verify health
docker compose ps

# Check connector status (example for Debezium)
curl -s http://localhost:8083/connectors | jq .
```

## Generating Load
Run the demo script or use pgbench:
```powershell
# PowerShell demo script (Windows)
.\scripts\demo-evolucao.ps1

# Or pgbench (Linux/macOS)
pgbench -c 10 -j 4 -T 60 postgresql://localhost:5432/mydb
```
Aim for ~5 k events/s.

## Verifying the Pipeline
- Query Postgres and Delta tables.
- Inspect Spark logs for successful writes.
- After replaying events, ensure no duplicate rows.

## Testing Exactly‑Once
```bash
# Stop the Spark job
docker compose stop spark

# Delete checkpoint directory
rm -rf /path/to/checkpoint

# Restart Spark
docker compose start spark

# Verify row counts are unchanged
# (run your validation queries)
```

## Testing Schema Evolution
```sql
-- Add a new column in Postgres
ALTER TABLE orders ADD COLUMN promo_code VARCHAR(20);

-- Update some rows
UPDATE orders SET promo_code = 'SPRING23' WHERE id = 1;
```
After the change, confirm the column appears in Delta and in dbt models without restarting any services.

## Dashboard
Open Metabase at **http://localhost:3000**. Browse dashboards for:
- Latency
- Throughput
- Consumer lag

## Contributing
1. Fork the repo.  
2. Add tests for your changes.  
3. Follow the ADR style guide.

## License
MIT License.