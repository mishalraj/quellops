# Database module

The database module collects health evidence without changing customer data. It has
no LLM and never connects directly to a writable primary.

## Access paths

| Path | Use | Safety boundary |
| --- | --- | --- |
| Cloud metrics | AWS, GCP, Azure, and OCI managed services | Calls monitoring APIs; never opens a database connection |
| Read replica | PostgreSQL, MySQL/MariaDB, SQL Server, Oracle, MongoDB, and Redis | Verifies replica role and read-only credentials before collecting bounded health data |
| CDC | Existing Kafka-compatible Debezium or provider change stream | Reads bounded event samples and never commits offsets |

Managed SQL, NoSQL, cache, search, and warehouse services appear in the UI catalog.
Provider metrics are normalized into the same signal format, so the central logic does
not need provider-specific code.

Cloud setup first uses the provider inventory API to discover matching databases. The
operator selects an allowed resource from that list; resource IDs do not need to be
copied manually. Discovery supports RDS, Aurora, DynamoDB, DocumentDB, ElastiCache,
Redshift, Cloud SQL, AlloyDB, Spanner, Firestore, Memorystore, Azure database services,
Cosmos DB, Azure Cache, and OCI database services.

## Flow

```mermaid
flowchart LR
    UI[Database setup UI] --> API[Database control API]
    API --> STORE[(Encrypted SQLite or PostgreSQL)]
    WATCH[Bounded watcher] --> MODE{Access path}
    MODE --> CLOUD[Cloud monitoring API]
    MODE --> REPLICA[Verified read replica]
    MODE --> CDC[Existing CDC topic]
    CLOUD --> NORMAL[Normalized database signals]
    REPLICA --> NORMAL
    CDC --> NORMAL
    NORMAL --> STORE
    NORMAL --> CORE[Central incident logic]
```

SQLite is the local default. Set `AIDBG_CONTROL_DATABASE_URL` to share state and job
leases across production replicas. Credentials are AES-GCM encrypted with
`AIDBG_MASTER_KEY`, and API operations require `AIDBG_ADMIN_API_TOKEN`.

The watcher limits each cycle by time, requests, returned rows, and connection count.
Failures use exponential backoff. Provider request counts and estimated cost are kept
with watcher status.

After ten samples of the same metric, the watcher learns a robust median and median
absolute deviation baseline. A large change is recorded as warning or critical evidence
with the baseline, score, relative change, and sample count. The dashboard plots recent
history against that baseline and marks detected anomalies.

The repeatable multi-replica soak test is `tests/database_multi_replica_soak.py`. It runs
several watcher processes against shared PostgreSQL, restarts one worker, and verifies
exclusive leases, persisted results, and zero collection failures.

Open `http://127.0.0.1:8088/ui/databases.html` after installation.
