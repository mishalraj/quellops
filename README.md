# AIDBG Infrastructure Debugger

An open-source system that connects to infrastructure tools, collects evidence, finds
likely causes of incidents, and helps operators decide what to fix.

## Run the logs module

```bash
git clone <repository-url>
cd infra-project-poc-mishalraj
./install.sh
```

The installer creates local secrets, builds the containers, starts the services, and
opens `http://127.0.0.1:8088/ui/logs.html`.

SQLite is the zero-setup default. For shared PostgreSQL storage across backend replicas:

```bash
./install.sh --postgres
```

Both backends store the same encrypted connections, watcher checkpoints, schedules,
and detection summaries. PostgreSQL adds shared transactions and replica-safe job
claims for horizontally scaled deployments. Every replica must use the same
`AIDBG_MASTER_KEY`.

## Architecture

```mermaid
flowchart LR
    UI[Web UI] --> CORE[Central logic]

    CORE --> LOGS[Logs module]
    CORE --> DB[Database module]
    CORE --> K8S[Kubernetes module]
    CORE --> TRACE[Tracing module]
    CORE --> CHANGE[Change module]

    LOGS --> PLATFORMS[Loki / Splunk / Datadog / Elastic / Cloud logs]
    K8S --> AGENT[Go infrastructure agent]
    DB --> DATABASES[Databases]

    CORE --> RESULT[Cause, evidence, and suggested fix]
```

The central logic is the brain. Infrastructure modules are independent tools that
collect and normalize evidence. An optional LLM belongs only in the central logic;
modules continue to work without it.

## Modules

| Module | Responsibility | Current implementation |
| --- | --- | --- |
| Central logic | Combines evidence, ranks causes, and suggests actions | Python / FastAPI |
| Logs | Fetches bounded logs, normalizes records, redacts configured values, and controls retrieval cost | Go / gRPC |
| Kubernetes agent | Watches workloads, events, deployments, and configuration changes | Go |
| Tracing | Receives OTLP traces and builds service relationships | Python |
| Database | Collects database health and slow-query evidence | Initial PostgreSQL support |
| Changes | Records deployments, code pushes, migrations, and configuration changes | Python |
| Web UI | Shows incidents and configures log sources and watchers | Static dashboard |
| Demo | Provides a small service mesh for testing incidents | Python services / Kubernetes |

## Logs module

```mermaid
flowchart LR
    CORE[Central logic] -->|gRPC| FETCH[Go logs service]
    FETCH --> LIMITS[Limits and adaptive scope]
    LIMITS --> CONNECTORS[Provider connectors]
    CONNECTORS --> SOURCE[Customer log platform]
    SOURCE --> NORMALIZE[Normalized records in memory]
    NORMALIZE --> CORE
```

The customer's log platform remains the source of truth. The module holds fetched
records in bounded memory and returns them to the central logic. Detection summaries
and watcher checkpoints are stored in the configured SQLite or PostgreSQL control
database so monitoring survives restarts. Raw logs are not permanently copied. The
logs module has no LLM.

Supported connectors include Loki, Splunk, Elasticsearch, OpenSearch, Graylog,
Datadog, CloudWatch Logs, Google Cloud Logging, and Azure Log Analytics.

Detailed logs configuration: [docs/LOG_CONNECTORS.md](docs/LOG_CONNECTORS.md)

Engineering overview: [docs/ENGINEERING_OVERVIEW.md](docs/ENGINEERING_OVERVIEW.md)
