# Standalone Go logs module

The logs module is a deterministic infrastructure tool. It contains no LLM client,
prompt, incident reasoning, or remediation code. The central Python backend calls it
over gRPC and decides how log evidence should be combined with Kubernetes, database,
trace, and change evidence.

```text
UI --HTTP--> Python central logic --gRPC--> Go logs module --> log platforms
                              optional MCP adapter ^
```

The customer's log platform remains the source of truth. A fetch is held in process
memory only for the duration of the bounded request and returned to the caller. The
module does not copy all logs into its own file or database. The central layer may
persist small incident evidence and watcher checkpoints separately.

## Responsibilities

The Go module owns:

- provider authentication and API shapes;
- native queries saved by an administrator;
- request, page, byte, time, record, and scope ceilings;
- normalization into one `LogRecord` contract;
- duplicate identities and deterministic ordering;
- adaptive incident retrieval;
- complete-range retrieval for checkpointed watchers.

The central logic owns:

- deciding when logs are needed;
- selecting the source, service, and incident time;
- scheduling watchers and storing their checkpoints;
- correlating log evidence with other modules;
- LLM calls, explanations, suggested actions, and approval handling.

This keeps the module usable without an LLM and prevents a model from supplying
arbitrary backend URLs, credentials, or native queries.

## Supported platforms

| Provider | API used | Authentication |
| --- | --- | --- |
| Loki | `query_range` | bearer, basic, or configured headers |
| Splunk | `search/v2/jobs/export` | bearer or basic |
| Elasticsearch | `_search` | API header, bearer, or basic |
| OpenSearch | `_search` | bearer, proxy header, basic, or AWS SigV4 |
| Graylog | scripting or legacy message search | basic or token-as-username |
| Datadog | v2 log event search | API and application keys |
| CloudWatch Logs | `FilterLogEvents` | standard AWS credential chain |
| Google Cloud Logging | `entries.list` | token environment variable or application default credentials |
| Azure Log Analytics | workspace query | token environment variable or default Azure credentials |

Grafana Cloud Loki uses Basic authentication: enter the Loki instance ID as the
username and an access-policy token with `logs:read` as the password. A separate
token with `logs:write`, or one policy containing both scopes, can push test logs to
the Loki ingestion endpoint.

Use read-only credentials. Set `aws_sigv4: true`, `region`, and optionally
`aws_service: aoss` for AWS-managed OpenSearch Serverless. The default SigV4 service
name is `es` for managed domains.

## Configuration

Copy the example and keep only the sources you use:

```sh
cp modules/logs/config.example.yaml modules/logs/config.yaml
cp modules/logs/provider.env.example modules/logs/provider.env
```

Secrets are never placed in YAML. Fields such as `token_env`, `username_env`, and
`api_key_env` contain environment variable names. Their values are read lazily when
that source is queried. Optional `redact_patterns` are regular expressions applied to
messages and nested attributes before records leave the Go service.

Native queries are trusted administrator configuration. The gRPC caller supplies only
the configured source name, time range, and bounded record target.

## Run locally

```sh
export AIDBG_LOG_CONFIG="$PWD/modules/logs/config.yaml"
export AIDBG_LOG_SERVICE_TOKEN="$(openssl rand -hex 32)"
cd modules/logs
go run ./cmd/aidbg-logs
```

The service listens on gRPC port `8091` and exposes only `GET /healthz` on HTTP port
`8092`. gRPC methods other than `Health` require the service bearer token.

For the optional Docker Compose block:

```sh
export AIDBG_LOG_CONFIG_FILE="$PWD/modules/logs/config.yaml"
export AIDBG_LOG_ENV_FILE="$PWD/modules/logs/provider.env"
export AIDBG_LOG_SERVICE_TOKEN="$(openssl rand -hex 32)"
export AIDBG_LOG_API_TOKEN="$(openssl rand -hex 32)"
export AIDBG_ADMIN_API_TOKEN="$(openssl rand -hex 32)"
export AIDBG_MASTER_KEY="$(openssl rand -base64 32)"
docker compose --profile logs up --build
```

The logs container has no published host port. It is reachable by the backend only on
the internal Compose network. Leaving the `logs` profile disabled keeps the module out
of the deployment and leaves the backend log routes disabled.

The protobuf contract is in `modules/logs/api/logs/v1/logs.proto`. It exposes:

- `ListSources` for safe source names and provider types;
- `Fetch` for one bounded provider query;
- `Investigate` for recent-small then older-larger evidence retrieval;
- `FetchComplete` for watcher windows that must be complete before a checkpoint moves;
- `ApplySource` and `DeleteSource` for central configuration sync;
- `TestSource` for a one-record connection check;
- `Health` for service health.

## Central backend and UI API

Configure the Python backend with:

```sh
export AIDBG_LOG_SERVICE_GRPC=127.0.0.1:8091
export AIDBG_LOG_SERVICE_TOKEN='<same service token>'
export AIDBG_LOG_API_TOKEN="$(openssl rand -hex 32)"
export AIDBG_ADMIN_API_TOKEN="$(openssl rand -hex 32)"
export AIDBG_MASTER_KEY="$(openssl rand -base64 32)"
```

The backend provides its existing HTTP boundary for the UI:

- `GET /logs/sources`
- `POST /logs/fetch`
- `POST /logs/investigate`

Those routes require `Authorization: Bearer <AIDBG_LOG_API_TOKEN>`. They contain no
provider code and only translate HTTP requests into gRPC calls.

Open `/ui/logs.html` to configure sources. The administrator token is held only in
the browser tab. The Python control layer saves ordinary settings in SQLite and
encrypts credentials with AES-256-GCM before writing them. Credential values are
never returned by its API. On restart, the control layer reads the saved sources and
applies them to the in-memory Go service over gRPC.
The central layer also reconciles saved sources periodically, so a restarted Go
service is repopulated without manual work.

The master key must remain stable across restarts. Losing it makes saved credentials
unreadable. Rotate provider credentials through the UI; backup the key separately
from the database volume.

## Checkpointed watcher

Each saved source can enable a watcher with an interval, ingestion lag, and first-run
lookback. The Python central layer schedules the watcher and calls `FetchComplete` in
bounded chunks. A durable checkpoint advances only when the Go service confirms that
the entire chunk was fetched. Failures keep the previous checkpoint, so the same
range is retried later.

The watcher stores its checkpoint, last error, record count, request count, and byte
count in the control database. It does not store fetched log messages. Each run is
passed to a deterministic central analyzer. Error words, matching-log volume, or a
sudden record increase can trigger one bounded `Investigate` call. A five-minute
cooldown limits repeated investigations. Small redacted samples are kept only in
memory as incident evidence; catch-up windows older than five minutes do not trigger
current incidents. Three consecutive watcher failures become a source-unavailable
hypothesis.

If the service was offline for longer than one allowed scope, the watcher catches up
one `max_scope_s` chunk at a time. This keeps each provider query bounded without
skipping the gap.

The gRPC connection is plaintext for the internal Docker network. Keep port `8091`
private. A production deployment should use network policy or a service mesh for
transport encryption and identity.

## Adaptive limits

An investigation starts with `initial_window_s` and `initial_records`. If the recent
window is dense, the record limit grows by `growth_factor` up to `max_records`. If it
is complete but sparse, the module moves backward through larger non-overlapping time
bands up to `max_window_s` per request and `max_scope_s` overall.

`FetchComplete` is intended for a central watcher. It increases the limit first and
then splits dense time ranges. It either returns complete coverage or an error. The
central watcher must advance its checkpoint only after a successful response.

Defaults:

| Limit | Default |
| --- | ---: |
| initial request | 60 seconds / 50 records |
| one provider query | 15 seconds / 2 MiB / 5 pages |
| maximum query window | 900 seconds |
| maximum records per query | 500 |
| one adaptive cycle | 20 fetches / 50 backend requests |
| adaptive transfer | 16 MiB |
| normalized records per cycle | 5,000 |
| adaptive elapsed time | 60 seconds |

These limits control our retrieval work and returned data. Provider-side scan costs
can still be larger, so saved native queries should remain selective.

## Build and verify

```sh
cd modules/logs
go test ./...
go build ./cmd/aidbg-logs

cd ../..
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover \
  -s tests -p 'test_log_api.py' -v
```

Provider fixtures cover normalization, safe transport behavior, adaptive growth,
time splitting, and gRPC authentication without contacting live accounts.
