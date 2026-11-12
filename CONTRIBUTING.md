# Contributing

Keep modules independent: infrastructure modules collect and normalize evidence;
central logic correlates it. Never commit credentials, customer data, generated
bytecode, local databases, or raw production logs.

Before opening a change, run the relevant checks:

```bash
PYTHONPATH=backend/src python -m unittest discover -s tests
ruff check .
ruff format --check .
(cd modules/logs && gofmt -l . && go test ./... && go vet ./...)
(cd agent && gofmt -l . && go test ./... && go vet ./...)
helm lint charts/quellops-agent --set controlPlaneURL=http://example --set clusterID=test --set credentials.bootstrapToken=test
docker compose config --quiet
```

Logs connectors must enforce request, byte, record, time, and pagination limits and
must redact configured secrets. Kubernetes changes must preserve default-deny event
delivery, namespace-limited RBAC, durable queue behavior, and per-cluster credentials.
Tests that require live providers must use environment variables and remain skipped
when credentials are absent.
