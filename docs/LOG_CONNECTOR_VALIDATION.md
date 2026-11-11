# Log connector validation

Every connector has deterministic contract tests. The live conformance test also
checks discovery, generated scope, authentication and a bounded fetch against an
actual provider API.

Run the local Loki validation:

```bash
./scripts/test_loki_connector_live.sh
```

For a hosted provider, set `QUELLOPS_LIVE_<PROVIDER>` to JSON with `values` and
`credentials`, then run:

```bash
cd modules/logs
go test ./internal/logs -run TestLiveProviders -v
```

Example:

```bash
export QUELLOPS_LIVE_DATADOG='{"values":{"url":"https://api.datadoghq.com"},"credentials":{"api_key":"...","app_key":"..."}}'
```

Supported names are `LOKI`, `ELASTICSEARCH`, `OPENSEARCH`, `SPLUNK`, `GRAYLOG`,
`DATADOG`, `CLOUDWATCH`, `GCP`, and `AZURE`. Provider secrets stay in environment
variables and are never written to test output.

