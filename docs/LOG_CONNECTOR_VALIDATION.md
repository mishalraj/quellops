# Log connector validation

Every connector has deterministic contract tests. The live conformance test also
checks discovery, generated scope, authentication and a bounded fetch against an
actual provider API.

Run the local Loki validation:

```bash
./scripts/test_loki_connector_live.sh
```

Run provider-compatible Splunk and Datadog API simulations:

```bash
./scripts/test_hosted_connectors_simulated.sh
```

These simulations validate request paths, authentication headers, discovery,
generated scopes, response parsing, and bounded fetching. They do not replace a
final test against the hosted products.

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
