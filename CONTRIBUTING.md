# Contributing

The logs module is a standalone Go service. A connector must implement bounded discovery and fetching, normalize records, report provider requests and bytes, honor all limits, and avoid returning credentials or unredacted configured secrets.

Before opening a change, run:

```bash
cd modules/logs && go test ./...
cd ../.. && .venv/bin/python -m unittest discover -s tests
```

Add provider response fixtures for discovery, pagination, partial results, record normalization, and limit behavior. Live connector checks must use environment variables; never commit credentials.
