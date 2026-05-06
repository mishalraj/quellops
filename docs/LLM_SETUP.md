# Azure OpenAI (explain layer)

The **ranker** in `/incident/latest` is always **local and deterministic**. The **LLM** only powers **`/explain/latest`** and the **Insights** panel on `/ui/`: it turns the same incident JSON into a short narrative (when configured).

## Required environment variables

Set these on the **aidbg-backend** process (Docker Compose, systemd, or shell):

| Variable | Example |
|----------|---------|
| `AZURE_OPENAI_ENDPOINT` | `https://<resource>.openai.azure.com/` |
| `AZURE_OPENAI_API_KEY` | your key |
| `AZURE_OPENAI_DEPLOYMENT` | deployment name (e.g. `gpt-4.1-mini`) |
| `AZURE_OPENAI_API_VERSION` | optional; default `2024-12-01-preview` |

## Docker Compose

Uncomment and fill in the block in `docker-compose.yml` under `aidbg-backend.environment`, or export vars before `docker compose up`:

```bash
export AZURE_OPENAI_ENDPOINT="https://YOUR_RESOURCE.openai.azure.com/"
export AZURE_OPENAI_API_KEY="YOUR_KEY"
export AZURE_OPENAI_DEPLOYMENT="YOUR_DEPLOYMENT"
docker compose up -d --build aidbg-backend
```

## Verify

```bash
curl -s http://localhost:8088/explain/latest | head
```

You should see `provider: azure_openai` and a `summary` field instead of `missing_azure_openai_env`.

## Security note

Do not commit real keys. Use a `.env` file referenced by Compose (`env_file:`) that is listed in `.gitignore`.
