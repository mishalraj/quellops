# QuellOps Kubernetes agent chart

Install or upgrade one agent per cluster:

```bash
helm upgrade --install quellops-agent ./charts/quellops-agent \
  --namespace quellops-system --create-namespace \
  --set controlPlaneURL=https://quellops.example.com \
  --set clusterID=production-eu \
  --set 'watchNamespaces={production,checkout}' \
  --set credentials.bootstrapToken="$QUELLOPS_BOOTSTRAP_TOKEN"
```

The chart creates read-only Roles only in `watchNamespaces`; the portal may select a
subset for event delivery. Set `watchNamespaces={*}` only when cluster-wide discovery
and node monitoring are required. Its persistent volume
keeps unsent events and the cluster-specific token across pod restarts. For an
existing secret, set `credentials.existingSecret`; it must contain
`bootstrap-token` and may contain a rotated `cluster-token`.
