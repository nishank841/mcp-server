# kube-helm MCP Server

MCP (Model Context Protocol) server that runs as a Kubernetes pod and exposes cluster management tools over HTTP/SSE.

## Tools exposed

| Tool | Description |
|------|-------------|
| `provision_app_infrastructure` | Create a GitHub PR with K8s manifests for a new app |
| `get_namespaces` | List all cluster namespaces |
| `get_pods` | Get pods in a namespace |
| `get_services` | Get services in a namespace |
| `get_ingress` | Get ingress resources |
| `get_load_balancer_url` | Get LB URL for a service |
| `apply_manifest` | Apply a YAML manifest to the cluster |
| `get_deployment_status` | Get deployment rollout status |

## Build & push Docker image

```bash
docker build -t nishank841/kube-helm-mcp:latest .
docker push nishank841/kube-helm-mcp:latest
```

## Deploy to Kubernetes

```bash
# 1. Create the GitHub token secret
kubectl create secret generic mcp-github-token \
  --from-literal=GITHUB_TOKEN=ghp_yourtoken \
  -n mcp-server

# 2. Apply all manifests
kubectl apply -f manifests/

# 3. Verify pod is running
kubectl get pods -n mcp-server
```

## Connect Claude Code

Add to `~/.claude.json` under `mcpServers`:

```json
{
  "mcpServers": {
    "kube-helm": {
      "type": "sse",
      "url": "http://<node-ip>:30082/sse"
    }
  }
}
```

Get a node IP: `kubectl get nodes -o wide`

## Health check

```
GET http://<node-ip>:30082/health
```
