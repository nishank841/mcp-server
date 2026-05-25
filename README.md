# kube-helm MCP Server

MCP (Model Context Protocol) server that runs as a Kubernetes pod and exposes cluster management tools over HTTP/SSE.

## Repository structure

This repo contains **only the application code and Docker build**. All Kubernetes resources (namespace, serviceaccount, RBAC, service) live in the [kube-helm](https://github.com/nishank841/kube-helm) repo.

```
mcp-server/
├── mcp-server-http.py        # FastAPI HTTP/SSE MCP server
├── Dockerfile                # Python + kubectl + helm + deps
└── .github/workflows/
    └── build.yml             # Build → push → rollout restart on every push to main
```

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

## CI/CD pipeline

Every push to `main` triggers GitHub Actions to:
1. Build the Docker image
2. Push `nishank840/kube-helm-mcp:latest` to Docker Hub
3. Run `kubectl rollout restart deployment/mcp-server-app -n mcp-server` on the cluster

### Required GitHub secrets

| Secret | Value |
|--------|-------|
| `DOCKERHUB_USERNAME` | `nishank840` |
| `DOCKERHUB_TOKEN` | Docker Hub access token (Read, Write, Delete) |
| `KUBECONFIG` | Raw content of `~/.kube/config` from master node |

## Kubernetes resources (in kube-helm repo)

| File | Resource |
|------|----------|
| `manifests/namespaces/mcp-server.yaml` | Namespace |
| `manifests/serviceaccounts/mcp-server.yaml` | ServiceAccount + ClusterRole + ClusterRoleBinding |
| `manifests/services/mcp-server-service.yaml` | NodePort 30082 |
| `app-values/mcp-server/values.yaml` | Helm deployment (image, SA, GITHUB_TOKEN secret) |

## Connect Claude Code (local)

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
