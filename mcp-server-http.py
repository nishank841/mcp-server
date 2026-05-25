#!/usr/bin/env python3
"""
MCP Kubernetes Server — HTTP/SSE transport
Drop-in replacement for mcp-kubernetes-server.py, designed to run as a K8s pod.
Uses in-cluster ServiceAccount for kubectl and GITHUB_TOKEN from env/secret.
"""

import asyncio
import json
import logging
import os
import subprocess
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="kube-helm MCP Server")

# session_id -> asyncio.Queue for SSE responses
_sessions: Dict[str, asyncio.Queue] = {}


# ── kubectl helpers ────────────────────────────────────────────────────────────

def run_kubectl(args: List[str], namespace: Optional[str] = None) -> str:
    cmd = ["kubectl"]
    if namespace:
        cmd.extend(["-n", namespace])
    cmd.extend(args)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.stdout if result.returncode == 0 else f"Error: {result.stderr}"
    except Exception as e:
        return f"Exception: {str(e)}"


def get_namespaces() -> str:
    return run_kubectl(["get", "namespaces", "-o", "wide"])

def get_pods(namespace: Optional[str] = None) -> str:
    return run_kubectl(["get", "pods", "-o", "wide"], namespace)

def get_services(namespace: Optional[str] = None) -> str:
    return run_kubectl(["get", "services", "-o", "wide"], namespace)

def get_ingress(namespace: Optional[str] = None) -> str:
    return run_kubectl(["get", "ingress", "-o", "wide"], namespace)

def get_deployment_status(name: str, namespace: str) -> str:
    return run_kubectl(["get", "deployment", name, "-o", "wide"], namespace)

def get_load_balancer_url(service_name: str, namespace: str) -> str:
    for jsonpath in [
        "{.status.loadBalancer.ingress[*].hostname}",
        "{.status.loadBalancer.ingress[*].ip}",
    ]:
        out = run_kubectl(["get", "service", service_name, f"-o=jsonpath={jsonpath}"], namespace)
        if out and not out.startswith("Error"):
            return f"http://{out}"
    svc_type = run_kubectl(
        ["get", "service", service_name, "-o", "jsonpath={.spec.type}"], namespace
    )
    if svc_type == "NodePort":
        node_port = run_kubectl(
            ["get", "service", service_name, "-o", "jsonpath={.spec.ports[0].nodePort}"], namespace
        )
        return f"NodePort service. Access via: http://<node-ip>:{node_port}"
    return "Load Balancer URL not available yet"

def apply_manifest(yaml_content: str) -> str:
    try:
        result = subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=yaml_content, capture_output=True, text=True, timeout=30
        )
        return result.stdout if result.returncode == 0 else f"Error: {result.stderr}"
    except Exception as e:
        return f"Exception: {str(e)}"

def helm_install(release_name: str, chart_path: str, values_file: str, namespace: str) -> str:
    cmd = [
        "helm", "upgrade", "--install", release_name,
        chart_path, "-f", values_file, "-n", namespace, "--create-namespace",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return result.stdout if result.returncode == 0 else f"Error: {result.stderr}"
    except Exception as e:
        return f"Exception: {str(e)}"


# ── GitHub provisioning ────────────────────────────────────────────────────────

def _language_defaults(language: str) -> Dict[str, Any]:
    mapping = {
        "python":     {"image": "nginx:latest", "port": 80},
        "nodejs":     {"image": "nginx:latest", "port": 80},
        "node":       {"image": "nginx:latest", "port": 80},
        "javascript": {"image": "nginx:latest", "port": 80},
        "go":         {"image": "nginx:latest", "port": 80},
        "golang":     {"image": "nginx:latest", "port": 80},
        "java":       {"image": "nginx:latest", "port": 80},
        "ruby":       {"image": "nginx:latest", "port": 80},
        "php":        {"image": "nginx:latest", "port": 80},
    }
    return mapping.get(language.lower(), {"image": "nginx:latest", "port": 80})

def _build_files(app_name: str, language: str) -> Dict[str, str]:
    lang = _language_defaults(language)
    image = lang["image"]
    port = lang["port"]

    namespace_yaml = f"""apiVersion: v1
kind: Namespace
metadata:
  name: {app_name}
  labels:
    language: {language.lower()}
"""
    pvc_yaml = f"""apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {app_name}-pvc
  namespace: {app_name}
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: kube-aws-gp3-storage
  resources:
    requests:
      storage: 1Gi
"""
    service_yaml = f"""apiVersion: v1
kind: Service
metadata:
  name: {app_name}-svc
  namespace: {app_name}
spec:
  selector:
    app.kubernetes.io/name: app
    app.kubernetes.io/instance: {app_name}
  ports:
  - port: 80
    targetPort: {port}
  type: NodePort
"""
    ingress_yaml = f"""apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {app_name}-ingress
  namespace: {app_name}
  annotations:
    alb.ingress.kubernetes.io/scheme: internet-facing
    alb.ingress.kubernetes.io/target-type: instance
    alb.ingress.kubernetes.io/listen-ports: '[{{"HTTP": 80}}]'
    alb.ingress.kubernetes.io/healthcheck-path: /
    alb.ingress.kubernetes.io/success-codes: "200-399"
    alb.ingress.kubernetes.io/healthcheck-interval-seconds: "15"
    alb.ingress.kubernetes.io/healthcheck-timeout-seconds: "5"
spec:
  ingressClassName: alb
  rules:
    - http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: {app_name}-svc
                port:
                  number: 80
"""
    values_yaml = f"""replicaCount: 1

namespace: {app_name}

persistence:
  enabled: true
  claimName: {app_name}-pvc
  mountPath: /data

containers:
  - name: {app_name}
    image: {image}
    port: {port}
    resources:
      requests:
        cpu: 100m
        memory: 128Mi
      limits:
        cpu: 300m
        memory: 256Mi
    volumeMounts:
      - name: app-storage
        mountPath: /app/data

volumes:
  - name: app-storage
    persistentVolumeClaim:
      claimName: {app_name}-pvc
"""
    return {
        f"manifests/namespaces/{app_name}.yaml":                   namespace_yaml,
        f"manifests/persistent-volume-claims/{app_name}-pvc.yaml": pvc_yaml,
        f"manifests/services/{app_name}-service.yaml":             service_yaml,
        f"manifests/ingress/{app_name}-ingress.yaml":              ingress_yaml,
        f"app-values/{app_name}/values.yaml":                      values_yaml,
    }

def provision_app_infrastructure(app_name: str, language: str) -> str:
    try:
        from github import Github, GithubException, Auth as GHAuth
    except ImportError:
        return "ERROR: PyGithub not installed."

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        return "ERROR: GITHUB_TOKEN environment variable is not set."

    repo_name = "nishank841/kube-helm"
    branch_name = f"infra/provision-{app_name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    try:
        gh = Github(auth=GHAuth.Token(token))
        repo = gh.get_repo(repo_name)
        base_sha = repo.get_branch(repo.default_branch).commit.sha
        repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_sha)

        files = _build_files(app_name, language)
        committed = []
        for path, content in files.items():
            try:
                existing = repo.get_contents(path, ref=branch_name)
                repo.update_file(
                    path=path,
                    message=f"chore: update {path} for {app_name}",
                    content=content, sha=existing.sha, branch=branch_name,
                )
            except GithubException:
                repo.create_file(
                    path=path,
                    message=f"feat: add {path} for {app_name}",
                    content=content, branch=branch_name,
                )
            committed.append(path)

        pr = repo.create_pull(
            title=f"feat: provision {language} app infrastructure for {app_name}",
            body=(
                f"## Infrastructure provisioning for `{app_name}`\n\n"
                f"**Language:** {language}\n\n"
                "### Files created\n"
                + "\n".join(f"- `{p}`" for p in committed)
                + "\n\n_Generated by kube-helm MCP Server (K8s pod)._"
            ),
            head=branch_name,
            base=repo.default_branch,
        )
        return (
            f"SUCCESS: PR created for '{app_name}' ({language})\n"
            f"PR URL: {pr.html_url}\n"
            "Files:\n" + "\n".join(f"  - {p}" for p in committed)
        )
    except Exception as e:
        return f"ERROR: {str(e)}"


# ── MCP tool schema ────────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "provision_app_infrastructure",
        "description": "Create a GitHub PR with all Kubernetes infrastructure files for a new app.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "app_name": {"type": "string", "description": "App/namespace name"},
                "language": {"type": "string", "description": "Language: python, nodejs, go, java, ruby, php"},
            },
            "required": ["app_name", "language"],
        },
    },
    {
        "name": "get_namespaces",
        "description": "List all namespaces in the cluster",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_pods",
        "description": "Get pods in a namespace",
        "inputSchema": {"type": "object", "properties": {"namespace": {"type": "string"}}},
    },
    {
        "name": "get_services",
        "description": "Get services in a namespace",
        "inputSchema": {"type": "object", "properties": {"namespace": {"type": "string"}}},
    },
    {
        "name": "get_ingress",
        "description": "Get ingress resources",
        "inputSchema": {"type": "object", "properties": {"namespace": {"type": "string"}}},
    },
    {
        "name": "get_load_balancer_url",
        "description": "Get the Load Balancer URL for a service",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service_name": {"type": "string"},
                "namespace": {"type": "string"},
            },
            "required": ["service_name", "namespace"],
        },
    },
    {
        "name": "apply_manifest",
        "description": "Apply a Kubernetes manifest YAML string",
        "inputSchema": {
            "type": "object",
            "properties": {"yaml_content": {"type": "string"}},
            "required": ["yaml_content"],
        },
    },
    {
        "name": "get_deployment_status",
        "description": "Get deployment rollout status",
        "inputSchema": {
            "type": "object",
            "properties": {"name": {"type": "string"}, "namespace": {"type": "string"}},
            "required": ["name", "namespace"],
        },
    },
]

TOOL_DISPATCH = {
    "provision_app_infrastructure": lambda a: provision_app_infrastructure(a["app_name"], a["language"]),
    "get_namespaces":        lambda _: get_namespaces(),
    "get_pods":              lambda a: get_pods(a.get("namespace")),
    "get_services":          lambda a: get_services(a.get("namespace")),
    "get_ingress":           lambda a: get_ingress(a.get("namespace")),
    "get_load_balancer_url": lambda a: get_load_balancer_url(a["service_name"], a["namespace"]),
    "apply_manifest":        lambda a: apply_manifest(a["yaml_content"]),
    "get_deployment_status": lambda a: get_deployment_status(a["name"], a["namespace"]),
}


# ── JSON-RPC dispatcher ────────────────────────────────────────────────────────

def handle_jsonrpc(req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    method = req.get("method", "")
    req_id = req.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "kube-helm-mcp", "version": "2.0.0"},
            },
        }

    if method == "notifications/initialized":
        return None  # notification — no response

    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}

    if method == "tools/call":
        tool_name = req["params"]["name"]
        arguments = req["params"].get("arguments", {})
        handler = TOOL_DISPATCH.get(tool_name)
        if not handler:
            text, is_error = f"Unknown tool: {tool_name}", True
        else:
            try:
                text = handler(arguments)
                is_error = isinstance(text, str) and (text.startswith("ERROR") or text.startswith("Error"))
            except Exception as e:
                text, is_error = f"ERROR: {str(e)}", True
        return {
            "jsonrpc": "2.0", "id": req_id,
            "result": {"content": [{"type": "text", "text": text}], "isError": is_error},
        }

    return {
        "jsonrpc": "2.0", "id": req_id,
        "error": {"code": -32601, "message": "Method not found"},
    }


# ── HTTP / SSE endpoints ───────────────────────────────────────────────────────

@app.get("/sse")
async def sse_endpoint(request: Request):
    sid = str(uuid.uuid4())
    _sessions[sid] = asyncio.Queue()
    logger.info("SSE session opened: %s", sid)

    async def stream():
        yield f"event: endpoint\ndata: /message?sessionId={sid}\n\n"
        try:
            while not await request.is_disconnected():
                try:
                    msg = await asyncio.wait_for(_sessions[sid].get(), timeout=20)
                    if msg is not None:
                        yield f"event: message\ndata: {json.dumps(msg)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _sessions.pop(sid, None)
            logger.info("SSE session closed: %s", sid)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.post("/message")
async def message_endpoint(request: Request, sessionId: str):
    queue = _sessions.get(sessionId)
    if not queue:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    body = await request.json()
    response = handle_jsonrpc(body)
    if response is not None:
        await queue.put(response)
    return {}


@app.get("/health")
async def health():
    return {"status": "ok", "sessions": len(_sessions)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
