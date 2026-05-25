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

import requests
from requests.auth import HTTPBasicAuth

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


# ── Jira helpers ──────────────────────────────────────────────────────────────

def _jira_auth():
    return HTTPBasicAuth(
        os.environ.get("JIRA_EMAIL", ""),
        os.environ.get("JIRA_API_TOKEN", ""),
    )

def _jira_url(path: str) -> str:
    base = os.environ.get("JIRA_URL", "").rstrip("/")
    return f"{base}/rest/api/3/{path.lstrip('/')}"

def _jira_headers():
    return {"Accept": "application/json", "Content-Type": "application/json"}


def jira_get_issue(issue_key: str) -> str:
    r = requests.get(_jira_url(f"issue/{issue_key}"), auth=_jira_auth(), headers=_jira_headers())
    if not r.ok:
        return f"ERROR: {r.status_code} {r.text}"
    d = r.json()
    f = d["fields"]
    assignee = (f.get("assignee") or {}).get("displayName", "Unassigned")
    return (
        f"Key: {d['key']}\n"
        f"Summary: {f.get('summary')}\n"
        f"Type: {f['issuetype']['name']}\n"
        f"Status: {f['status']['name']}\n"
        f"Assignee: {assignee}\n"
        f"Priority: {(f.get('priority') or {}).get('name', 'None')}\n"
        f"Reporter: {(f.get('reporter') or {}).get('displayName', 'Unknown')}\n"
        f"Description: {str(f.get('description') or '')[:300]}"
    )


def jira_create_ticket(project_key: str, summary: str, issue_type: str,
                        description: str, epic_key: str, assignee_email: str) -> str:
    payload: Dict[str, Any] = {
        "fields": {
            "project": {"key": project_key},
            "summary": summary,
            "issuetype": {"name": issue_type},
            "description": {
                "type": "doc", "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": description}]}],
            },
        }
    }
    if assignee_email:
        # look up accountId by email first
        search = requests.get(
            _jira_url(f"user/search?query={assignee_email}"),
            auth=_jira_auth(), headers=_jira_headers(),
        )
        if search.ok and search.json():
            payload["fields"]["assignee"] = {"accountId": search.json()[0]["accountId"]}

    if epic_key:
        # try parent (next-gen) first; fall back to Epic Link (classic)
        payload["fields"]["parent"] = {"key": epic_key}

    r = requests.post(_jira_url("issue"), auth=_jira_auth(),
                      headers=_jira_headers(), json=payload)
    if not r.ok:
        # retry without parent for classic projects using Epic Link field
        if epic_key and "parent" in payload["fields"]:
            del payload["fields"]["parent"]
            payload["fields"]["customfield_10014"] = epic_key
            r = requests.post(_jira_url("issue"), auth=_jira_auth(),
                              headers=_jira_headers(), json=payload)
    if not r.ok:
        return f"ERROR: {r.status_code} {r.text}"
    key = r.json()["key"]
    base = os.environ.get("JIRA_URL", "").rstrip("/")
    return f"SUCCESS: Created {issue_type} {key}\nURL: {base}/browse/{key}"


def jira_create_epic(project_key: str, summary: str, description: str) -> str:
    payload = {
        "fields": {
            "project": {"key": project_key},
            "summary": summary,
            "issuetype": {"name": "Epic"},
            "description": {
                "type": "doc", "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": description}]}],
            },
        }
    }
    r = requests.post(_jira_url("issue"), auth=_jira_auth(),
                      headers=_jira_headers(), json=payload)
    if not r.ok:
        return f"ERROR: {r.status_code} {r.text}"
    key = r.json()["key"]
    base = os.environ.get("JIRA_URL", "").rstrip("/")
    return f"SUCCESS: Created Epic {key}\nURL: {base}/browse/{key}"


def jira_change_assignee(issue_key: str, assignee_email: str) -> str:
    search = requests.get(
        _jira_url(f"user/search?query={assignee_email}"),
        auth=_jira_auth(), headers=_jira_headers(),
    )
    if not search.ok or not search.json():
        return f"ERROR: Could not find user with email '{assignee_email}'"
    account_id = search.json()[0]["accountId"]
    r = requests.put(
        _jira_url(f"issue/{issue_key}/assignee"),
        auth=_jira_auth(), headers=_jira_headers(),
        json={"accountId": account_id},
    )
    if r.status_code == 204:
        return f"SUCCESS: {issue_key} assigned to {assignee_email}"
    return f"ERROR: {r.status_code} {r.text}"


def jira_close_issue(issue_key: str) -> str:
    # get available transitions
    r = requests.get(_jira_url(f"issue/{issue_key}/transitions"),
                     auth=_jira_auth(), headers=_jira_headers())
    if not r.ok:
        return f"ERROR fetching transitions: {r.status_code} {r.text}"
    transitions = r.json().get("transitions", [])
    done_id = None
    for t in transitions:
        if t["name"].lower() in ("done", "closed", "resolved", "close", "complete"):
            done_id = t["id"]
            break
    if not done_id:
        names = [t["name"] for t in transitions]
        return f"ERROR: No 'Done/Closed' transition found. Available: {names}"
    r2 = requests.post(
        _jira_url(f"issue/{issue_key}/transitions"),
        auth=_jira_auth(), headers=_jira_headers(),
        json={"transition": {"id": done_id}},
    )
    if r2.status_code == 204:
        return f"SUCCESS: {issue_key} closed/done"
    return f"ERROR: {r2.status_code} {r2.text}"


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
    {
        "name": "jira_get_issue",
        "description": "Get details of a Jira issue by key (e.g. PROJ-123)",
        "inputSchema": {
            "type": "object",
            "properties": {"issue_key": {"type": "string", "description": "Jira issue key e.g. PROJ-123"}},
            "required": ["issue_key"],
        },
    },
    {
        "name": "jira_create_ticket",
        "description": "Create a Jira ticket (Story, Task, Bug) optionally linked to an epic",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_key":     {"type": "string", "description": "Jira project key e.g. PROJ"},
                "summary":         {"type": "string", "description": "Ticket title/summary"},
                "issue_type":      {"type": "string", "description": "Story, Task, or Bug"},
                "description":     {"type": "string", "description": "Ticket description"},
                "epic_key":        {"type": "string", "description": "Epic key to link under (optional)"},
                "assignee_email":  {"type": "string", "description": "Email of assignee (optional)"},
            },
            "required": ["project_key", "summary", "issue_type", "description"],
        },
    },
    {
        "name": "jira_create_epic",
        "description": "Create a new Jira Epic in a project",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_key":  {"type": "string", "description": "Jira project key e.g. PROJ"},
                "summary":      {"type": "string", "description": "Epic title"},
                "description":  {"type": "string", "description": "Epic description"},
            },
            "required": ["project_key", "summary", "description"],
        },
    },
    {
        "name": "jira_change_assignee",
        "description": "Change the assignee of a Jira issue",
        "inputSchema": {
            "type": "object",
            "properties": {
                "issue_key":       {"type": "string", "description": "Jira issue key e.g. PROJ-123"},
                "assignee_email":  {"type": "string", "description": "Email of the new assignee"},
            },
            "required": ["issue_key", "assignee_email"],
        },
    },
    {
        "name": "jira_close_issue",
        "description": "Close or mark a Jira issue as Done",
        "inputSchema": {
            "type": "object",
            "properties": {"issue_key": {"type": "string", "description": "Jira issue key e.g. PROJ-123"}},
            "required": ["issue_key"],
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
    "jira_get_issue":        lambda a: jira_get_issue(a["issue_key"]),
    "jira_create_ticket":    lambda a: jira_create_ticket(
                                a["project_key"], a["summary"], a["issue_type"],
                                a.get("description", ""), a.get("epic_key", ""), a.get("assignee_email", "")),
    "jira_create_epic":      lambda a: jira_create_epic(a["project_key"], a["summary"], a.get("description", "")),
    "jira_change_assignee":  lambda a: jira_change_assignee(a["issue_key"], a["assignee_email"]),
    "jira_close_issue":      lambda a: jira_close_issue(a["issue_key"]),
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
