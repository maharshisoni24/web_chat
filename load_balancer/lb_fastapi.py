"""
lb_fastapi.py — Async FastAPI load balancer (replaces ThreadingHTTPServer)
Handles /message, /feed, /health, /lb/status, /ws proxy
"""
import os, sys, time, json, asyncio, logging, random, hashlib, uuid, urllib.parse
from typing import List, Optional, Dict, Any
import httpx
import websockets
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("LB")

# ── Backend config ─────────────────────────────────────────────────────────
BACKEND_URLS = [u.strip() for u in os.environ.get("BACKENDS", "").split(",") if u.strip()]
if not BACKEND_URLS:
    BACKEND_URLS = [
        "http://10.1.75.53:5206",
        "http://10.1.75.53:5207",
        "http://10.1.75.53:5208",
    ]

OVERLOAD_THRESHOLD = float(os.environ.get("THRESHOLD", "500"))
HEALTH_INTERVAL   = float(os.environ.get("HEALTH_INTERVAL", "5"))

class BackendState:
    def __init__(self, url: str, node_id: str):
        self.url = url
        self.node_id = node_id
        self.healthy = True
        self.active = 0
        self.total = 0
        self.failed = 0
        self._samples: List[float] = []
        self.last_latency = 0.0

    def record_latency(self, ms: float):
        self._samples.append(ms)
        if len(self._samples) > 20:
            self._samples.pop(0)
        self.last_latency = ms

    def avg_latency(self) -> float:
        return (sum(self._samples) / len(self._samples)) if self._samples else 1.0

    def score(self) -> float:
        return self.avg_latency() * (1 + self.active)

    def to_dict(self):
        return {
            "id": self.node_id, "url": self.url,
            "healthy": self.healthy,
            "active_connections": self.active,
            "total_requests": self.total,
            "failed_requests": self.failed,
            "avg_latency_ms": round(self.avg_latency(), 2),
            "performance_score": round(self.score(), 2),
        }

backends: List[BackendState] = [
    BackendState(url, f"Sys{i+2}") for i, url in enumerate(BACKEND_URLS)
]
START_TIME = time.time()
REQ_COUNT = 0

def select_backend(exclude: set = None) -> Optional[BackendState]:
    healthy = [b for b in backends if b.healthy and (exclude is None or b.node_id not in exclude)]
    if not healthy:
        return None
    scored = sorted(healthy, key=lambda b: b.score())
    for b in scored:
        if b.score() <= OVERLOAD_THRESHOLD:
            return b
    return scored[0]

# ── Health check loop ──────────────────────────────────────────────────────
async def health_loop():
    async with httpx.AsyncClient(timeout=2.0) as client:
        while True:
            for b in backends:
                try:
                    t0 = time.time()
                    r = await client.get(f"{b.url}/health")
                    ms = (time.time() - t0) * 1000
                    if r.status_code == 200:
                        if not b.healthy:
                            log.info(f"[UP] {b.node_id} restored ({ms:.0f}ms)")
                        b.healthy = True
                        b.record_latency(ms)
                    else:
                        if b.healthy:
                            log.warning(f"[DOWN] {b.node_id} status {r.status_code}")
                        b.healthy = False
                except Exception as e:
                    if b.healthy:
                        log.warning(f"[DOWN] {b.node_id}: {e}")
                    b.healthy = False
            await asyncio.sleep(HEALTH_INTERVAL)

app = FastAPI(title="Async Load Balancer")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Shared persistent httpx client — created once at startup, reused always ─
# Without this, each request creates a new client = no keep-alive, no pooling
# Under 1000 concurrent users this caused 100% timeouts / connection exhaustion
_http_client: Optional[httpx.AsyncClient] = None

def get_client() -> httpx.AsyncClient:
    return _http_client  # type: ignore

@app.on_event("startup")
async def startup():
    global _http_client
    _http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=3.0, read=5.0, write=3.0, pool=10.0),
        limits=httpx.Limits(
            max_connections=2000,        # enough for 1000 users × 2 retries
            max_keepalive_connections=500,  # keep many connections alive per backend
            keepalive_expiry=60,
        ),
    )
    asyncio.create_task(health_loop())
    log.info(f"LB started. pool=2000 keepalive=500. Backends: {BACKEND_URLS}")

@app.on_event("shutdown")
async def shutdown():
    if _http_client:
        await _http_client.aclose()

# ── Status ─────────────────────────────────────────────────────────────────
@app.get("/lb/status")
async def lb_status():
    return {
        "service": "LoadBalancer",
        "algorithm": "performance",
        "uptime_seconds": round(time.time() - START_TIME, 1),
        "total_requests_proxied": REQ_COUNT,
        "backends_count": len(backends),
        "healthy_backends_count": sum(1 for b in backends if b.healthy),
        "backends": [b.to_dict() for b in backends],
    }

@app.get("/")
async def root():
    return {
        "service": "WebChat Load Balancer",
        "status": "running",
        "healthy_backends": sum(1 for b in backends if b.healthy),
        "routes": {
            "POST /message": "Send a message (fields: client-name, msg)",
            "GET /feed": "Get all messages",
            "GET /lb/status": "Load balancer stats",
            "WS /ws": "WebSocket chat connection",
        }
    }

# ── Proxy all requests ─────────────────────────────────────────────────────

FEED_LIMIT = 500  # max messages returned — prevents OOM under 1000-user load

@app.get("/feed")
async def aggregated_feed():
    """Aggregate /feed from ALL backends, dedup by msg-id, return latest 500."""
    client = get_client()
    all_messages = {}
    tasks = [client.get(f"{b.url}/feed") for b in backends if b.healthy]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            continue
        try:
            data = r.json()
            for msg in data.get("messages", []):
                mid = msg.get("id") or msg.get("msg-id")
                if mid and mid not in all_messages:
                    all_messages[mid] = msg
        except Exception:
            pass
    msgs = sorted(all_messages.values(), key=lambda m: m.get("timestamp", 0))
    msgs = msgs[-FEED_LIMIT:]
    return JSONResponse({"messages": msgs, "count": len(msgs)})


@app.post("/message")
async def post_message(request: Request):
    """POST /message — route to ONE best backend (performance-selected).

    Routes to a single backend instead of broadcasting to all 3.
    Broadcasting caused 3× connection multiplier that exhausted the pool
    under 200+ concurrent users (200 users × 3 backends = 600 connections).
    Dedup (INSERT OR IGNORE on msg_id) prevents duplicates if retried.
    """
    body = await request.body()
    content_type = request.headers.get("content-type", "application/x-www-form-urlencoded")

    # Inject msg-id so retries to different backends dedup correctly
    msg_id = str(uuid.uuid4())
    if "application/json" in content_type:
        try:
            payload = json.loads(body)
            payload.setdefault("msg-id", msg_id)
            body = json.dumps(payload).encode()
        except Exception:
            pass
    else:
        try:
            params = dict(urllib.parse.parse_qsl(body.decode()))
            params.setdefault("msg-id", msg_id)
            body = urllib.parse.urlencode(params).encode()
            content_type = "application/x-www-form-urlencoded"
        except Exception:
            pass

    # Try up to 3 backends (retry on failure)
    tried = set()
    client = get_client()
    last_error = None

    for attempt in range(len(backends)):
        backend = select_backend(exclude=tried)
        if not backend:
            break
        tried.add(backend.node_id)
        backend.active += 1
        backend.total += 1
        t0 = time.time()
        try:
            r = await client.post(
                f"{backend.url}/message",
                content=body,
                headers={"Content-Type": content_type,
                         "X-Forwarded-For": request.client.host if request.client else "unknown"},
            )
            ms = (time.time() - t0) * 1000
            backend.record_latency(ms)
            if r.status_code < 500:
                return Response(
                    content=r.content,
                    status_code=r.status_code,
                    headers={"Content-Type": "application/json",
                             "X-Load-Balancer": "Sys1",
                             "X-Backend": backend.node_id},
                )
            # 5xx from backend — retry on another
            last_error = f"HTTP {r.status_code}"
            backend.failed += 1
        except Exception as e:
            backend.active = max(0, backend.active - 1)
            backend.failed += 1
            last_error = str(e)
            log.warning(f"[/message] {backend.node_id} attempt {attempt+1} failed: {e}")
            continue
        finally:
            backend.active = max(0, backend.active - 1)

    return JSONResponse({"error": "All backends failed", "detail": str(last_error)}, status_code=503)


@app.websocket("/ws")
async def ws_proxy(client_ws: WebSocket):
    """Proxy WebSocket connection to a selected healthy backend."""
    await client_ws.accept()

    backend = select_backend()
    if not backend:
        await client_ws.close(code=1013, reason="No healthy backends")
        return

    # Convert http:// backend URL to ws://
    backend_ws_url = backend.url.replace("http://", "ws://").replace("https://", "wss://") + "/ws"
    backend.active += 1
    log.info(f"[WS] Client -> {backend.node_id} ({backend_ws_url})")

    try:
        async with websockets.connect(backend_ws_url) as backend_ws:

            async def client_to_backend():
                try:
                    async for msg in client_ws.iter_text():
                        await backend_ws.send(msg)
                except (WebSocketDisconnect, Exception):
                    pass

            async def backend_to_client():
                try:
                    async for msg in backend_ws:
                        await client_ws.send_text(msg)
                except (WebSocketDisconnect, Exception):
                    pass

            # Run both directions concurrently until either side disconnects
            done, pending = await asyncio.wait(
                [
                    asyncio.ensure_future(client_to_backend()),
                    asyncio.ensure_future(backend_to_client()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()

    except Exception as e:
        log.warning(f"[WS] {backend.node_id} proxy error: {e}")
        try:
            await client_ws.close(code=1011, reason="Backend error")
        except Exception:
            pass
    finally:
        backend.active = max(0, backend.active - 1)
        log.info(f"[WS] Connection closed ({backend.node_id})")


@app.api_route("/{path:path}", methods=["GET","POST","PUT","DELETE","PATCH","HEAD","OPTIONS"])
async def proxy(request: Request, path: str):
    global REQ_COUNT
    REQ_COUNT += 1

    backend = select_backend()
    if not backend:
        return JSONResponse({"error": "No healthy backends"}, status_code=503)

    url = f"{backend.url}/{path}"
    if request.url.query:
        url += f"?{request.url.query}"

    headers = dict(request.headers)
    headers.pop("host", None)
    headers["X-Forwarded-For"] = request.client.host if request.client else "unknown"

    body = await request.body()
    backend.active += 1
    backend.total += 1
    t0 = time.time()

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body,
            )
        ms = (time.time() - t0) * 1000
        backend.active -= 1
        backend.record_latency(ms)

        resp_headers = dict(r.headers)
        resp_headers["X-Load-Balancer"] = "Sys1"
        resp_headers["X-Selected-Backend"] = backend.node_id
        resp_headers.pop("transfer-encoding", None)

        log.info(f"[{REQ_COUNT}] {request.method} /{path} -> {backend.node_id} [{r.status_code}] {ms:.0f}ms")
        return Response(content=r.content, status_code=r.status_code, headers=resp_headers)

    except Exception as e:
        ms = (time.time() - t0) * 1000
        backend.active = max(0, backend.active - 1)
        backend.failed += 1
        backend.healthy = False
        log.error(f"[{REQ_COUNT}] Backend {backend.node_id} failed: {e}")
        # Retry with another backend
        retry = select_backend()
        if retry and retry is not backend:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    r = await client.request(method=request.method, url=f"{retry.url}/{path}",
                                             headers=headers, content=body)
                retry.record_latency((time.time() - t0) * 1000)
                return Response(content=r.content, status_code=r.status_code,
                                headers={"X-Load-Balancer":"Sys1","X-Selected-Backend":retry.node_id})
            except Exception as e2:
                pass
        return JSONResponse({"error": f"All backends failed: {e}"}, status_code=502)

if __name__ == "__main__":
    import uvicorn, argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--backends", default="")
    parser.add_argument("--threshold", type=float, default=500.0)
    args = parser.parse_args()
    if args.backends:
        os.environ["BACKENDS"] = args.backends
    os.environ["THRESHOLD"] = str(args.threshold)
    # Reinit backends from env
    import importlib, lb_fastapi as self_mod
    self_mod.BACKEND_URLS[:] = [u.strip() for u in args.backends.split(",") if u.strip()]
    self_mod.backends[:] = [BackendState(url, f"Sys{i+2}") for i, url in enumerate(self_mod.BACKEND_URLS)]
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
