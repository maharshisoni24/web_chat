"""
main.py — FastAPI WebSocket backend with Private Rooms & Persistence
===================================================================
WebSocket : ws://localhost:<PORT>/ws
Health    : GET  /health
Feed      : GET  /feed          — returns all messages (required by assignment)
Message   : POST /message       — submit a message via HTTP (required by assignment)
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
import logging
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import sys
from pathlib import Path

logging.getLogger("asyncio").setLevel(logging.CRITICAL)

server_dir = Path(__file__).parent
if str(server_dir) not in sys.path:
    sys.path.insert(0, str(server_dir))

try:
    import database as db
except ImportError:
    from server import database as db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

db.init_db()

app = FastAPI(title="Group Chat Server with Private Rooms", version="2.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Connection Manager ────────────────────────────────────────────────────────
class RoomConnectionManager:
    """Manages WebSocket connections partitioned by room_id."""

    def __init__(self) -> None:
        self._clients: Dict[WebSocket, dict] = {}
        self._room_members: Dict[str, Set[WebSocket]] = {}

    async def _send(self, ws: WebSocket, payload: dict) -> None:
        try:
            await ws.send_text(json.dumps(payload))
        except Exception:
            pass

    async def broadcast_to_room(self, room_id: str, payload: dict, exclude: WebSocket | None = None) -> None:
        members = list(self._room_members.get(room_id, set()))
        for ws in members:
            if ws is not exclude:
                await self._send(ws, payload)

    def get_room_users(self, room_id: str) -> list[str]:
        members = self._room_members.get(room_id, set())
        users = []
        for ws in members:
            info = self._clients.get(ws)
            if info and info.get("username"):
                users.append(info["username"])
        return users

    async def register_user(self, ws: WebSocket, username: str) -> str:
        client_id = str(uuid.uuid4())[:8]
        self._clients[ws] = {
            "id": client_id,
            "username": username,
            "room_id": "global"
        }
        await self.switch_room(ws, "global")
        return client_id

    async def switch_room(self, ws: WebSocket, target_room_id: str) -> bool:
        client = self._clients.get(ws)
        if not client:
            return False

        current_room_id = client.get("room_id")
        username = client["username"]
        room_info = db.get_room_by_id(target_room_id)

        if not room_info:
            await self._send(ws, {"type": "error", "message": "Room not found"})
            return False

        if current_room_id and current_room_id in self._room_members:
            self._room_members[current_room_id].discard(ws)
            if not self._room_members[current_room_id]:
                del self._room_members[current_room_id]
            await self.broadcast_to_room(current_room_id, {
                "type": "user_left",
                "roomId": current_room_id,
                "username": username
            })

        client["room_id"] = target_room_id
        if target_room_id not in self._room_members:
            self._room_members[target_room_id] = set()
        self._room_members[target_room_id].add(ws)

        log.info("User %s switched to room: %s (%s)", username, room_info["name"], target_room_id)

        history = db.get_room_messages(target_room_id)
        room_users = self.get_room_users(target_room_id)

        await self._send(ws, {
            "type": "room_entered",
            "room": room_info,
            "history": history,
            "users": [u for u in room_users if u != username]
        })

        await self.broadcast_to_room(target_room_id, {
            "type": "user_joined",
            "roomId": target_room_id,
            "username": username
        }, exclude=ws)

        return True

    async def disconnect(self, ws: WebSocket) -> None:
        client = self._clients.pop(ws, None)
        if not client:
            return

        room_id = client.get("room_id")
        username = client.get("username")

        if room_id and room_id in self._room_members:
            self._room_members[room_id].discard(ws)
            if not self._room_members[room_id]:
                del self._room_members[room_id]

            if username:
                log.info("User left: %s from room %s", username, room_id)
                await self.broadcast_to_room(room_id, {
                    "type": "user_left",
                    "roomId": room_id,
                    "username": username
                })

    async def handle_message(self, ws: WebSocket, text: str, msg_id: str = None) -> None:
        client = self._clients.get(ws)
        if not client:
            return

        room_id = client["room_id"]
        username = client["username"]
        sender_id = client["id"]
        now = int(time.time() * 1000)

        msg_record = db.save_message(room_id, username, sender_id, text, now, msg_id=msg_id)

        payload = {
            "type": "message",
            "id": msg_record["id"],
            "roomId": room_id,
            "sender": username,
            "senderId": sender_id,
            "text": msg_record["text"],
            "timestamp": now,
            "verified": msg_record["verified"],
            "tampered": msg_record["tampered"],
        }
        await self.broadcast_to_room(room_id, payload)


manager = RoomConnectionManager()


# ── HTTP Routes ───────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "total_clients": len(manager._clients)}


@app.get("/feed")
async def feed():
    """Return latest messages across all rooms. Required assignment endpoint."""
    messages = db.get_all_messages(limit=500)
    return JSONResponse(content={"messages": messages, "count": len(messages)})


@app.post("/message")
async def submit_message(request: Request):
    """Accept a chat message via HTTP POST. Required assignment endpoint.

    Accepts:
      - form data: client-name, msg
      - OR JSON body: {"client-name": "...", "msg": "..."}
      - Optional: msg-id (UUID) for dedup across backends
    """
    content_type = request.headers.get("content-type", "")

    if "application/json" in content_type:
        body = await request.json()
        client_name = str(body.get("client-name", body.get("client_name", "Anonymous"))).strip()[:50]
        msg_text = str(body.get("msg", "")).strip()
        msg_id = body.get("msg-id") or body.get("msg_id") or str(uuid.uuid4())
    else:
        # form-encoded
        form = await request.form()
        client_name = str(form.get("client-name", form.get("client_name", "Anonymous"))).strip()[:50]
        msg_text = str(form.get("msg", "")).strip()
        msg_id = form.get("msg-id") or form.get("msg_id") or str(uuid.uuid4())

    if not client_name:
        client_name = "Anonymous"
    if not msg_text:
        return JSONResponse(status_code=400, content={"error": "msg field is required"})

    sender_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, client_name))[:8]
    now = int(time.time() * 1000)

    # Direct call — thread-local connections + _write_lock in database.py
    # handle concurrency. run_in_executor was bottlenecked by thread pool size.
    msg_record = db.save_message("global", client_name, sender_id, msg_text, now, msg_id=msg_id)

    # Broadcast to any connected WebSocket clients in the global room
    payload = {
        "type": "message",
        "id": msg_record["id"],
        "roomId": "global",
        "sender": client_name,
        "senderId": sender_id,
        "text": msg_record["text"],
        "timestamp": now,
        "verified": msg_record["verified"],
        "tampered": msg_record["tampered"],
    }
    await manager.broadcast_to_room("global", payload)

    return JSONResponse(content={
        "status": "ok",
        "msg-id": msg_record["id"],
        "inserted": msg_record.get("inserted", True),
        "sender": client_name,
        "msg": msg_text,
        "timestamp": now,
    })


# ── WebSocket Endpoint ────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    username: str | None = None

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")

            if msg_type == "join":
                username = str(msg.get("username", "Anonymous")).strip()[:20]
                await manager.register_user(ws, username)

            elif msg_type == "create_room":
                if not username:
                    continue
                room_name = str(msg.get("name", "Private Room")).strip()[:30] or "Private Room"
                new_room = db.create_room(room_name, is_private=True)
                log.info("Room created: %s (code=%s)", new_room["name"], new_room["code"])
                await ws.send_text(json.dumps({"type": "room_created", "room": new_room}))
                await manager.switch_room(ws, new_room["id"])

            elif msg_type == "join_room":
                if not username:
                    continue
                target_code = str(msg.get("code", "")).strip().upper()
                target_id = str(msg.get("roomId", "")).strip()

                room = None
                if target_code:
                    room = db.get_room_by_code(target_code)
                elif target_id:
                    room = db.get_room_by_id(target_id)

                if room:
                    await manager.switch_room(ws, room["id"])
                else:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "message": f"Room code '{target_code}' not found." if target_code else "Room not found."
                    }))

            elif msg_type == "switch_room":
                if not username:
                    continue
                target_id = str(msg.get("roomId", "global")).strip()
                await manager.switch_room(ws, target_id)

            elif msg_type == "message":
                if not username:
                    continue
                text = str(msg.get("text", "")).strip()
                msg_id = msg.get("id") or msg.get("msg_id") or None
                if text:
                    await manager.handle_message(ws, text, msg_id=msg_id)

    except WebSocketDisconnect:
        await manager.disconnect(ws)
