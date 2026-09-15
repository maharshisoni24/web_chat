"""
database.py — SQLite persistence layer for Group Chat
=====================================================
Key changes:
  - messages.msg_id (TEXT PRIMARY KEY) — globally-unique UUID
  - INSERT OR IGNORE prevents duplicate insertions on retry/reconnect
  - get_all_messages() powers the /feed HTTP route
  - WAL mode for better concurrent access
  - DB path still driven by BACKEND_PORT env var
"""

import os
import sqlite3
import random
import string
import time
import uuid
from pathlib import Path
from typing import List, Dict, Optional
try:
    import crypto_utils as crypto
except ImportError:
    from server import crypto_utils as crypto

_port = os.environ.get("BACKEND_PORT", "")
_db_filename = f"chat_{_port}.db" if _port else "chat.db"
DB_PATH = Path(__file__).parent / _db_filename


import threading

# ── Per-thread connection pool ────────────────────────────────────────────────
# Each thread gets its OWN connection via threading.local().
# This avoids "another row available" (cursor shared across threads) and
# eliminates "database is locked" within a process.
# WAL mode allows concurrent readers + one writer across threads/processes.
_thread_local = threading.local()
_write_lock = threading.Lock()   # serialize commits within this process

def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-16384")   # 16MB per thread
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn

def _get_conn() -> sqlite3.Connection:
    if not hasattr(_thread_local, "conn") or _thread_local.conn is None:
        _thread_local.conn = _make_conn()
    return _thread_local.conn

# Compatibility alias
def get_db_connection() -> sqlite3.Connection:
    return _get_conn()


def init_db() -> None:
    """Initialize database tables and ensure default General Room exists."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rooms (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT UNIQUE,
            is_private INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        )
    """)

    # msg_id is globally-unique UUID; PRIMARY KEY prevents duplicate insertions.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            msg_id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            sender TEXT NOT NULL,
            sender_id TEXT NOT NULL,
            ciphertext TEXT NOT NULL,
            nonce TEXT NOT NULL,
            signature TEXT NOT NULL,
            sender_public_key TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_ts
        ON messages (timestamp ASC)
    """)

    cursor.execute("SELECT id FROM rooms WHERE id = 'global'")
    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO rooms (id, name, code, is_private, created_at)
            VALUES ('global', 'General Room', NULL, 0, ?)
        """, (int(time.time() * 1000),))

    conn.commit()


def _generate_room_code(length: int = 6) -> str:
    chars = string.ascii_uppercase + string.digits
    clean_chars = ''.join(c for c in chars if c not in 'O0I1')
    return ''.join(random.choices(clean_chars, k=length))


def create_room(name: str, is_private: bool = True) -> Dict:
    room_id = f"room_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    code = _generate_room_code() if is_private else None
    now = int(time.time() * 1000)

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO rooms (id, name, code, is_private, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (room_id, name.strip(), code, 1 if is_private else 0, now))
        conn.commit()

    return {"id": room_id, "name": name.strip(), "code": code,
            "is_private": is_private, "created_at": now}


def get_room_by_id(room_id: str) -> Optional[Dict]:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM rooms WHERE id = ?", (room_id,))
        row = cursor.fetchone()
        if row:
            return {"id": row["id"], "name": row["name"], "code": row["code"],
                    "is_private": bool(row["is_private"]), "created_at": row["created_at"]}
    return None


def get_room_by_code(code: str) -> Optional[Dict]:
    clean_code = code.strip().upper()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM rooms WHERE UPPER(code) = ?", (clean_code,))
        row = cursor.fetchone()
        if row:
            return {"id": row["id"], "name": row["name"], "code": row["code"],
                    "is_private": bool(row["is_private"]), "created_at": row["created_at"]}
    return None


def get_all_rooms() -> List[Dict]:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM rooms ORDER BY created_at ASC")
        rows = cursor.fetchall()
        return [{"id": r["id"], "name": r["name"], "code": r["code"],
                 "is_private": bool(r["is_private"]), "created_at": r["created_at"]}
                for r in rows]


def save_message(
    room_id: str,
    sender: str,
    sender_id: str,
    text: str,
    timestamp: int,
    msg_id: str = None
) -> Dict:
    """Encrypt + sign + persist. Duplicate msg_id silently ignored (idempotent)."""
    if not msg_id:
        msg_id = str(uuid.uuid4())

    ciphertext, nonce = crypto.encrypt_message(text)
    signature = crypto.sign_message(sender, text)
    public_key = crypto.get_public_key_b64(sender)

    # Each thread has its own conn/cursor — no cursor conflicts.
    # _write_lock serializes commits so WAL only has 1 writer at a time per process.
    conn = _get_conn()
    cursor = conn.cursor()
    with _write_lock:
        cursor.execute("""
            INSERT OR IGNORE INTO messages
                (msg_id, room_id, sender, sender_id, ciphertext, nonce,
                 signature, sender_public_key, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (msg_id, room_id, sender, sender_id, ciphertext, nonce,
              signature, public_key, timestamp))
        conn.commit()
        inserted = cursor.rowcount > 0

    return {
        "id": msg_id,
        "room_id": room_id,
        "sender": sender,
        "senderId": sender_id,
        "text": text,
        "timestamp": timestamp,
        "verified": True,
        "tampered": False,
        "inserted": inserted,
    }


def get_room_messages(room_id: str, limit: int = 100) -> List[Dict]:
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT msg_id, room_id, sender, sender_id,
               ciphertext, nonce, signature, sender_public_key, timestamp
        FROM messages WHERE room_id = ?
        ORDER BY timestamp ASC LIMIT ?
    """, (room_id, limit))
    rows = cursor.fetchall()

    result = []
    for r in rows:
        plaintext = crypto.decrypt_message(r["ciphertext"], r["nonce"])
        tampered = plaintext is None
        verified = (crypto.verify_signature(r["sender_public_key"], plaintext, r["signature"])
                    if not tampered else False)
        result.append({
            "type": "message",
            "id": r["msg_id"],
            "roomId": r["room_id"],
            "sender": r["sender"],
            "senderId": r["sender_id"],
            "text": plaintext if not tampered else "⚠️ [TAMPERED — cannot decrypt]",
            "timestamp": r["timestamp"],
            "verified": verified,
            "tampered": tampered,
        })
    return result


def get_all_messages(limit: int = 1000) -> List[Dict]:
    """Latest messages across all rooms — powers the /feed HTTP route.

    Returns newest `limit` messages ordered oldest-first for display.
    Using DESC + reverse ensures new messages always appear even when DB is full.
    """
    conn = _get_conn()
    cursor = conn.cursor()
    # DESC to get the NEWEST messages, then reverse for chronological display
    cursor.execute("""
        SELECT msg_id, room_id, sender, sender_id,
               ciphertext, nonce, signature, sender_public_key, timestamp
        FROM messages ORDER BY timestamp DESC LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    rows = list(reversed(rows))  # oldest-first for display

    result = []
    for r in rows:
        plaintext = crypto.decrypt_message(r["ciphertext"], r["nonce"])
        tampered = plaintext is None
        verified = (crypto.verify_signature(r["sender_public_key"], plaintext, r["signature"])
                    if not tampered else False)
        result.append({
            "id": r["msg_id"],
            "roomId": r["room_id"],
            "sender": r["sender"],
            "senderId": r["sender_id"],
            "text": plaintext if not tampered else "⚠️ [TAMPERED]",
            "timestamp": r["timestamp"],
            "verified": verified,
            "tampered": tampered,
        })
    return result
