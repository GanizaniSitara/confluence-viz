"""SQLite-backed EventStore for MCP session resumability across restarts.

When the MCP server restarts, clients lose their sessions and get 404
"Session not found" errors.  With the default in-memory EventStore the
events are lost too, so clients cannot resume.

This module provides a SQLite-backed EventStore that persists events to
disk.  After a server restart, clients that reconnect with a
Last-Event-ID header can replay missed events and continue their session
without the user having to restart the client or toggle the MCP on/off.

Usage:
    from sqlite_event_store import SQLiteEventStore
    store = SQLiteEventStore("events.db")
    app = mcp.http_app(event_store=store, retry_interval=3000)
"""

import json
import logging
import os
import sqlite3
import time
from typing import Optional

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    StreamId,
)
from mcp.server.streamable_http import EventStore as SDKEventStore
from mcp.types import JSONRPCMessage

logger = logging.getLogger(__name__)


class SQLiteEventStore(SDKEventStore):
    """Persist MCP events to SQLite so they survive server restarts.

    Args:
        db_path: Path to the SQLite database file.
        max_events_per_stream: Maximum events kept per stream (oldest pruned).
        ttl: Event time-to-live in seconds.  Events older than this are
             purged on startup and periodically during writes.
    """

    def __init__(
        self,
        db_path: str = "mcp_events.db",
        max_events_per_stream: int = 200,
        ttl: int = 7200,
    ):
        self._db_path = db_path
        self._max_events_per_stream = max_events_per_stream
        self._ttl = ttl
        self._conn = self._init_db()
        self._purge_expired()

    # ------------------------------------------------------------------
    # Database setup
    # ------------------------------------------------------------------

    def _init_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id   TEXT PRIMARY KEY,
                stream_id  TEXT NOT NULL,
                message    TEXT,
                created_at REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_stream
            ON events (stream_id, created_at)
        """)
        conn.commit()
        logger.info("SQLiteEventStore opened: %s", os.path.abspath(self._db_path))
        return conn

    def _purge_expired(self) -> None:
        """Delete events older than TTL."""
        cutoff = time.time() - self._ttl
        cur = self._conn.execute(
            "DELETE FROM events WHERE created_at < ?", (cutoff,)
        )
        if cur.rowcount:
            logger.info("Purged %d expired events", cur.rowcount)
        self._conn.commit()

    def _trim_stream(self, stream_id: str) -> None:
        """Keep only the newest max_events_per_stream events for a stream."""
        self._conn.execute(
            """
            DELETE FROM events
            WHERE stream_id = ? AND event_id NOT IN (
                SELECT event_id FROM events
                WHERE stream_id = ?
                ORDER BY created_at DESC
                LIMIT ?
            )
            """,
            (stream_id, stream_id, self._max_events_per_stream),
        )

    # ------------------------------------------------------------------
    # SDKEventStore interface
    # ------------------------------------------------------------------

    async def store_event(
        self, stream_id: StreamId, message: JSONRPCMessage | None
    ) -> EventId:
        import uuid

        event_id = uuid.uuid4().hex
        msg_json = (
            json.dumps(message.model_dump(mode="json")) if message else None
        )

        self._conn.execute(
            "INSERT INTO events (event_id, stream_id, message, created_at) VALUES (?, ?, ?, ?)",
            (event_id, stream_id, msg_json, time.time()),
        )
        self._trim_stream(stream_id)
        self._conn.commit()
        return event_id

    async def replay_events_after(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> Optional[StreamId]:
        # Find the stream and timestamp for the last-seen event
        row = self._conn.execute(
            "SELECT stream_id, created_at FROM events WHERE event_id = ?",
            (last_event_id,),
        ).fetchone()
        if row is None:
            logger.warning("Event %s not found — cannot resume", last_event_id)
            return None

        stream_id, created_at = row

        # Replay all events in that stream that came *after* the given one
        rows = self._conn.execute(
            """
            SELECT event_id, message FROM events
            WHERE stream_id = ? AND created_at > ?
            ORDER BY created_at
            """,
            (stream_id, created_at),
        ).fetchall()

        for eid, msg_json in rows:
            if msg_json is not None:
                msg = JSONRPCMessage.model_validate(json.loads(msg_json))
                await send_callback(EventMessage(msg, eid))

        logger.info(
            "Replayed %d event(s) for stream %s after event %s",
            len(rows),
            stream_id,
            last_event_id,
        )
        return stream_id
