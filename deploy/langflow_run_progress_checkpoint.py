# ruff: noqa: INP001

"""Langflow custom component used by the research flow's progress side-lane.

Each instance is configured with a stage label and order.  The component is
deliberately fail-safe: a FalkorDB outage is returned as checkpoint metadata
and never replaces the primary analysis payload or blocks the next stage.
"""

import contextlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

from langflow.custom import Component
from langflow.io import IntInput, MessageTextInput, Output
from lfx.schema.message import Message


class RunProgressCheckpoint(Component):
    display_name = "Run Progress Checkpoint"
    description = (
        "Writes a monotonic, timestamped run-stage checkpoint to FalkorDB without changing the analysis payload."
    )
    icon = "Activity"
    name = "RunProgressCheckpoint"
    inputs = [
        MessageTextInput(name="input_value", display_name="Workflow Payload", required=True),
        MessageTextInput(name="stage", display_name="Stage", value="Unknown stage", required=False),
        IntInput(name="stage_order", display_name="Stage Order", value=0, required=False),
    ]
    outputs = [Output(display_name="Checkpoint Result", name="output", method="checkpoint")]
    PHASE_PERCENT = {
        10: 1.0,
        20: 3.0,
        30: 8.0,
        45: 18.0,
        55: 62.0,
        60: 70.0,
        70: 76.0,
        75: 80.0,
        80: 84.0,
        85: 89.0,
        95: 97.0,
        90: 94.0,
        100: 100.0,
    }
    FINAL_STAGE_ORDER = 100
    MIN_RESULT_COLUMNS = 2

    @staticmethod
    def _raw(value):
        return str(getattr(value, "text", value) or "")

    @staticmethod
    def _literal(value):
        return json.dumps(str(value or ""), ensure_ascii=False)

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def _graph_name():
        return (
            re.sub(
                r"[^A-Za-z0-9_]",
                "_",
                os.getenv("LANGFLOW_FALKORDB_GRAPH_NAME", "undervalued_stocks_knowledge"),
            )
            or "undervalued_stocks_knowledge"
        )

    @classmethod
    def _client(cls):
        try:
            import redis
        except Exception:  # noqa: BLE001
            return None
        url = os.getenv("LANGFLOW_FALKORDB_URL") or os.getenv("FALKORDB_URL")
        if not url:
            host = os.getenv("FALKORDB_HOST", "langflow-falkordb")
            port = os.getenv("FALKORDB_PORT", "6379")
            database = os.getenv("FALKORDB_DATABASE", "0")
            username = quote(os.getenv("FALKORDB_USERNAME", "default"), safe="")
            password = quote(os.getenv("FALKORDB_PASSWORD", ""), safe="")
            url = f"redis://{username}:{password}@{host}:{port}/{database}"
        client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
            health_check_interval=30,
        )
        client.ping()
        return client

    @classmethod
    def _query(cls, client, query):
        return client.execute_command("GRAPH.QUERY", cls._graph_name(), query)

    @staticmethod
    def _rows(raw):
        if not isinstance(raw, list) or len(raw) < RunProgressCheckpoint.MIN_RESULT_COLUMNS:
            return []
        headers, rows = raw[0], raw[1]
        if isinstance(headers, list) and len(headers) == 1 and isinstance(headers[0], list):
            headers = headers[0]
        if not isinstance(headers, list) or not isinstance(rows, list):
            return []
        if rows and not isinstance(rows[0], (list, tuple)):
            rows = [rows]
        return [
            {str(key): value for key, value in zip(headers, row, strict=False)}
            for row in rows
            if isinstance(row, (list, tuple))
        ]

    @staticmethod
    def _close(client):
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()

    def _run_id(self, payload, raw=""):
        value = str(payload.get("run_id") or "").strip()
        if value:
            return value
        marker = re.search(r"\brun_id\s*:\s*([A-Za-z0-9._-]+)", raw, flags=re.IGNORECASE)
        if marker:
            return marker.group(1)
        graph = getattr(self, "graph", None)
        session_id = str(getattr(graph, "session_id", "") or "").strip()
        return f"research-{session_id or uuid.uuid4().hex[:20]}"

    def checkpoint(self) -> Message:
        raw = self._raw(self.input_value)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"request": raw}
        if not isinstance(payload, dict):
            payload = {"request": str(payload)}

        now = self._now()
        run_id = self._run_id(payload, raw)
        stage = str(getattr(self, "stage", "Unknown stage") or "Unknown stage").strip()
        try:
            stage_order = int(getattr(self, "stage_order", 0) or 0)
        except (TypeError, ValueError):
            stage_order = 0
        stocks = payload.get("stocks") if isinstance(payload.get("stocks"), list) else []
        batches = payload.get("batches") if isinstance(payload.get("batches"), list) else []
        stock_count = sum(len(batch.get("stocks") or []) for batch in batches)
        total_stocks = int(
            payload.get("requested_stock_count") or payload.get("full_universe_count") or len(stocks) or stock_count
        )
        batch_size = int(payload.get("batch_size") or 5)
        calculated_batches = (total_stocks + batch_size - 1) // batch_size if total_stocks else 0
        total_batches = int(payload.get("total_batches") or len(batches) or calculated_batches)
        completed_batches = len(payload.get("batch_elimination") or [])
        if payload.get("batch_iteration_complete") and total_batches:
            completed_batches = total_batches
        completed_stocks = min(total_stocks, completed_batches * batch_size)
        percent = self.PHASE_PERCENT.get(stage_order, 0.0)
        status = "running"
        email_text = self._raw(payload.get("email_status") or raw)
        if stage_order >= self.FINAL_STAGE_ORDER:
            status = "completed_with_email_error" if "email delivery failed" in email_text.lower() else "completed"
        detail = (
            f"{stage}; {completed_batches}/{total_batches or '?'} batches evaluated; "
            f"{completed_stocks}/{total_stocks or '?'} stocks accounted for."
        )
        checkpoint = {
            "run_id": run_id,
            "status": status,
            "stage": stage,
            "stage_order": stage_order,
            "checkpoint_at": now,
            "total_stocks": total_stocks,
            "completed_stocks": completed_stocks,
            "total_batches": total_batches,
            "completed_batches": completed_batches,
            "batch_size": batch_size,
            "percent_complete": percent,
            "last_batch_index": completed_batches or None,
            "detail": detail,
        }
        client = None
        checkpoint["status_write"] = "unavailable"
        try:
            client = self._client()
            if client is None:
                msg = "redis client unavailable"
                raise RuntimeError(msg)
            existing = self._rows(
                self._query(
                    client,
                    "MATCH (r:ResearchRun {run_id: " + self._literal(run_id) + "}) "
                    "RETURN r.stage_order AS stage_order LIMIT 1",
                )
            )
            current_order = -1
            if existing:
                try:
                    current_order = int(float(existing[0].get("stage_order") or -1))
                except (TypeError, ValueError):
                    current_order = -1
            checkpoint_id = f"{run_id}:{stage_order}"
            self._query(
                client,
                "MERGE (c:RunCheckpoint {checkpoint_id: " + self._literal(checkpoint_id) + "}) "
                "SET c.run_id = "
                + self._literal(run_id)
                + ", c.stage = "
                + self._literal(stage)
                + ", c.stage_order = "
                + self._literal(stage_order)
                + ", c.checkpoint_at = "
                + self._literal(now)
                + ", c.detail = "
                + self._literal(detail)
                + ", c.percent_complete = "
                + self._literal(percent),
            )
            if stage_order >= current_order:
                self._query(
                    client,
                    "MERGE (r:ResearchRun {run_id: " + self._literal(run_id) + "}) "
                    "ON CREATE SET r.started_at = "
                    + self._literal(now)
                    + " SET r.status = "
                    + self._literal(status)
                    + ", r.stage = "
                    + self._literal(stage)
                    + ", r.stage_order = "
                    + self._literal(stage_order)
                    + ", r.last_checkpoint_at = "
                    + self._literal(now)
                    + ", r.total_stocks = "
                    + self._literal(total_stocks)
                    + ", r.completed_stocks = "
                    + self._literal(completed_stocks)
                    + ", r.total_batches = "
                    + self._literal(total_batches)
                    + ", r.completed_batches = "
                    + self._literal(completed_batches)
                    + ", r.batch_size = "
                    + self._literal(batch_size)
                    + ", r.percent_complete = "
                    + self._literal(percent)
                    + ", r.last_batch_index = "
                    + self._literal(completed_batches)
                    + ", r.detail = "
                    + self._literal(detail)
                    + ", r.error = "
                    + self._literal("")
                    + (", r.completed_at = " + self._literal(now) if stage_order >= self.FINAL_STAGE_ORDER else ""),
                )
                self._query(
                    client,
                    "MATCH (r:ResearchRun {run_id: " + self._literal(run_id) + "}), "
                    "(c:RunCheckpoint {checkpoint_id: " + self._literal(checkpoint_id) + "}) "
                    "MERGE (r)-[:HAS_CHECKPOINT]->(c)",
                )
            checkpoint["status_write"] = "stored"
        except Exception as exc:  # noqa: BLE001
            checkpoint["error_type"] = type(exc).__name__
        finally:
            self._close(client)

        payload["run_id"] = run_id
        payload["run_progress_checkpoint"] = checkpoint
        return Message(text=json.dumps(payload, ensure_ascii=False))
