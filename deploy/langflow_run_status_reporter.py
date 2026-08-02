# ruff: noqa: INP001

"""Send an idempotent 12-hour summary of the stock-analysis run ledger.

This script intentionally uses only the Python standard library.  It talks to
FalkorDB through the Redis RESP protocol so the reporter has no package or
network dependency on the Langflow process that it is observing.
"""

from __future__ import annotations

import html
import json
import logging
import os
import socket
from datetime import datetime, timezone
from typing import Any
from urllib import request

REPORT_INTERVAL_SECONDS = 12 * 60 * 60
STALE_AFTER_SECONDS = 24 * 60 * 60
MIN_RESULT_COLUMNS = 2
HTTP_OK = 200
HTTP_REDIRECT = 300
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.startswith("${"):
        msg = f"{name} is not configured"
        raise RuntimeError(msg)
    return value


def _literal(value: Any) -> str:
    return json.dumps(str(value or ""), ensure_ascii=False)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resp_encode(parts: tuple[Any, ...]) -> bytes:
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = str(part).encode("utf-8")
        encoded.append(f"${len(value)}\r\n".encode())
        encoded.append(value)
        encoded.append(b"\r\n")
    return b"".join(encoded)


class _RedisConnection:
    def __init__(self, host: str, port: int, password: str, username: str, database: int):
        self.socket = socket.create_connection((host, port), timeout=10)
        self.socket.settimeout(20)
        self.reader = self.socket.makefile("rb")
        if username:
            self.command("AUTH", username, password)
        else:
            self.command("AUTH", password)
        if database:
            self.command("SELECT", database)

    def close(self) -> None:
        try:
            self.reader.close()
        finally:
            self.socket.close()

    def command(self, *parts: Any) -> Any:
        self.socket.sendall(_resp_encode(parts))
        return self._read_response()

    def _read_response(self) -> Any:
        prefix = self.reader.read(1)
        if not prefix:
            msg = "FalkorDB closed the connection"
            raise RuntimeError(msg)
        line = self.reader.readline().rstrip(b"\r\n")
        if prefix == b"+":
            return line.decode("utf-8", "replace")
        if prefix == b"-":
            raise RuntimeError(line.decode("utf-8", "replace"))
        if prefix == b":":
            return int(line)
        if prefix == b"$":
            length = int(line)
            if length < 0:
                return None
            value = self.reader.read(length)
            self.reader.read(2)
            return value.decode("utf-8", "replace")
        if prefix == b"*":
            length = int(line)
            if length < 0:
                return None
            return [self._read_response() for _ in range(length)]
        msg = f"Unsupported Redis response: {prefix!r}"
        raise RuntimeError(msg)


def _rows(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or len(raw) < MIN_RESULT_COLUMNS:
        return []
    headers, values = raw[0], raw[1]
    if isinstance(headers, list) and len(headers) == 1 and isinstance(headers[0], list):
        headers = headers[0]
    if not isinstance(headers, list) or not isinstance(values, list):
        return []
    if values and not isinstance(values[0], (list, tuple)):
        values = [values]
    return [
        {str(key): value for key, value in zip(headers, row, strict=False)}
        for row in values
        if isinstance(row, (list, tuple))
    ]


def _graph_name() -> str:
    value = os.getenv("FALKORDB_GRAPH_NAME", "undervalued_stocks_knowledge")
    return "".join(char if char.isalnum() or char == "_" else "_" for char in value) or "undervalued_stocks_knowledge"


def _query(client: _RedisConnection, query: str) -> list[dict[str, Any]]:
    return _rows(client.command("GRAPH.QUERY", _graph_name(), query))


def _run_rows(client: _RedisConnection) -> list[dict[str, Any]]:
    return _query(
        client,
        "MATCH (r:ResearchRun) "
        "RETURN r.run_id AS run_id, r.status AS status, r.stage AS stage, "
        "r.stage_order AS stage_order, r.started_at AS started_at, "
        "r.last_checkpoint_at AS last_checkpoint_at, r.completed_at AS completed_at, "
        "r.total_stocks AS total_stocks, r.completed_stocks AS completed_stocks, "
        "r.total_batches AS total_batches, r.completed_batches AS completed_batches, "
        "r.batch_size AS batch_size, r.percent_complete AS percent_complete, "
        "r.last_batch_index AS last_batch_index, r.detail AS detail, "
        "r.error AS error, r.email_status AS email_status "
        "ORDER BY r.last_checkpoint_at DESC LIMIT 20",
    )


def _already_sent(client: _RedisConnection, slot: str) -> bool:
    return bool(
        _query(
            client,
            "MATCH (e:RunStatusEmail {slot: " + _literal(slot) + "}) RETURN e.sent_at AS sent_at LIMIT 1",
        )
    )


def _mark_sent(client: _RedisConnection, slot: str, sent_at: str, run_id: str) -> None:
    _query(
        client,
        "MERGE (e:RunStatusEmail {slot: "
        + _literal(slot)
        + "}) SET e.sent_at = "
        + _literal(sent_at)
        + ", e.run_id = "
        + _literal(run_id)
        + ", e.interval_hours = 12",
    )


def _status(row: dict[str, Any], now: datetime) -> tuple[str, float | None, str]:
    completed_at = _parse_time(row.get("completed_at"))
    last_checkpoint = _parse_time(row.get("last_checkpoint_at") or row.get("started_at"))
    raw_status = str(row.get("status") or "").lower()
    if completed_at or raw_status in {"completed", "completed_with_email_error"}:
        status = raw_status if raw_status in {"completed", "completed_with_email_error"} else "completed"
    elif last_checkpoint and (now - last_checkpoint).total_seconds() > STALE_AFTER_SECONDS:
        status = "stale"
    elif raw_status in {"running", "stale", "failed"}:
        status = raw_status
    else:
        status = raw_status or "running"
    age_hours = None
    if last_checkpoint:
        age_hours = max(0.0, (now - last_checkpoint).total_seconds() / 3600)
    return status, age_hours, last_checkpoint.isoformat() if last_checkpoint else "unknown"


def _selected_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def is_active(row: dict[str, Any]) -> bool:
        raw_status = str(row.get("status") or "").lower()
        if raw_status in {"running", "stale", "failed"}:
            return True
        if raw_status in {"completed", "completed_with_email_error"}:
            return False
        return not row.get("completed_at")

    active = [row for row in rows if is_active(row)]
    return (active or rows)[:5]


def _summary_html(rows: list[dict[str, Any]], now: datetime, slot: str) -> tuple[str, str]:
    cards: list[str] = []
    for row in rows:
        status, age_hours, checkpoint = _status(row, now)
        total_batches = int(_number(row.get("total_batches"), 0))
        completed_batches = int(_number(row.get("completed_batches"), 0))
        percent = _number(row.get("percent_complete"), 0.0)
        if not percent and total_batches:
            percent = completed_batches / total_batches * 100
        percent = max(0.0, min(100.0, percent))
        run_id = html.escape(str(row.get("run_id") or "unknown"))
        stage = html.escape(str(row.get("stage") or "No checkpoint yet"))
        detail = html.escape(str(row.get("detail") or ""))
        error = html.escape(str(row.get("error") or ""))
        card_status = html.escape(status.upper())
        card_color = "#16a34a" if status == "completed" else "#dc2626" if status in {"stale", "failed"} else "#2563eb"
        error_line = f'<p class="error">{error}</p>' if error else ""
        cards.append(
            '<article class="card">'
            f'<div class="card-head"><strong>{run_id}</strong>'
            f'<span style="color:{card_color}">{card_status}</span></div>'
            f'<p class="stage">{stage}</p>'
            f'<div class="bar"><span style="width:{percent:.1f}%"></span></div>'
            f'<p class="meta">{percent:.1f}% · {completed_batches}/{total_batches or "?"} batches · '
            f"last checkpoint {html.escape(checkpoint)} UTC</p>"
            f'<p class="meta">Checkpoint age: {age_hours:.1f} hours</p>'
            f"<p>{detail}</p>{error_line}"
            "</article>"
        )
    if not cards:
        cards.append('<article class="card"><p>No run checkpoints have been recorded yet.</p></article>')
    title = f"Langflow compute status — {now.date().isoformat()}"
    style = (
        "body{margin:0;background:#eef2f7;color:#172033;font-family:-apple-system,"
        "BlinkMacSystemFont,Segoe UI,Arial,sans-serif}"
        ".wrap{padding:24px 12px}.email{max-width:760px;margin:auto;background:#fff;"
        "border:1px solid #dbe5ef;border-radius:18px;overflow:hidden}.hero{padding:28px 32px;"
        "color:#fff;background:linear-gradient(135deg,#0f172a,#1d4ed8)}h1{margin:0;font-size:25px}"
        ".sub{margin-top:8px;color:#dbeafe;font-size:13px}.content{padding:24px 32px}.card{margin:0 0 14px;"
        "padding:17px;border:1px solid #dbe5ef;border-radius:13px;background:#f8fafc}.card-head{display:flex;"
        "justify-content:space-between;gap:16px;font-size:14px}.stage{margin:10px 0 12px;font-size:16px;"
        "font-weight:700;color:#123d74}.bar{height:9px;background:#dbeafe;border-radius:99px;overflow:hidden}."
        "bar span{display:block;height:100%;background:#2563eb;border-radius:99px}.meta{color:#64748b;"
        "font-size:12px;line-height:1.5}.error{color:#b91c1c;font-size:13px}.footer{padding:18px 32px;"
        "color:#64748b;background:#f8fafc;font-size:11px}"
    )
    body = (
        '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
        "<style>" + style + "</style></head>"
        f'<body><div class="wrap"><main class="email"><header class="hero"><h1>{html.escape(title)}</h1>'
        f'<div class="sub">Durable checkpoint summary · slot {html.escape(slot)} · '
        f'generated {html.escape(now.isoformat())} UTC</div></header><div class="content">'
        + "".join(cards)
        + '</div><footer class="footer">This message reads the independent FalkorDB run ledger. '
        "It does not execute or modify the stock-analysis workflow.</footer></main></div></body></html>"
    )
    return title, body


def _send_email(subject: str, body: str) -> None:
    endpoint = os.getenv("EMAIL_API_URL", "https://email-api.727th.com/api/email/send")
    api_key = _required("EMAIL_API_KEY")
    recipient = _required("EMAIL_RECIPIENT")
    payload = json.dumps(
        {"to": [recipient], "subject": subject, "body": body, "body_type": "html"},
        ensure_ascii=False,
    ).encode("utf-8")
    req = request.Request(  # noqa: S310
        endpoint,
        data=payload,
        headers={"api-key": api_key, "Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=119) as response:  # noqa: S310
        if response.status < HTTP_OK or response.status >= HTTP_REDIRECT:
            msg = f"email API returned HTTP {response.status}"
            raise RuntimeError(msg)


def main() -> int:
    now = _now()
    slot = str(int(now.timestamp()) // REPORT_INTERVAL_SECONDS)
    client = _RedisConnection(
        os.getenv("FALKORDB_HOST", "langflow-falkordb"),
        int(os.getenv("FALKORDB_PORT", "6379")),
        _required("FALKORDB_PASSWORD"),
        os.getenv("FALKORDB_USERNAME", "default").strip(),
        int(os.getenv("FALKORDB_DATABASE", "0")),
    )
    try:
        if _already_sent(client, slot):
            LOGGER.info("status report already sent for slot=%s", slot)
            return 0
        rows = _run_rows(client)
        selected = _selected_rows(rows)
        subject, body = _summary_html(selected, now, slot)
        _send_email(subject, body)
        _mark_sent(client, slot, now.isoformat(), str(selected[0].get("run_id") if selected else "none"))
        LOGGER.info("status report sent for slot=%s; runs=%s", slot, len(selected))
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        LOGGER.exception("status reporter failed")
        raise
