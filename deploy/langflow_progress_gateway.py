# ruff: noqa: INP001

"""Real-time run-progress gateway.

Every step of a workflow lands in a Redis stream inside FalkorDB, and this service
fans that stream out over SSE and WebSocket. Two sources feed it:

* the flow components POST domain milestones ("question 7 of 13 answered")
* a background watcher tails Langflow's own durable job events and republishes
  each vertex start/finish, so progress appears without editing any flow

Both SSE and WebSocket emit a heartbeat every 15 seconds. That is not cosmetic:
Cloudflare closes any connection idle for ~100s, and a long LLM step easily
exceeds that, which is exactly how Langflow's own event stream keeps dying.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx
import redis.asyncio as redis
import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

HEARTBEAT_SECONDS = 15
STREAM_MAX_LEN = 20000
RUN_INDEX_KEY = "progress:runs"
FIREHOSE_RUN_ID = "all"
RUN_TTL_SECONDS = 14 * 24 * 3600
BLOCK_MILLISECONDS = 2000
HTTP_OK = 200
HTTP_REDIRECT = 300


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _stream_key(run_id: str) -> str:
    return f"progress:run:{run_id}"


def _flow_run_key(flow_id: str) -> str:
    return f"progress:flowrun:{flow_id}"


def _state_key(run_id: str) -> str:
    return f"progress:state:{run_id}"


def _redis_url() -> str:
    url = os.getenv("PROGRESS_REDIS_URL") or os.getenv("LANGFLOW_FALKORDB_URL") or os.getenv("FALKORDB_URL")
    if url:
        return url
    host = os.getenv("FALKORDB_HOST", "langflow-falkordb")
    port = os.getenv("FALKORDB_PORT", "6379")
    database = os.getenv("FALKORDB_DATABASE", "0")
    username = quote(os.getenv("FALKORDB_USERNAME", "default"), safe="")
    password = quote(os.getenv("FALKORDB_PASSWORD", ""), safe="")
    return f"redis://{username}:{password}@{host}:{port}/{database}"


class ProgressEvent(BaseModel):
    run_id: str = Field(..., min_length=1, max_length=200)
    workflow: str = Field(default="", max_length=120)
    stage: str = Field(default="", max_length=200)
    status: str = Field(default="running", max_length=40)
    step: int | None = None
    total: int | None = None
    percent: float | None = None
    detail: str = Field(default="", max_length=4000)
    source: str = Field(default="component", max_length=40)
    data: dict[str, Any] = Field(default_factory=dict)


class ProgressStore:
    def __init__(self) -> None:
        self._client: redis.Redis | None = None

    async def client(self) -> redis.Redis:
        if self._client is None:
            self._client = redis.from_url(
                _redis_url(),
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                await self._client.aclose()
            self._client = None

    async def publish(self, event: ProgressEvent) -> str:
        client = await self.client()
        # derive percent centrally so every producer reports it the same way
        if event.percent is None and event.step is not None and event.total:
            event.percent = round(100.0 * event.step / event.total, 1)
        payload = event.model_dump()
        payload["data"] = json.dumps(payload.get("data") or {}, ensure_ascii=False)
        payload["emitted_at"] = _now()
        payload = {key: ("" if value is None else str(value)) for key, value in payload.items()}
        stream = _stream_key(event.run_id)
        event_id = await client.xadd(stream, payload, maxlen=STREAM_MAX_LEN, approximate=True)
        await client.expire(stream, RUN_TTL_SECONDS)

        # Mirror into a firehose so one connection can watch everything at once:
        # domain milestones and vertex events otherwise live in separate streams.
        if event.run_id != FIREHOSE_RUN_ID:
            firehose = _stream_key(FIREHOSE_RUN_ID)
            await client.xadd(firehose, payload, maxlen=STREAM_MAX_LEN, approximate=True)
            await client.expire(firehose, RUN_TTL_SECONDS)

        state = {
            "run_id": event.run_id,
            "workflow": event.workflow,
            "stage": event.stage,
            "status": event.status,
            "detail": event.detail[:500],
            "percent": "" if event.percent is None else str(event.percent),
            "step": "" if event.step is None else str(event.step),
            "total": "" if event.total is None else str(event.total),
            "last_event_id": event_id,
            "updated_at": payload["emitted_at"],
        }
        await client.hset(_state_key(event.run_id), mapping=state)
        await client.expire(_state_key(event.run_id), RUN_TTL_SECONDS)
        if event.run_id != FIREHOSE_RUN_ID:
            await client.zadd(RUN_INDEX_KEY, {event.run_id: time.time()})

        # Bind flow -> current run so the vertex watcher can file its events under the
        # same run id a component reports, instead of a separate flow:<id> stream.
        flow_id = str((event.data or {}).get("flow_id") or "")
        if flow_id and event.source == "component" and not event.run_id.startswith("flow:"):
            await client.set(_flow_run_key(flow_id), event.run_id, ex=RUN_TTL_SECONDS)
        return event_id

    async def run_for_flow(self, flow_id: str) -> str | None:
        client = await self.client()
        return await client.get(_flow_run_key(flow_id))

    async def history(self, run_id: str, after_id: str = "0-0", count: int = 500) -> list[tuple[str, dict[str, str]]]:
        client = await self.client()
        start = f"({after_id}" if after_id and after_id != "0-0" else "-"
        rows = await client.xrange(_stream_key(run_id), min=start, max="+", count=count)
        return rows

    async def tail(self, run_id: str, last_id: str):
        """Yield events as they arrive, starting after last_id."""
        client = await self.client()
        cursor = last_id or "$"
        while True:
            rows = await client.xread({_stream_key(run_id): cursor}, count=100, block=BLOCK_MILLISECONDS)
            if not rows:
                yield None  # lets the caller emit a heartbeat
                continue
            for _stream, entries in rows:
                for event_id, fields in entries:
                    cursor = event_id
                    yield event_id, fields

    async def runs(self, limit: int = 50) -> list[dict[str, Any]]:
        client = await self.client()
        ids = await client.zrevrange(RUN_INDEX_KEY, 0, max(0, limit - 1))
        out = []
        for run_id in ids:
            state = await client.hgetall(_state_key(run_id))
            if state:
                out.append(state)
        return out


store = ProgressStore()
app = FastAPI(title="Langflow Progress Gateway", docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def _expected_key() -> str:
    return os.getenv("PROGRESS_API_KEY", "").strip()


def require_key(api_key: str = Header(default=None, alias="x-api-key")) -> bool:
    expected = _expected_key()
    if not expected:
        return True
    if api_key != expected:
        raise HTTPException(status_code=403, detail="Invalid API key.")
    return True


def _decode(event_id: str, fields: dict[str, str]) -> dict[str, Any]:
    out = dict(fields)
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        out["data"] = json.loads(out.get("data") or "{}")
    for key in ("step", "total", "percent"):
        value = out.get(key)
        if value in (None, ""):
            out[key] = None
        else:
            with contextlib.suppress(ValueError):
                out[key] = float(value) if key == "percent" else int(value)
    out["event_id"] = event_id
    return out


@app.get("/health")
async def health() -> JSONResponse:
    try:
        client = await store.client()
        await client.ping()
    except Exception as exc:
        return JSONResponse({"status": "degraded", "error": str(exc)[:200]}, status_code=503)
    return JSONResponse({"status": "ok", "time": _now()})


@app.post("/api/progress")
async def ingest(event: ProgressEvent, _: bool = Depends(require_key)) -> JSONResponse:
    event_id = await store.publish(event)
    return JSONResponse({"status": "recorded", "event_id": event_id, "run_id": event.run_id})


@app.get("/api/runs")
async def list_runs(limit: int = Query(default=50, ge=1, le=200), _: bool = Depends(require_key)) -> JSONResponse:
    return JSONResponse({"runs": await store.runs(limit)})


def _graph_name() -> str:
    import re as _re

    return _re.sub(r"[^A-Za-z0-9_]", "_", os.getenv("FALKORDB_GRAPH_NAME", "undervalued_stocks_knowledge"))


def _rows(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or len(raw) < 2:  # noqa: PLR2004
        return []
    headers, rows = raw[0], raw[1]
    if isinstance(headers, list) and len(headers) == 1 and isinstance(headers[0], list):
        headers = headers[0]
    if not isinstance(headers, list) or not isinstance(rows, list):
        return []
    if rows and not isinstance(rows[0], (list, tuple)):
        rows = [rows]
    return [dict(zip(headers, row, strict=False)) for row in rows if isinstance(row, (list, tuple))]


def _loads(value: Any, default: Any) -> Any:
    try:
        parsed = json.loads(str(value or ""))
    except (json.JSONDecodeError, TypeError):
        return default
    return parsed if isinstance(parsed, type(default)) else default


def _age_minutes(stamp: Any) -> float | None:
    try:
        parsed = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    parsed = parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)
    return round((datetime.now(timezone.utc) - parsed).total_seconds() / 60.0, 1)


class RunRequest(BaseModel):
    flow: str = Field(default="research", max_length=40)
    input_value: str = Field(default="", max_length=2000)


def _flow_ids() -> dict[str, str]:
    return {
        "research": os.getenv("RESEARCH_FLOW_ID", ""),
        "regime": os.getenv("REGIME_FLOW_ID", ""),
    }


def _job_key(flow: str) -> str:
    return f"progress:activejob:{flow}"


@app.post("/api/run")
async def start_run(request: RunRequest, _: bool = Depends(require_key)) -> JSONResponse:
    """Start a flow, cancelling whatever run of it is already going.

    Two long runs of the same flow would fight over the same LiteLLM capacity and both
    degrade, so pressing play replaces the current run rather than racing it.
    """
    flow_id = _flow_ids().get(request.flow, "")
    if not flow_id:
        raise HTTPException(status_code=400, detail=f"Unknown flow '{request.flow}'.")

    base = os.getenv("LANGFLOW_INTERNAL_URL", "http://langflow.langflow.svc.cluster.local:80")
    headers = {"x-api-key": os.getenv("LANGFLOW_API_KEY", ""), "Content-Type": "application/json"}
    client_store = await store.client()
    previous = await client_store.get(_job_key(request.flow))

    stopped = None
    async with httpx.AsyncClient(timeout=45.0) as client:
        if previous:
            try:
                status_response = await client.get(f"{base}/api/v2/workflows", params={"job_id": previous}, headers=headers)
                running = status_response.status_code == HTTP_OK and str(
                    (status_response.json() or {}).get("status")
                ) in {"queued", "in_progress", "running"}
            except Exception:
                running = False
            if running:
                try:
                    stop_response = await client.post(
                        f"{base}/api/v2/workflows/stop", headers=headers, json={"job_id": previous}
                    )
                    stopped = {"job_id": previous, "ok": stop_response.status_code == HTTP_OK}
                except Exception as exc:
                    stopped = {"job_id": previous, "ok": False, "error": str(exc)[:160]}

        try:
            started = await client.post(
                f"{base}/api/v2/workflows",
                headers=headers,
                json={
                    "flow_id": flow_id,
                    "mode": "background",
                    "input_value": request.input_value or f"Run requested from the dashboard ({request.flow}).",
                },
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Could not reach Langflow: {str(exc)[:160]}") from exc

    if started.status_code < HTTP_OK or started.status_code >= HTTP_REDIRECT:
        raise HTTPException(status_code=502, detail=f"Langflow returned {started.status_code}: {started.text[:200]}")

    job_id = str((started.json() or {}).get("job_id") or "")
    if job_id:
        await client_store.set(_job_key(request.flow), job_id, ex=RUN_TTL_SECONDS)

    await store.publish(
        ProgressEvent(
            run_id=f"flow:{flow_id}",
            workflow=request.flow,
            stage="run requested",
            status="running",
            detail=("replaced the previous run" if stopped and stopped.get("ok") else "started from the dashboard"),
            source="dashboard",
            data={"flow_id": flow_id, "job_id": job_id, "stopped": stopped},
        )
    )
    return JSONResponse({"started": True, "flow": request.flow, "job_id": job_id, "stopped": stopped})


@app.get("/api/state")
async def engine_state(_: bool = Depends(require_key)) -> JSONResponse:
    """One small snapshot for dashboards: where the research run is, what it likes, and the regime.

    Deliberately compact — a dashboard should be able to poll this without paying for the
    full event history or the megabyte-scale run payloads.
    """
    client = await store.client()
    graph = _graph_name()

    async def query(statement: str) -> list[dict[str, Any]]:
        try:
            return _rows(await client.execute_command("GRAPH.QUERY", graph, statement))
        except Exception:
            return []

    # Two kinds of ResearchRun node exist: checkpointed progress rows and result rows written
    # at the end. Querying one ordered list picks whichever sorts first and yields nulls, so
    # progress and results are fetched separately.
    run_rows = await query(
        "MATCH (r:ResearchRun) WHERE r.last_checkpoint_at IS NOT NULL "
        "RETURN r.run_id AS run_id, r.status AS status, r.stage AS stage, "
        "r.percent_complete AS percent, r.completed_batches AS completed_batches, "
        "r.total_batches AS total_batches, r.completed_stocks AS completed_stocks, "
        "r.total_stocks AS total_stocks, r.started_at AS started_at, "
        "r.last_checkpoint_at AS last_checkpoint_at, r.completed_at AS completed_at, "
        "r.detail AS detail, r.error AS error "
        "ORDER BY r.last_checkpoint_at DESC LIMIT 1"
    )
    result_rows = await query(
        "MATCH (r:ResearchRun) WHERE r.top_five_json IS NOT NULL "
        "RETURN r.run_id AS run_id, r.completed_at AS completed_at, r.subject AS subject, "
        "r.top_three_json AS top_three_json, r.top_five_json AS top_five_json "
        "ORDER BY r.completed_at DESC LIMIT 1"
    )
    regime_rows = await query(
        "MATCH (m:MacroRegime) RETURN m.run_id AS run_id, m.as_of AS as_of, "
        "m.headline_state AS headline_state, m.overall_confidence AS confidence, "
        "m.liquidity_regime AS liquidity, m.credit_regime AS credit, "
        "m.inflation_regime AS inflation, m.growth_regime AS growth, m.market_regime AS market, "
        "m.confirmed_group_count AS confirmed_groups, "
        "m.recession_warning_supported AS recession_warning, m.horizons_json AS horizons_json "
        "ORDER BY m.as_of DESC LIMIT 1"
    )

    run = run_rows[0] if run_rows else {}
    results = result_rows[0] if result_rows else {}
    picks = []
    for item in _loads(results.get("top_three_json"), [])[:3]:
        if not isinstance(item, dict):
            continue
        stock = item.get("stock") if isinstance(item.get("stock"), dict) else {}
        picks.append(
            {
                "rank": item.get("rank"),
                "ticker": (item.get("ticker") or stock.get("ticker") or "").upper(),
                "company": stock.get("company") or item.get("company") or "",
                "score": item.get("weighted_score"),
                "status": item.get("selection_status"),
            }
        )
    shortlist = []
    for item in _loads(results.get("top_five_json"), [])[:5]:
        if isinstance(item, dict):
            stock = item.get("stock") if isinstance(item.get("stock"), dict) else {}
            ticker = (item.get("ticker") or stock.get("ticker") or "").upper()
            if ticker:
                shortlist.append(ticker)

    regime = regime_rows[0] if regime_rows else {}
    horizons = {}
    for horizon, record in (_loads(regime.get("horizons_json"), {}) or {}).items():
        if not isinstance(record, dict):
            continue
        probabilities = record.get("state_probabilities") or {}
        horizons[horizon] = {
            "state": record.get("most_likely_state"),
            "probability": max(probabilities.values()) if probabilities else None,
            "confidence": (record.get("confidence") or {}).get("band"),
        }

    live = await store.runs(6)
    active = next((r for r in live if str(r.get("status")) not in {"completed", "failed"}), live[0] if live else {})

    return JSONResponse(
        {
            "generated_at": _now(),
            "research_run": {
                "run_id": run.get("run_id"),
                "status": run.get("status"),
                "stage": run.get("stage"),
                "detail": str(run.get("detail") or "")[:280],
                "percent": run.get("percent"),
                "batches": {"completed": run.get("completed_batches"), "total": run.get("total_batches")},
                "stocks": {"completed": run.get("completed_stocks"), "total": run.get("total_stocks")},
                "started_at": run.get("started_at"),
                "last_checkpoint_at": run.get("last_checkpoint_at"),
                "checkpoint_age_minutes": _age_minutes(run.get("last_checkpoint_at")),
                "completed_at": run.get("completed_at"),
                "error": str(run.get("error") or "")[:280] or None,
            },
            "recommendations": picks,
            "shortlist": shortlist,
            "results_from": {
                "run_id": results.get("run_id"),
                "completed_at": results.get("completed_at"),
                "age_minutes": _age_minutes(results.get("completed_at")),
                "subject": str(results.get("subject") or "")[:160] or None,
            },
            "regime": {
                "run_id": regime.get("run_id"),
                "as_of": regime.get("as_of"),
                "age_minutes": _age_minutes(regime.get("as_of")),
                "headline_state": regime.get("headline_state"),
                "confidence": regime.get("confidence"),
                "labels": {
                    "liquidity": regime.get("liquidity"),
                    "credit": regime.get("credit"),
                    "inflation": regime.get("inflation"),
                    "growth": regime.get("growth"),
                    "market": regime.get("market"),
                },
                "confirmed_groups": regime.get("confirmed_groups"),
                "recession_warning_supported": regime.get("recession_warning"),
                "horizons": horizons,
            },
            "live": {
                "run_id": active.get("run_id"),
                "workflow": active.get("workflow"),
                "stage": active.get("stage"),
                "percent": active.get("percent"),
                "status": active.get("status"),
                "updated_at": active.get("updated_at"),
            },
        }
    )


@app.get("/api/runs/{run_id}")
async def run_snapshot(run_id: str, _: bool = Depends(require_key)) -> JSONResponse:
    client = await store.client()
    state = await client.hgetall(_state_key(run_id))
    rows = await store.history(run_id, "0-0", count=1000)
    # the firehose has no state hash of its own, and a run mid-write may not have one yet
    if not state and not rows:
        raise HTTPException(status_code=404, detail="Unknown run id.")
    return JSONResponse({"state": state or {"run_id": run_id}, "events": [_decode(i, f) for i, f in rows]})


@app.get("/api/runs/{run_id}/events")
async def run_events(
    run_id: str,
    request: Request,
    last_event_id: str = Header(default=None, alias="Last-Event-ID"),
    replay: bool = Query(default=True),
    _: bool = Depends(require_key),
) -> StreamingResponse:
    """SSE with replay from Last-Event-ID and a heartbeat that keeps proxies from idling out."""

    async def generator():
        cursor = last_event_id or "0-0"
        if replay:
            for event_id, fields in await store.history(run_id, cursor):
                cursor = event_id
                yield f"id: {event_id}\nevent: progress\ndata: {json.dumps(_decode(event_id, fields), ensure_ascii=False)}\n\n"
        elif not last_event_id:
            # replay=false means live-only; "$" tails from now instead of the stream start
            cursor = "$"
        yield f": replay-complete {_now()}\n\n"
        last_beat = time.monotonic()
        async for item in store.tail(run_id, cursor):
            if await request.is_disconnected():
                break
            if item is None:
                if time.monotonic() - last_beat >= HEARTBEAT_SECONDS:
                    last_beat = time.monotonic()
                    yield f": heartbeat {_now()}\n\n"
                continue
            event_id, fields = item
            last_beat = time.monotonic()
            decoded = _decode(event_id, fields)
            yield f"id: {event_id}\nevent: progress\ndata: {json.dumps(decoded, ensure_ascii=False)}\n\n"
            if decoded.get("status") in {"completed", "failed", "cancelled"} and decoded.get("stage") == "run":
                yield f"event: end\ndata: {json.dumps(decoded, ensure_ascii=False)}\n\n"
                break

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.websocket("/ws/runs/{run_id}")
async def run_websocket(websocket: WebSocket, run_id: str) -> None:
    expected = _expected_key()
    supplied = websocket.query_params.get("api_key") or websocket.headers.get("x-api-key")
    if expected and supplied != expected:
        await websocket.close(code=4403)
        return
    await websocket.accept()
    cursor = websocket.query_params.get("last_event_id") or "0-0"
    try:
        for event_id, fields in await store.history(run_id, cursor):
            cursor = event_id
            await websocket.send_json(_decode(event_id, fields))
        last_beat = time.monotonic()
        async for item in store.tail(run_id, cursor):
            if item is None:
                if time.monotonic() - last_beat >= HEARTBEAT_SECONDS:
                    last_beat = time.monotonic()
                    await websocket.send_json({"type": "heartbeat", "time": _now()})
                continue
            event_id, fields = item
            last_beat = time.monotonic()
            await websocket.send_json(_decode(event_id, fields))
    except WebSocketDisconnect:
        return
    except Exception:
        with contextlib.suppress(Exception):
            await websocket.close(code=1011)


class LangflowWatcher:
    """Republishes Langflow's own vertex events so progress needs no flow edits."""

    def __init__(self) -> None:
        self.base = os.getenv("LANGFLOW_INTERNAL_URL", "http://langflow.langflow.svc.cluster.local:7860")
        self.api_key = os.getenv("LANGFLOW_API_KEY", "")
        self.poll_seconds = int(os.getenv("PROGRESS_POLL_SECONDS", "20"))
        self.seen: dict[str, str] = {}

    async def _transactions(self, client: httpx.AsyncClient, flow_id: str) -> list[dict[str, Any]]:
        response = await client.get(
            f"{self.base}/api/v1/monitor/transactions",
            params={"flow_id": flow_id},
            headers={"x-api-key": self.api_key},
            timeout=30.0,
        )
        if response.status_code < HTTP_OK or response.status_code >= HTTP_REDIRECT:
            return []
        body = response.json()
        return body if isinstance(body, list) else body.get("items") or []

    async def run(self) -> None:
        flow_ids = [f.strip() for f in os.getenv("PROGRESS_WATCH_FLOW_IDS", "").split(",") if f.strip()]
        if not flow_ids:
            return
        async with httpx.AsyncClient() as client:
            while True:
                for flow_id in flow_ids:
                    try:
                        rows = await self._transactions(client, flow_id)
                    except Exception:
                        continue
                    rows = sorted(rows, key=lambda r: str(r.get("timestamp") or ""))
                    for row in rows[-200:]:
                        stamp = str(row.get("timestamp") or "")
                        vertex = str(row.get("vertex_id") or "")
                        marker = f"{flow_id}:{vertex}:{stamp}"
                        if not vertex or self.seen.get(marker):
                            continue
                        self.seen[marker] = stamp
                        bound = await store.run_for_flow(flow_id)
                        await store.publish(
                            ProgressEvent(
                                run_id=bound or f"flow:{flow_id}",
                                workflow=flow_id,
                                stage=vertex,
                                status="completed" if row.get("status") == "success" else str(row.get("status") or ""),
                                detail=str(row.get("error") or "")[:400],
                                source="langflow-vertex",
                                data={"timestamp": stamp, "flow_id": flow_id},
                            )
                        )
                if len(self.seen) > 20000:
                    self.seen = dict(list(self.seen.items())[-5000:])
                await asyncio.sleep(self.poll_seconds)


@app.on_event("startup")
async def _startup() -> None:
    app.state.watcher = asyncio.create_task(LangflowWatcher().run())


@app.on_event("shutdown")
async def _shutdown() -> None:
    task = getattr(app.state, "watcher", None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await store.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PROGRESS_PORT", "8900")), log_level="info")  # noqa: S104
