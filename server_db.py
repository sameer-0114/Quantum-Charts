import asyncio
import json
import os
import sys
from typing import Any, Dict, List, Optional
import csv
from io import StringIO

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from db_async import DB, BarsNotifier
from XMR_L2 import LocalOrderBook, SYMBOL as L2_SYMBOL, FSTREAM_BASE, WS_URL, fetch_snapshot, ws_reader  # type: ignore
import aiohttp
import asyncpg


APP_TITLE = "Realtime Liquidity (DB)"
SYMBOL = os.getenv("APP_SYMBOL", "xmrusdt").upper()

app = FastAPI(title=APP_TITLE)
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Windows SelectorEventLoop for aiodns/aiohttp compatibility
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# Serve static if present
WEB_DIR = os.path.join(os.path.dirname(__file__), "web")
if os.path.isdir(WEB_DIR):
    app.mount("/web", StaticFiles(directory=WEB_DIR, html=True), name="web")
    
    @app.get("/")
    async def index_root() -> FileResponse:
        idx = os.path.join(WEB_DIR, "index.html")
        if os.path.isfile(idx):
            return FileResponse(idx)
        # fallback: serve any file listing disabled; return 404 if missing
        return FileResponse(idx)  # will 404 if not present
    
    @app.get("/heatmap")
    async def heatmap_chart() -> FileResponse:
        idx2 = os.path.join(WEB_DIR, "index2.html")
        if os.path.isfile(idx2):
            return FileResponse(idx2)
        return FileResponse(idx2)  # will 404 if not present
    
    # Serve heatmap utility JavaScript files
    @app.get("/heatmap-utils.js")
    async def heatmap_utils() -> FileResponse:
        js_file = os.path.join(WEB_DIR, "heatmap-utils.js")
        if os.path.isfile(js_file):
            return FileResponse(js_file, media_type="application/javascript")
        return FileResponse(js_file)  # will 404 if not present
    
    @app.get("/heatmap-series.js") 
    async def heatmap_series() -> FileResponse:
        js_file = os.path.join(WEB_DIR, "heatmap-series.js")
        if os.path.isfile(js_file):
            return FileResponse(js_file, media_type="application/javascript")
        return FileResponse(js_file)  # will 404 if not present


db = DB()
notifier: Optional[BarsNotifier] = None
reconnect_task: Optional[asyncio.Task] = None
csv_store = None  # type: ignore
csv_notifier = None  # type: ignore
# Timeframe seconds mapping and helper for heatmap bin size (4 bins per candle)
TF_SECONDS: Dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
}

def _bin_sec_for_tf(tf: Optional[str]) -> int:
    tf_key = (tf or "1m").lower()
    base = TF_SECONDS.get(tf_key, 60)
    # Ensure integer division yields desired bin (4 per candle)
    return max(1, base // 4)


# Helper: build DOM snapshots DB DSN
def _dom_dsn() -> str:
    return (
        f"postgresql://{os.getenv('DOM_PGUSER')}:{os.getenv('DOM_PGPASSWORD')}@"
        f"{os.getenv('DOM_PGHOST')}:{os.getenv('DOM_PGPORT')}/{os.getenv('DOM_PGDATABASE')}"
        f"?sslmode={os.getenv('DOM_PGSSLMODE')}"
    )


class HeatmapNotifier:
    def __init__(self) -> None:
        self._conn: Optional[asyncpg.Connection] = None
        self._task: Optional[asyncio.Task] = None
        self._queue: asyncio.Queue = asyncio.Queue()
        # Map client -> (tick_size, bin_sec)
        self._clients: dict[WebSocket, tuple[float, int]] = {}
        self._lock = asyncio.Lock()
        self._stop_evt = asyncio.Event()
        # Per-binSec last-bucket caches
        self._prev_bucket_by_bin: Dict[int, int] = {}
        self._prev_payload_by_bin: Dict[int, dict] = {}

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_evt.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_evt.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
            self._task = None
        if self._conn:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None

    async def add_client(self, ws: WebSocket, tick: float, bin_sec: int) -> None:
        async with self._lock:
            self._clients[ws] = (float(tick), int(bin_sec))
        # kick worker in case idle
        try:
            self._queue.put_nowait("ping")
        except Exception:
            pass

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.pop(ws, None)

    async def _run(self) -> None:
        async def _on_notify(conn, pid, channel, payload):
            try:
                self._queue.put_nowait((channel, payload))
            except Exception:
                pass
        backoff = 1.0
        while not self._stop_evt.is_set():
            try:
                # Ensure connection
                if self._conn is None or self._conn.is_closed():
                    self._conn = await asyncpg.connect(_dom_dsn())
                    await self._conn.add_listener("xmr_snapshots", _on_notify)
                    await self._conn.execute("LISTEN xmr_snapshots;")

                # Coalesce notifications, and also poll occasionally in case NOTIFY missed
                try:
                    await asyncio.wait_for(self._queue.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass

                # If no clients, skip work
                async with self._lock:
                    if not self._clients:
                        continue
                    clients_snapshot = list(self._clients.items())
                    # Group clients by bin_sec and collect ticks per group
                    by_bin: Dict[int, list[tuple[WebSocket, float]]] = {}
                    ticks_by_bin: Dict[int, set[float]] = {}
                    for ws, (tk, bsec) in clients_snapshot:
                        by_bin.setdefault(bsec, []).append((ws, tk))
                        ticks_by_bin.setdefault(bsec, set()).add(tk)

                # Fetch latest snapshot row
                row = await self._conn.fetchrow(
                    """
                    SELECT timestamp, bids, asks
                    FROM xmr_snapshots
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """
                )
                if not row:
                    continue
                ts = float(row["timestamp"]) if row.get("timestamp") is not None else None
                bids_data = row["bids"] or {}
                asks_data = row["asks"] or {}
                if isinstance(bids_data, str):
                    try: bids_data = json.loads(bids_data)
                    except Exception: bids_data = {}
                if isinstance(asks_data, str):
                    try: asks_data = json.loads(asks_data)
                    except Exception: asks_data = {}

                if ts is None:
                    continue

                # Aggregator
                import math
                def _agg_rows(bids_map: dict, asks_map: dict, tick_size: float):
                    EPS = 1e-9
                    buckets: Dict[float, float] = {}
                    for p_str, q_val in bids_map.items():
                        try:
                            p = float(p_str); q = float(q_val)
                        except (ValueError, TypeError):
                            continue
                        if not math.isfinite(p) or not math.isfinite(q):
                            continue
                        idx = math.floor((p / tick_size) + EPS)
                        bucket_low = round(idx * tick_size, 8)
                        buckets[bucket_low] = buckets.get(bucket_low, 0.0) + q
                    for p_str, q_val in asks_map.items():
                        try:
                            p = float(p_str); q = float(q_val)
                        except (ValueError, TypeError):
                            continue
                        if not math.isfinite(p) or not math.isfinite(q):
                            continue
                        idx = math.ceil((p / tick_size) - EPS) - 1
                        bucket_low = round(idx * tick_size, 8)
                        buckets[bucket_low] = buckets.get(bucket_low, 0.0) + q
                    rows = [[bl, float(round(q, 6))] for bl, q in buckets.items() if q > 0]
                    rows.sort(key=lambda x: x[0])
                    return rows

                # For each binSec group, apply last-bucket gating and broadcast only to that group
                stale: list[WebSocket] = []
                for bsec, group_clients in by_bin.items():
                    bucket = int(ts) - (int(ts) % int(bsec))
                    prev_bucket = self._prev_bucket_by_bin.get(bsec)
                    if prev_bucket is None:
                        # initialize cache for this bin
                        self._prev_bucket_by_bin[bsec] = bucket
                        self._prev_payload_by_bin[bsec] = {"ts": ts, "bids": bids_data, "asks": asks_data}
                        continue
                    if bucket == prev_bucket:
                        # update last row cache only
                        self._prev_payload_by_bin[bsec] = {"ts": ts, "bids": bids_data, "asks": asks_data}
                        continue
                    # bucket advanced: broadcast cached snapshot for this bin
                    cache = self._prev_payload_by_bin.get(bsec) or {"ts": ts, "bids": bids_data, "asks": asks_data}
                    send_ts = float(prev_bucket)
                    cached_bids = cache.get("bids") or {}
                    cached_asks = cache.get("asks") or {}

                    # Precompute per-unique tick for this bin's clients
                    rows_by_tick: Dict[float, list] = {}
                    try:
                        for tk in ticks_by_bin.get(bsec, set()):
                            rows_by_tick[tk] = _agg_rows(cached_bids if isinstance(cached_bids, dict) else {}, cached_asks if isinstance(cached_asks, dict) else {}, tk)
                    except Exception:
                        pass
                    # Prepare payloads
                    payloads: Dict[float, str] = {}
                    for tk, rows in rows_by_tick.items():
                        payloads[tk] = json.dumps({"type":"hm", "snap": {"t": int(send_ts), "rows": rows}})
                    # Send only to clients in this bin group
                    for ws, tk in group_clients:
                        try:
                            data = payloads.get(tk)
                            if data:
                                await ws.send_text(data)
                        except Exception:
                            stale.append(ws)
                    # Update caches for this bin
                    self._prev_bucket_by_bin[bsec] = bucket
                    self._prev_payload_by_bin[bsec] = {"ts": ts, "bids": bids_data, "asks": asks_data}

                # Clean up stale
                for ws in stale:
                    await self.remove_client(ws)

                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)


hm_notifier = HeatmapNotifier()


# ================= CSV-backed heatmap support =================
class CSVHeatmapStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self.ready: bool = False
        self.total_bytes: int = 0
        self.loaded_bytes: int = 0
        self.parsed_rows: int = 0
        self.last_timestamp: Optional[float] = None
        self._rows: List[Dict[str, Any]] = []  # {'timestamp': float, 'bids': str, 'asks': str, 'spread': float|None}
        self._timestamps: List[float] = []
        self._load_task: Optional[asyncio.Task] = None
        self._tail_task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self._header: Optional[List[str]] = None
        self._file_offset: int = 0
        self._lock = asyncio.Lock()
        self._on_rows_callbacks: List[Any] = []

    async def start(self) -> None:
        if self._load_task and not self._load_task.done():
            return
        self._stop_evt.clear()
        self._load_task = asyncio.create_task(self._load())

    async def stop(self) -> None:
        self._stop_evt.set()
        for t in [self._tail_task, self._load_task]:
            if t:
                try:
                    t.cancel()
                    await t
                except Exception:
                    pass
        self._tail_task = None
        self._load_task = None

    def on_rows(self, cb) -> None:
        self._on_rows_callbacks.append(cb)

    async def _load(self) -> None:
        try:
            self.total_bytes = os.path.getsize(self.path)
        except Exception:
            self.total_bytes = 0
        # Load in a thread to avoid blocking event loop
        await asyncio.to_thread(self._blocking_load)
        self.ready = True
        # Start tailing after initial load
        self._tail_task = asyncio.create_task(self._tail_loop())

    def _blocking_load(self) -> None:
        try:
            with open(self.path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return
                self._header = header
                # Use DictReader for robust quoting/embedded commas
                f.seek(0)
                dict_reader = csv.DictReader(f)
                for row in dict_reader:
                    try:
                        ts = float(row.get('timestamp') or 0)
                    except Exception:
                        continue
                    bids = row.get('bids(json)') if 'bids(json)' in row else row.get('bids')
                    asks = row.get('asks(json)') if 'asks(json)' in row else row.get('asks')
                    spread_val = row.get('spread')
                    try:
                        spread = float(spread_val) if spread_val not in (None, '') else None
                    except Exception:
                        spread = None
                    if bids is None or asks is None:
                        continue
                    self._rows.append({'timestamp': ts, 'bids': bids, 'asks': asks, 'spread': spread})
                    self._timestamps.append(ts)
                    self.parsed_rows += 1
                    self.last_timestamp = ts
                self._file_offset = f.tell()
                self.loaded_bytes = self._file_offset
        except Exception:
            # leave ready False on failure
            pass

    async def _tail_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                await asyncio.sleep(1.0)
                try:
                    size = os.path.getsize(self.path)
                except Exception:
                    continue
                if size <= self._file_offset:
                    continue
                with open(self.path, 'r', newline='', encoding='utf-8') as f:
                    f.seek(self._file_offset)
                    chunk = f.read()
                    self._file_offset = f.tell()
                    self.loaded_bytes = self._file_offset
                if not chunk:
                    continue
                sio = StringIO()
                if self._header:
                    sio.write(','.join(self._header) + '\n')
                sio.write(chunk)
                sio.seek(0)
                dict_reader = csv.DictReader(sio)
                new_rows: List[Dict[str, Any]] = []
                for row in dict_reader:
                    try:
                        ts = float(row.get('timestamp') or 0)
                    except Exception:
                        continue
                    bids = row.get('bids(json)') if 'bids(json)' in row else row.get('bids')
                    asks = row.get('asks(json)') if 'asks(json)' in row else row.get('asks')
                    spread_val = row.get('spread')
                    try:
                        spread = float(spread_val) if spread_val not in (None, '') else None
                    except Exception:
                        spread = None
                    if bids is None or asks is None:
                        continue
                    r = {'timestamp': ts, 'bids': bids, 'asks': asks, 'spread': spread}
                    new_rows.append(r)
                if not new_rows:
                    continue
                async with self._lock:
                    for r in new_rows:
                        self._rows.append(r)
                        self._timestamps.append(r['timestamp'])
                        self.parsed_rows += 1
                        self.last_timestamp = r['timestamp']
                # Notify listeners
                for cb in self._on_rows_callbacks:
                    try:
                        cb(new_rows)
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)

    def _aggregate_to_rows(self, bids_json: str, asks_json: str, tick: float) -> List[List[float]]:
        try:
            bids_map = json.loads(bids_json) if bids_json else {}
        except Exception:
            bids_map = {}
        try:
            asks_map = json.loads(asks_json) if asks_json else {}
        except Exception:
            asks_map = {}
        import math
        EPS = 1e-9
        buckets: Dict[float, float] = {}
        for p_str, q_val in bids_map.items():
            try:
                p = float(p_str); q = float(q_val)
            except Exception:
                continue
            if not math.isfinite(p) or not math.isfinite(q):
                continue
            idx = math.floor((p / tick) + EPS)
            bucket_low = round(idx * tick, 8)
            buckets[bucket_low] = buckets.get(bucket_low, 0.0) + q
        for p_str, q_val in asks_map.items():
            try:
                p = float(p_str); q = float(q_val)
            except Exception:
                continue
            if not math.isfinite(p) or not math.isfinite(q):
                continue
            idx = math.ceil((p / tick) - EPS) - 1
            bucket_low = round(idx * tick, 8)
            buckets[bucket_low] = buckets.get(bucket_low, 0.0) + q
        rows = [[bl, float(round(q, 6))] for bl, q in buckets.items() if q > 0]
        rows.sort(key=lambda x: x[0])
        return rows

    async def get_range(self, start: Optional[float], end: Optional[float], limit: int, fmt: str, tick: float) -> Dict[str, Any]:
        import bisect
        limit = max(1, min(5000, int(limit)))
        if not self._timestamps:
            return {"data": [], "count": 0, "tick_size": tick, "format": ('rows' if fmt=='rows' else 'raw')}
        idx_end = len(self._timestamps) - 1
        if end is not None:
            idx_end = bisect.bisect_right(self._timestamps, end) - 1
            idx_end = max(-1, min(idx_end, len(self._timestamps)-1))
        idx = idx_end
        data: List[Dict[str, Any]] = []
        n = 0
        while idx >= 0 and n < limit:
            ts = self._timestamps[idx]
            if start is not None and ts < start:
                break
            r = self._rows[idx]
            if fmt == 'rows':
                rows_fmt = self._aggregate_to_rows(r['bids'], r['asks'], tick)
                data.append({'t': int(float(ts)), 'rows': rows_fmt})
            else:
                bids: Dict[float, float] = {}
                asks: Dict[float, float] = {}
                try:
                    bmap = json.loads(r['bids']) if r['bids'] else {}
                except Exception:
                    bmap = {}
                try:
                    amap = json.loads(r['asks']) if r['asks'] else {}
                except Exception:
                    amap = {}
                for k, v in bmap.items():
                    try: bids[float(k)] = float(v)
                    except Exception: pass
                for k, v in amap.items():
                    try: asks[float(k)] = float(v)
                    except Exception: pass
                data.append({'timestamp': float(ts), 'bids': bids, 'asks': asks, 'tick_size': tick})
            n += 1
            idx -= 1
        return {"data": data, "count": len(data), "tick_size": tick, "format": ('rows' if fmt=='rows' else 'raw')}


class CSVHeatmapNotifier:
    def __init__(self, store: CSVHeatmapStore) -> None:
        self.store = store
        # Map client -> (tick_size, bin_sec)
        self.clients: Dict[WebSocket, tuple[float, int]] = {}
        self._lock = asyncio.Lock()
        self.store.on_rows(self._on_rows_appended)
        self._stopped = False
        # Per-binSec last-bucket caches
        self._prev_bucket_by_bin: Dict[int, int] = {}
        self._prev_row_by_bin: Dict[int, Dict[str, Any]] = {}

    async def stop(self) -> None:
        self._stopped = True

    async def add_client(self, ws: WebSocket, tick: float, bin_sec: int) -> None:
        async with self._lock:
            self.clients[ws] = (float(tick), int(bin_sec))

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._lock:
            self.clients.pop(ws, None)

    def _on_rows_appended(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        last = rows[-1]
        ts = float(last.get('timestamp') or 0)
        # Snapshot client groups
        try:
            by_bin: Dict[int, list[tuple[WebSocket, float]]] = {}
            for ws, (tk, bsec) in list(self.clients.items()):
                by_bin.setdefault(int(bsec), []).append((ws, float(tk)))
        except Exception:
            by_bin = {}

        async def _broadcast_group(bin_sec: int, prev_row: Dict[str, Any], send_ts_bucket: int, group: list[tuple[WebSocket, float]]):
            payloads: Dict[float, str] = {}
            try:
                ticks = sorted(set(tk for (_ws, tk) in group))
            except Exception:
                ticks = []
            for t in ticks:
                rows_fmt = self.store._aggregate_to_rows(prev_row.get('bids') or '{}', prev_row.get('asks') or '{}', t)
                payloads[t] = json.dumps({"type": "hm", "snap": {"t": int(send_ts_bucket), "rows": rows_fmt}})
            await self._broadcast(payloads, bin_sec)

        # For each bin group, gate and broadcast
        for bin_sec, group in by_bin.items():
            bucket = int(ts) - (int(ts) % int(bin_sec))
            prev_bucket = self._prev_bucket_by_bin.get(bin_sec)
            if prev_bucket is None:
                self._prev_bucket_by_bin[bin_sec] = bucket
                self._prev_row_by_bin[bin_sec] = last
                continue
            if bucket == prev_bucket:
                self._prev_row_by_bin[bin_sec] = last
                continue
            # bucket advanced: broadcast cached row for this bin group
            prev_row = self._prev_row_by_bin.get(bin_sec, last)
            send_bucket = int(prev_bucket)
            asyncio.create_task(_broadcast_group(bin_sec, prev_row, send_bucket, group))
            # update caches
            self._prev_bucket_by_bin[bin_sec] = bucket
            self._prev_row_by_bin[bin_sec] = last

    async def _broadcast(self, payloads: Dict[float, str], bin_sec: Optional[int] = None) -> None:
        stale: List[WebSocket] = []
        async with self._lock:
            for ws, (t, bsec) in list(self.clients.items()):
                if bin_sec is not None and int(bsec) != int(bin_sec):
                    continue
                try:
                    data = payloads.get(t)
                    if data:
                        await ws.send_text(data)
                except Exception:
                    stale.append(ws)
            for ws in stale:
                self.clients.pop(ws, None)


@app.on_event("startup")
async def on_start() -> None:
    """Initialize DB and notifier without failing app startup if DB is down.

    On failure, schedule a background reconnect loop and keep the app serving / and /health.
    """
    global notifier, reconnect_task
    try:
        await db.init()
        notifier = BarsNotifier(db)
        await notifier.start(["1m", "3m", "5m", "15m"])
        # Start heatmap notifier (LISTEN/NOTIFY)
        await hm_notifier.start()
        print("✅ DB connected and notifier started")
        print("📊 Main Chart: http://127.0.0.1:8000")
        print("🔥 Heatmap Chart: http://127.0.0.1:8000/heatmap")
    except Exception as e:
        notifier = None
        print(f"DB init failed at startup: {e}")

        async def _reconnect_loop():
            backoff = 3.0
            while True:
                try:
                    await db.init()
                    _n = BarsNotifier(db)
                    await _n.start(["1m", "3m", "5m", "15m"])
                    # promote
                    global notifier
                    notifier = _n
                    print("DB reconnected; notifier started")
                    return
                except Exception as e:
                    print(f"DB reconnect failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)

        reconnect_task = asyncio.create_task(_reconnect_loop())


@app.on_event("shutdown")
async def on_stop() -> None:
    global notifier, reconnect_task, csv_store, csv_notifier
    if notifier:
        await notifier.stop()
        notifier = None
    await hm_notifier.stop()
    if csv_notifier:
        try:
            await csv_notifier.stop()
        except Exception:
            pass
        csv_notifier = None
    if csv_store:
        try:
            await csv_store.stop()
        except Exception:
            pass
        csv_store = None
    if reconnect_task:
        try:
            reconnect_task.cancel()
            await reconnect_task
        except Exception:
            pass
        reconnect_task = None
    await db.close()


@app.get("/api/bars")
async def api_bars(tf: str = "1m", limit: int = 1500):
    limit = max(10, min(5000, limit))
    if db.pool is None:
        return JSONResponse({"error": "db not connected"}, status_code=503)
    bars = await db.fetch_last_bars(tf, limit)
    return JSONResponse(bars)


@app.get("/api/footprint")
async def api_footprint(tf: str = "1m", limit: int = 300):
    if db.pool is None:
        return JSONResponse({"error": "db not connected"}, status_code=503)
    
    # Add retry logic for database connection issues
    max_retries = 3
    for attempt in range(max_retries):
        try:
            cols = await db.fetch_footprint_cols(tf, limit)
            return JSONResponse(cols)
        except Exception as e:
            print(f"Footprint fetch attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5)  # Wait 500ms before retry
            else:
                # Last attempt failed, return empty data structure to prevent frontend errors
                print(f"All footprint fetch attempts failed. Returning empty data.")
                return JSONResponse([])  # Return empty array instead of error


@app.get("/api/delta")
async def api_delta(tf: str = "1m", limit: int = 300):
    """Fetch delta columns computed from price_levels (buy_volume - sell_volume)."""
    if db.pool is None:
        return JSONResponse({"error": "db not connected"}, status_code=503)
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            cols = await db.fetch_delta_cols(tf, limit)
            return JSONResponse(cols)
        except Exception as e:
            print(f"Delta fetch attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5)
            else:
                print(f"All delta fetch attempts failed. Returning empty data.")
                return JSONResponse([])


@app.get("/api/heatmap-data")
async def api_heatmap_data(
    start: float = None,
    end: float = None, 
    tick: float = 0.10,
    limit: int = 500,
    format: str = "raw",
    source: str = "db",
    tf: str = "1m",
    mode: str = "auto",
    tol: float = 5.0,
    targets: str = "",
):
    """
    Fetch DOM snapshot data for heatmap visualization.
    
    Args:
        start: Start timestamp (Unix timestamp)
        end: End timestamp (Unix timestamp)  
        tick: Tick size for aggregation (0.01, 0.05, 0.10, 0.20, 0.25, 0.50, 1.00)
        limit: Maximum number of snapshots to return
        format: 'raw' to return bids/asks maps, 'rows' to return frontend-ready {t, rows}
    """
    # Validate tick size
    valid_ticks = [0.01, 0.05, 0.10, 0.20, 0.25, 0.50, 1.00]
    if tick not in valid_ticks:
        return JSONResponse({"error": f"Invalid tick size. Must be one of: {valid_ticks}"}, status_code=400)
    
    limit = max(10, min(2000, limit))  # Reasonable limits for heatmap data
    bin_sec = _bin_sec_for_tf(tf)

    # Parse targets if provided as comma-separated list
    targets_list: Optional[list[float]] = None
    if targets:
        try:
            targets_list = [float(x) for x in targets.split(',') if x.strip()]
        except Exception:
            targets_list = None
    
    # Branch by source (db or csv)
    if source == "csv":
        global csv_store
        if not csv_store:
            # Auto-start CSV store lazily if env var provided
            csv_path = os.getenv("HEATMAP_CSV_PATH")
            if not csv_path:
                return JSONResponse({"error": "CSV source requested but HEATMAP_CSV_PATH not set"}, status_code=400)
            csv_store = CSVHeatmapStore(csv_path)
            await csv_store.start()
        # If not ready yet, return progress info (empty data with status)
        if not csv_store.ready:
            return JSONResponse({
                "data": [],
                "count": 0,
                "tick_size": tick,
                "format": ('rows' if format == 'rows' else 'raw'),
                "status": {
                    "ready": False,
                    "loaded_bytes": csv_store.loaded_bytes,
                    "total_bytes": csv_store.total_bytes,
                    "parsed_rows": csv_store.parsed_rows,
                    "last_timestamp": csv_store.last_timestamp,
                }
            })
        # Targets mode for CSV: pick nearest row within tolerance for each target
        if (mode == 'targets' or targets_list) and format == 'rows' and targets_list:
            import bisect
            data_out: list[dict[str, Any]] = []
            ts_list = csv_store._timestamps
            rows_list = csv_store._rows
            for tgt in targets_list:
                # find nearest index
                i = bisect.bisect_left(ts_list, tgt)
                candidates = []
                if i < len(ts_list):
                    candidates.append(i)
                if i-1 >= 0:
                    candidates.append(i-1)
                best_idx = None
                best_dist = 1e18
                for idx in candidates:
                    dist = abs(float(ts_list[idx]) - float(tgt))
                    if dist < best_dist:
                        best_dist = dist
                        best_idx = idx
                if best_idx is None or best_dist > float(tol):
                    continue
                r = rows_list[best_idx]
                rows_fmt = csv_store._aggregate_to_rows(r.get('bids') or '{}', r.get('asks') or '{}', tick)
                # Force t to target anchor (bin-aligned on client side)
                data_out.append({
                    't': int(float(tgt)),
                    'rows': rows_fmt,
                })
            res = {
                'data': data_out,
                'count': len(data_out),
                'tick_size': tick,
                'format': 'rows',
            }
        else:
            # Ready: perform range query on in-memory data
            res = await csv_store.get_range(start, end, limit, format, tick)
        # Downsample to tf-dependent bins when format=rows (last-bucket policy)
        if format == 'rows' and isinstance(res, dict) and isinstance(res.get('data'), list):
            # Skip additional bucketing for targets mode (already one per target)
            if not (mode == 'targets' and targets_list):
                bin_sec = _bin_sec_for_tf(tf)
            else:
                bin_sec = bin_sec
            bucketed: Dict[int, Dict[str, Any]] = {}
            for snap in res['data']:
                try:
                    t = int(snap.get('t') or 0)
                except Exception:
                    continue
                b = t - (t % bin_sec)
                # Always keep the last occurrence (res['data'] comes newest-first)
                if b not in bucketed:
                    bucketed[b] = {'t': int(b), 'rows': snap.get('rows') or []}
            # Preserve original order as best as possible
            new_list = list(bucketed.values())
            res['data'] = new_list
            res['count'] = len(new_list)
        res["status"] = {
            "ready": True,
            "loaded_bytes": csv_store.loaded_bytes,
            "total_bytes": csv_store.total_bytes,
            "parsed_rows": csv_store.parsed_rows,
            "last_timestamp": csv_store.last_timestamp,
        }
        return JSONResponse(res)

    # Default DB path
    try:
        dom_dsn = f"postgresql://{os.getenv('DOM_PGUSER')}:{os.getenv('DOM_PGPASSWORD')}@{os.getenv('DOM_PGHOST')}:{os.getenv('DOM_PGPORT')}/{os.getenv('DOM_PGDATABASE')}?sslmode={os.getenv('DOM_PGSSLMODE')}"
        conn = await asyncpg.connect(dom_dsn)
        try:
            # DB: targets mode
            if (mode == 'targets' or targets_list) and format == 'rows' and targets_list:
                # Use nearest-within-tolerance per target
                sql = """
                WITH t AS (
                    SELECT unnest($1::double precision[]) AS target
                ), c AS (
                    SELECT t.target, s.timestamp, s.bids, s.asks,
                           ABS(s.timestamp - t.target) AS dist
                    FROM t
                    JOIN xmr_snapshots s
                      ON s.timestamp BETWEEN t.target - $2 AND t.target + $2
                ), picked AS (
                    SELECT DISTINCT ON (target) target, timestamp, bids, asks
                    FROM c
                    ORDER BY target, dist ASC, timestamp DESC
                )
                SELECT target, timestamp, bids, asks
                FROM picked
                ORDER BY target DESC
                """
                rows = await conn.fetch(sql, targets_list, float(tol))
                snapshots: list[dict[str, Any]] = []
                for row in rows:
                    t_anchor = float(row['target'])
                    bids_data = row['bids'] or {}
                    asks_data = row['asks'] or {}
                    if isinstance(bids_data, str):
                        try: bids_data = json.loads(bids_data)
                        except Exception: bids_data = {}
                    if isinstance(asks_data, str):
                        try: asks_data = json.loads(asks_data)
                        except Exception: asks_data = {}
                    # Aggregate into rows
                    def _aggregate_to_rows(bids_map: dict, asks_map: dict, tick_size: float):
                        import math
                        EPS = 1e-9
                        buckets: Dict[float, float] = {}
                        for p_str, q_val in bids_map.items():
                            try:
                                p = float(p_str); q = float(q_val)
                            except (ValueError, TypeError):
                                continue
                            if not math.isfinite(p) or not math.isfinite(q):
                                continue
                            idx = math.floor((p / tick_size) + EPS)
                            bucket_low = round(idx * tick_size, 8)
                            buckets[bucket_low] = buckets.get(bucket_low, 0.0) + q
                        for p_str, q_val in asks_map.items():
                            try:
                                p = float(p_str); q = float(q_val)
                            except (ValueError, TypeError):
                                continue
                            if not math.isfinite(p) or not math.isfinite(q):
                                continue
                            idx = math.ceil((p / tick_size) - EPS) - 1
                            bucket_low = round(idx * tick_size, 8)
                            buckets[bucket_low] = buckets.get(bucket_low, 0.0) + q
                        rws = [[bl, float(round(q, 6))] for bl, q in buckets.items() if q > 0]
                        rws.sort(key=lambda x: x[0])
                        return rws
                    rows_fmt = _aggregate_to_rows(bids_data if isinstance(bids_data, dict) else {}, asks_data if isinstance(asks_data, dict) else {}, tick)
                    snapshots.append({'t': int(t_anchor), 'rows': rows_fmt})
                return JSONResponse({
                    'data': snapshots,
                    'count': len(snapshots),
                    'tick_size': tick,
                    'format': 'rows',
                    'time_range': { 'start': start, 'end': end, 'requested_limit': limit },
                })
            if start is not None and end is not None:
                query = """
                SELECT timestamp, bids, asks
                FROM xmr_snapshots 
                WHERE timestamp BETWEEN $1 AND $2
                ORDER BY timestamp DESC
                LIMIT $3
                """
                rows = await conn.fetch(query, start, end, limit)
            elif end is not None:
                query = """
                SELECT timestamp, bids, asks
                FROM xmr_snapshots
                WHERE timestamp <= $1
                ORDER BY timestamp DESC
                LIMIT $2
                """
                rows = await conn.fetch(query, end, limit)
            elif start is not None:
                query = """
                SELECT timestamp, bids, asks
                FROM xmr_snapshots
                WHERE timestamp >= $1
                ORDER BY timestamp ASC
                LIMIT $2
                """
                rows = await conn.fetch(query, start, limit)
            else:
                query = """
                SELECT timestamp, bids, asks
                FROM xmr_snapshots 
                ORDER BY timestamp DESC
                LIMIT $1
                """
                rows = await conn.fetch(query, limit)

            # Convert to list for JSON response, optionally aggregating into frontend rows
            snapshots = []
            for row in rows:
                # Handle JSONB data - it might be string or dict depending on driver
                bids_data = row['bids'] if row['bids'] else {}
                asks_data = row['asks'] if row['asks'] else {}
                
                # Parse as JSON if it's a string
                if isinstance(bids_data, str):
                    try:
                        bids_data = json.loads(bids_data)
                    except (json.JSONDecodeError, TypeError):
                        bids_data = {}
                        
                if isinstance(asks_data, str):
                    try:
                        asks_data = json.loads(asks_data)
                    except (json.JSONDecodeError, TypeError):
                        asks_data = {}
                
                # Helper to aggregate into frontend format rows [[bucketLow, totalQty], ...]
                def _aggregate_to_rows(bids_map: dict, asks_map: dict, tick_size: float):
                    import math
                    EPS = 1e-9
                    buckets: Dict[float, float] = {}
                    # Bids: bucketLow = floor(p/tick)*tick
                    for p_str, q_val in bids_map.items():
                        try:
                            p = float(p_str); q = float(q_val)
                        except (ValueError, TypeError):
                            continue
                        if not math.isfinite(p) or not math.isfinite(q):
                            continue
                        idx = math.floor((p / tick_size) + EPS)
                        bucket_low = round(idx * tick_size, 8)
                        buckets[bucket_low] = buckets.get(bucket_low, 0.0) + q
                    # Asks: bucketLow = ceil(p/tick)*tick - tick
                    for p_str, q_val in asks_map.items():
                        try:
                            p = float(p_str); q = float(q_val)
                        except (ValueError, TypeError):
                            continue
                        if not math.isfinite(p) or not math.isfinite(q):
                            continue
                        idx = math.ceil((p / tick_size) - EPS) - 1
                        bucket_low = round(idx * tick_size, 8)
                        buckets[bucket_low] = buckets.get(bucket_low, 0.0) + q
                    rows = [[bl, float(round(q, 6))] for bl, q in buckets.items() if q > 0]
                    rows.sort(key=lambda x: x[0])
                    return rows

                # If format=='rows' build frontend-ready snapshot; else emit raw maps converted to floats
                ts = float(row['timestamp'])
                if format == "rows":
                    bids_map = bids_data if isinstance(bids_data, dict) else {}
                    asks_map = asks_data if isinstance(asks_data, dict) else {}
                    rows_fmt = _aggregate_to_rows(bids_map, asks_map, tick)
                    snapshots.append({
                        't': int(ts),
                        'rows': rows_fmt,
                    })
                else:
                    # Convert string keys to float prices and values to volumes
                    bids: Dict[float, float] = {}
                    if bids_data:
                        for price_str, volume in bids_data.items():
                            try:
                                bids[float(price_str)] = float(volume)
                            except (ValueError, TypeError):
                                continue
                    asks: Dict[float, float] = {}
                    if asks_data:
                        for price_str, volume in asks_data.items():
                            try:
                                asks[float(price_str)] = float(volume)
                            except (ValueError, TypeError):
                                continue
                    snapshots.append({
                        'timestamp': ts,
                        'bids': bids,
                        'asks': asks,
                        'tick_size': tick  # Include requested tick size
                    })
            
            # Downsample to tf-dependent bins when format=rows (last-bucket policy)
            if format == 'rows':
                bin_sec = _bin_sec_for_tf(tf)
                bucketed: Dict[int, Dict[str, Any]] = {}
                for snap in snapshots:
                    try:
                        t = int(snap.get('t') or 0)
                    except Exception:
                        continue
                    b = t - (t % bin_sec)
                    if b not in bucketed:
                        bucketed[b] = {'t': int(b), 'rows': snap.get('rows') or []}
                snapshots = list(bucketed.values())

            return JSONResponse({
                'data': snapshots,
                'count': len(snapshots),
                'tick_size': tick,
                'format': ('rows' if format == 'rows' else 'raw'),
                'time_range': {
                    'start': start,
                    'end': end,
                    'requested_limit': limit
                }
            })
            
        finally:
            await conn.close()
            
    except Exception as e:
        print(f"Heatmap data fetch error: {str(e)}")
        return JSONResponse({
            "error": "Failed to fetch heatmap data", 
            "details": str(e)
        }, status_code=500)


class BarsBroadcaster:
    def __init__(self, tf: str) -> None:
        self.tf = tf
        self.clients: List[WebSocket] = []
        self.task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._last_sent_time: Optional[int] = None
        self._last_sent_close: Optional[float] = None
        self._last_sent_vol: Optional[float] = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except Exception:
                pass
            self.task = None

    async def _run(self) -> None:
        q: Optional[asyncio.Queue] = None
        while not self._stop.is_set():
            try:
                # Wait for notifier/queue to be available
                global notifier
                if notifier is None:
                    await asyncio.sleep(0.5)
                    continue
                if q is None:
                    q = notifier.queue_for(self.tf)
                # Wait for a signal or timeout to do a periodic reconcile
                try:
                    await asyncio.wait_for(q.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
                # Coalesce fast successive signals
                await asyncio.sleep(0.08)
                while q is not None and not q.empty():
                    try:
                        q.get_nowait()
                    except Exception:
                        break
                # Skip until DB connected
                if db.pool is None:
                    await asyncio.sleep(0.5)
                    continue
                # Fetch latest 1-2 bars to compute delta
                tail = await db.fetch_tail_bars(self.tf, 2)
                if not tail:
                    continue
                latest = tail[0]
                t = int(latest["time"]) if isinstance(latest["time"], (int, float)) else latest["time"]
                c = float(latest["close"]) if latest.get("close") is not None else None
                v = float(latest.get("volume", 0) or 0)

                changed = False
                if self._last_sent_time != t:
                    changed = True
                elif (self._last_sent_close is None or c is None) or (self._last_sent_close != c) or (self._last_sent_vol != v):
                    changed = True

                if changed:
                    payload = {"type": "update", "bar": latest}
                    await self._broadcast(payload)
                    self._last_sent_time = t
                    self._last_sent_close = c
                    self._last_sent_vol = v
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.25)

    async def _broadcast(self, obj: Dict[str, Any]) -> None:
        if not self.clients:
            return
        msg = json.dumps(obj)
        stale: List[WebSocket] = []
        for ws in self.clients:
            try:
                await ws.send_text(msg)
            except Exception:
                stale.append(ws)
        for ws in stale:
            try:
                self.clients.remove(ws)
            except ValueError:
                pass


broads: Dict[str, BarsBroadcaster] = {}


def get_broadcaster(tf: str) -> BarsBroadcaster:
    b = broads.get(tf)
    if not b:
        b = BarsBroadcaster(tf)
        broads[tf] = b
        asyncio.create_task(b.start())
    return b


@app.websocket("/ws/bars")
async def ws_bars(ws: WebSocket):
    await ws.accept()
    tf = ws.query_params.get("tf", "1m")
    b = get_broadcaster(tf)
    b.clients.append(ws)
    # On connect, send a small seed (last bars) so client can sync
    try:
        # Send a snapshot (last 300 bars) for context
        snap = await db.fetch_last_bars(tf, 300)
        if snap:
            await ws.send_text(json.dumps({"type":"snapshot", "series": snap}))
        # Also send the latest bar explicitly
        tail = await db.fetch_tail_bars(tf, 1)
        if tail:
            await ws.send_text(json.dumps({"type":"update", "bar": tail[0]}))
        while True:
            # Keep alive
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        pass
    except Exception:
        await asyncio.sleep(0.1)
    finally:
        try:
            b.clients.remove(ws)
        except ValueError:
            pass


class DOMBroadcaster:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self.last_payload: Optional[dict] = None

    async def add_client(self, ws: WebSocket):
        async with self._lock:
            self.clients.add(ws)
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())

    async def remove_client(self, ws: WebSocket):
        async with self._lock:
            self.clients.discard(ws)

    async def _broadcast(self, payload: dict):
        if not self.clients:
            self.last_payload = payload
            return
        self.last_payload = payload
        data = json.dumps(payload)
        dead: List[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.remove_client(ws)

    async def _run(self):
        book = LocalOrderBook(L2_SYMBOL)
        queue: asyncio.Queue = asyncio.Queue()

        def _compact_snapshot(
            book: LocalOrderBook, levels: Optional[int] = None
        ) -> tuple[list[tuple[str, str]], list[tuple[str, str]], Optional[float]]:
            bids = sorted(book.bids.items(), key=lambda x: x[0], reverse=True)
            asks = sorted(book.asks.items(), key=lambda x: x[0])
            if levels is not None:
                bids = bids[:levels]
                asks = asks[:levels]
            best_bid = float(bids[0][0]) if bids else None
            best_ask = float(asks[0][0]) if asks else None
            mid = None
            if best_bid is not None and best_ask is not None:
                mid = (best_bid + best_ask) / 2.0
            cbids = [(format(p, 'f'), format(q, 'f')) for p, q in bids]
            casks = [(format(p, 'f'), format(q, 'f')) for p, q in asks]
            return cbids, casks, mid

        backoff = 1.0
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=None)
                connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    async with session.ws_connect(
                        WS_URL,
                        heartbeat=20.0,
                        max_msg_size=0,
                    ) as ws:
                        reader_task = asyncio.create_task(ws_reader(ws, queue))

                        # Snapshot sync
                        snap = await fetch_snapshot(session)
                        book.clear()
                        book.load_snapshot(snap)
                        last_update_id = book.last_update_id

                        pre_bcast_cancel = False
                        async def pre_broadcaster():
                            while not pre_bcast_cancel:
                                await asyncio.sleep(0.5)
                                bids, asks, mid = _compact_snapshot(book)
                                await self._broadcast({"type": "dom", "mid": mid, "bids": bids, "asks": asks})

                        pre_bcast_task = asyncio.create_task(pre_broadcaster())

                        synced = False
                        prev_u = None
                        while not synced:
                            ev = await queue.get()
                            U = int(ev["U"])  # noqa
                            u = int(ev["u"])  # noqa
                            if u < last_update_id:
                                continue
                            if U <= last_update_id <= u:
                                book.apply_diff(ev.get("b", []), ev.get("a", []))
                                prev_u = u
                                book.prev_u = prev_u
                                synced = True

                        pre_bcast_cancel = True
                        try:
                            pre_bcast_task.cancel()
                        except Exception:
                            pass

                        last_payload = None
                        async def broadcaster():
                            while True:
                                await asyncio.sleep(0.25)
                                bids, asks, mid = _compact_snapshot(book)
                                payload = {"type": "dom", "mid": mid, "bids": bids, "asks": asks}
                                await self._broadcast(payload)

                        bcast_task = asyncio.create_task(broadcaster())

                        while True:
                            ev = await queue.get()
                            u = int(ev["u"])  # current
                            pu = int(ev.get("pu", -1))
                            if pu != book.prev_u:
                                bcast_task.cancel()
                                snap = await fetch_snapshot(session)
                                book.clear()
                                book.load_snapshot(snap)
                                last_update_id = book.last_update_id
                                synced = False
                                prev_u = None
                                while not synced:
                                    ev = await queue.get()
                                    U = int(ev["U"])  # noqa
                                    u = int(ev["u"])  # noqa
                                    if u < last_update_id:
                                        continue
                                    if U <= last_update_id <= u:
                                        book.apply_diff(ev.get("b", []), ev.get("a", []))
                                        prev_u = u
                                        book.prev_u = prev_u
                                        synced = True
                                bcast_task = asyncio.create_task(broadcaster())
                                continue

                            book.apply_diff(ev.get("b", []), ev.get("a", []))
                            book.prev_u = u

            except asyncio.CancelledError:
                break
            except Exception:
                if not self.clients:
                    return
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)


dom_broadcaster = DOMBroadcaster()


# ===== Mark/Index/Settlement price broadcaster =====
class MarkPriceBroadcaster:
    """Maintains a single upstream Binance futures markPrice stream and
    broadcasts {p: mark, i: index, P: est. settlement} to connected clients.

    Endpoint: /ws/mark
    Upstream: wss://fstream.binance.com/ws/{symbol}@markPrice
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol.lower()
        self.clients: set[WebSocket] = set()
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._last: Optional[dict] = None
        self._stop_evt = asyncio.Event()

    async def add_client(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self.clients.add(ws)
            # Start upstream if not running
            if self._task is None or self._task.done():
                self._stop_evt.clear()
                self._task = asyncio.create_task(self._run())
        # Seed with last value if available
        if self._last:
            try:
                await ws.send_text(json.dumps(self._last))
            except Exception:
                pass

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._lock:
            self.clients.discard(ws)
            # If no clients, stop upstream after a short delay
            if not self.clients and self._task and not self._task.done():
                self._stop_evt.set()

    async def _broadcast(self, obj: dict) -> None:
        if obj:
            self._last = obj
        if not self.clients:
            return
        data = json.dumps(obj)
        stale: list[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_text(data)
            except Exception:
                stale.append(ws)
        for ws in stale:
            await self.remove_client(ws)

    async def _run(self) -> None:
        backoff = 1.0
        url = f"wss://fstream.binance.com/ws/{self.symbol}@markPrice"
        while not self._stop_evt.is_set():
            try:
                timeout = aiohttp.ClientTimeout(total=None)
                connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    async with session.ws_connect(url, heartbeat=20.0, max_msg_size=0) as ws:
                        async for msg in ws:
                            if self._stop_evt.is_set():
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue
                            # Binance fields: p (mark), i (index), P (est. settlement), E event time
                            try:
                                mp = float(data.get("p")) if data.get("p") is not None else None
                                ip = float(data.get("i")) if data.get("i") is not None else None
                                esp = float(data.get("P")) if data.get("P") is not None else None
                            except Exception:
                                mp = ip = esp = None
                            payload = {
                                "type": "mark",
                                "p": mp,
                                "i": ip,
                                "P": esp,
                                "t": int((data.get("E") or 0) // 1000),
                            }
                            await self._broadcast(payload)
                # normal close -> break
                if self._stop_evt.is_set():
                    break
            except asyncio.CancelledError:
                break
            except Exception:
                if self._stop_evt.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 1.6)


mark_broadcaster = MarkPriceBroadcaster(SYMBOL)


@app.websocket("/ws/dom")
async def ws_dom(ws: WebSocket):
    await ws.accept()
    await dom_broadcaster.add_client(ws)
    try:
        while True:
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        await dom_broadcaster.remove_client(ws)
        return


@app.websocket("/ws/mark")
async def ws_mark(ws: WebSocket):
    """Client subscription for mark/index/settlement prices.
    Forwards latest Binance futures markPrice stream values.
    """
    await mark_broadcaster.add_client(ws)
    try:
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        await mark_broadcaster.remove_client(ws)
        return


@app.post("/api/heatmap-csv/init")
async def heatmap_csv_init(path: Optional[str] = None) -> JSONResponse:
    """Initialize CSV-backed heatmap store and start tailing.

    If path is not provided, use HEATMAP_CSV_PATH env var.
    """
    global csv_store, csv_notifier
    try:
        csv_path = path or os.getenv("HEATMAP_CSV_PATH")
        if not csv_path:
            return JSONResponse({"error": "CSV path not provided and HEATMAP_CSV_PATH not set"}, status_code=400)
        if csv_store is None:
            csv_store = CSVHeatmapStore(csv_path)
            await csv_store.start()
        # Ensure notifier exists
        if csv_notifier is None:
            csv_notifier = CSVHeatmapNotifier(csv_store)
        return JSONResponse({
            "status": "started",
            "path": csv_path,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/heatmap-csv/status")
async def heatmap_csv_status() -> JSONResponse:
    global csv_store
    if csv_store is None:
        return JSONResponse({"ready": False, "initialized": False})
    return JSONResponse({
        "ready": csv_store.ready,
        "initialized": True,
        "parsed_rows": csv_store.parsed_rows,
        "loaded_bytes": csv_store.loaded_bytes,
        "total_bytes": csv_store.total_bytes,
        "last_timestamp": csv_store.last_timestamp,
        "path": csv_store.path,
    })


@app.websocket("/ws/heatmap")
async def ws_heatmap(ws: WebSocket):
    await ws.accept()
    q = ws.query_params
    tick_str = q.get("tick", "0.10") if hasattr(q, 'get') else "0.10"
    source = q.get("source", "db") if hasattr(q, 'get') else "db"
    tf = q.get("tf", "1m") if hasattr(q, 'get') else "1m"
    try:
        tick = float(tick_str)
    except Exception:
        tick = 0.10
    bin_sec = _bin_sec_for_tf(tf)
    try:
        if source == "csv":
            global csv_store, csv_notifier
            if not csv_store:
                csv_path = os.getenv("HEATMAP_CSV_PATH")
                if not csv_path:
                    await ws.send_text(json.dumps({"type": "error", "message": "CSV source requested but HEATMAP_CSV_PATH not set"}))
                    await ws.close()
                    return
                csv_store = CSVHeatmapStore(csv_path)
                await csv_store.start()
            if not csv_notifier:
                csv_notifier = CSVHeatmapNotifier(csv_store)
            await csv_notifier.add_client(ws, tick, bin_sec)
            # Optionally send a last snapshot immediately if available (bucket-aligned)
            if csv_store.last_timestamp:
                r = csv_store._rows[-1] if csv_store._rows else None
                if r:
                    rows_fmt = csv_store._aggregate_to_rows(r['bids'], r['asks'], tick)
                    ts = float(r.get('timestamp') or csv_store.last_timestamp)
                    b = int(ts) - (int(ts) % bin_sec)
                    await ws.send_text(json.dumps({"type": "hm", "snap": {"t": int(b), "rows": rows_fmt}}))
        else:
            await hm_notifier.add_client(ws, tick, bin_sec)
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        if source == "csv" and csv_notifier:
            await csv_notifier.remove_client(ws)
        else:
            await hm_notifier.remove_client(ws)
        return
    except Exception:
        if source == "csv" and csv_notifier:
            await csv_notifier.remove_client(ws)
        else:
            await hm_notifier.remove_client(ws)
        return


@app.get("/health")
async def health() -> JSONResponse:
    db_status = "up" if db.pool is not None else "down"
    csv_status = None
    if 'csv_store' in globals() and csv_store is not None:
        csv_status = {
            "ready": bool(getattr(csv_store, 'ready', False)),
            "parsed_rows": int(getattr(csv_store, 'parsed_rows', 0)),
            "loaded_bytes": int(getattr(csv_store, 'loaded_bytes', 0)),
            "total_bytes": int(getattr(csv_store, 'total_bytes', 0)),
        }
    return JSONResponse({"status": "ok", "db": db_status, "csv": csv_status})


@app.on_event("startup")
async def startup_event():
    if not os.path.isdir(WEB_DIR):
        return
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            snap = await fetch_snapshot(session)
            book = LocalOrderBook(L2_SYMBOL)
            book.load_snapshot(snap)
            bids = sorted(book.bids.items(), key=lambda x: x[0], reverse=True)
            asks = sorted(book.asks.items(), key=lambda x: x[0])
            best_bid = float(bids[0][0]) if bids else None
            best_ask = float(asks[0][0]) if asks else None
            mid = (best_bid + best_ask) / 2.0 if best_bid is not None and best_ask is not None else None
            dom_broadcaster.last_payload = {
                "type": "dom",
                "mid": mid,
                "bids": [(format(p, 'f'), format(q, 'f')) for p,q in bids],
                "asks": [(format(p, 'f'), format(q, 'f')) for p,q in asks],
            }
    except Exception:
        pass


@app.get("/api/dom/last")
async def get_dom_last():
    if dom_broadcaster.last_payload is None:
        return JSONResponse({"error": "no dom yet"}, status_code=404)
    return JSONResponse(dom_broadcaster.last_payload)


@app.get("/api/dom/snapshot")
async def get_dom_snapshot(levels: int = -1):
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            snap = await fetch_snapshot(session)
            book = LocalOrderBook(L2_SYMBOL)
            book.load_snapshot(snap)
            def _compact(book: LocalOrderBook, levels_opt: Optional[int] = None):
                bids = sorted(book.bids.items(), key=lambda x: x[0], reverse=True)
                asks = sorted(book.asks.items(), key=lambda x: x[0])
                if levels_opt is not None:
                    bids = bids[:levels_opt]
                    asks = asks[:levels_opt]
                best_bid = float(bids[0][0]) if bids else None
                best_ask = float(asks[0][0]) if asks else None
                mid = None
                if best_bid is not None and best_ask is not None:
                    mid = (best_bid + best_ask) / 2.0
                cbids = [(format(p, 'f'), format(q, 'f')) for p, q in bids]
                casks = [(format(p, 'f'), format(q, 'f')) for p, q in asks]
                return {"type": "dom", "mid": mid, "bids": cbids, "asks": casks}
            levels_opt = None if levels is None or levels < 0 else int(levels)
            payload = _compact(book, levels_opt)
            return JSONResponse(payload)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


# Root index under / using static web dir
if os.path.isdir(WEB_DIR):
    # Root handler defined above in the static mount section; keep a single definition.
    pass


if __name__ == "__main__":
    import uvicorn
    print("=" * 60)
    print("🚀 FH Terminal Server Starting")
    print("=" * 60)
    print("📊 Main Chart (with DOM/FP/Tools):  http://127.0.0.1:8000")
    print("🔥 Heatmap Chart (simplified):    http://127.0.0.1:8000/heatmap")
    print("=" * 60)
    uvicorn.run("server_db:app", host="127.0.0.1", port=8000, reload=False, log_level="info")
