import os
import contextlib
import asyncio
import json
import math
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from dotenv import load_dotenv


# Load env vars from .env if present
load_dotenv()


DB_DSN = os.getenv(
    "PG_DSN",
    # Fallback builds DSN from individual env vars for convenience
    None,
)

def _build_dsn() -> str:
    if DB_DSN:
        return DB_DSN
    user = os.getenv("PGUSER", "doadmin")
    pwd = os.getenv("PGPASSWORD", "")
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    db = os.getenv("PGDATABASE", "defaultdb")
    sslmode = os.getenv("PGSSLMODE", "require")
    return f"postgres://{user}:{pwd}@{host}:{port}/{db}?sslmode={sslmode}"


SYMBOL = os.getenv("APP_SYMBOL", "xmrusdt")
TABLE = os.getenv("APP_TABLE", "timeframe_data")


def compute_imbalances_for_levels(levels, tick_size, p0):
    """
    Compute imbalances from price_levels data and return frontend-compatible format.
    
    Args:
        levels: dict of price -> {buy_volume, sell_volume, ...}
        tick_size: float, the price tick size  
        p0: float, the base price for row index calculation
        
    Returns:
        List[List[int]] in format [[row_index, side, kind, strength], ...]
        where:
        - side: 0=sell/ask, 1=buy/bid
        - kind: 0=same_level, 1=diagonal  
        - strength: 1=normal, 2=strong, 3=heavy
    """
    if not levels or not isinstance(levels, dict) or tick_size <= 0:
        return []
    
    # Imbalance detection parameters
    MIN_VOL = 5.0      # minimum opposite-side volume to qualify
    R_NORM = 3.0       # ratio ≥ 3 → "normal" 
    R_STRONG = 5.0     # ratio ≥ 5 → "strong"
    R_HEAVY = 7.0      # ratio ≥ 7 → "heavy"
    
    imb_compact = []
    
    # Convert string keys to float for calculations
    float_levels = {}
    for price_str, data in levels.items():
        try:
            price_float = float(price_str)
            if isinstance(data, dict):
                buy_vol = float(data.get("buy_volume", 0.0) or 0.0)
                sell_vol = float(data.get("sell_volume", 0.0) or 0.0)
                float_levels[price_float] = {
                    "buy_volume": buy_vol,
                    "sell_volume": sell_vol
                }
        except (ValueError, TypeError, AttributeError):
            continue
    
    if not float_levels:
        return []
    
    prices = sorted(float_levels.keys())
    
    for price in prices:
        try:
            data = float_levels[price]
            buy_vol = data["buy_volume"]
            sell_vol = data["sell_volume"]
            
            # Calculate row index for this price
            row_index = int(round((price - p0) / tick_size))
            
            # 1) Same-level BUY imbalance: Ask(P) / Bid(P)
            if sell_vol > MIN_VOL:
                ratio = buy_vol / sell_vol
                if ratio >= R_NORM:
                    strength = (3 if ratio >= R_HEAVY else
                               2 if ratio >= R_STRONG else 1)
                    imb_compact.append([row_index, 1, 0, strength])  # buy, same-level
            
            # 2) Same-level SELL imbalance: Bid(P) / Ask(P)  
            if buy_vol > MIN_VOL:
                ratio = sell_vol / buy_vol
                if ratio >= R_NORM:
                    strength = (3 if ratio >= R_HEAVY else
                               2 if ratio >= R_STRONG else 1)
                    imb_compact.append([row_index, 0, 0, strength])  # sell, same-level
            
            # 3) Diagonal BUY imbalance: Ask(P) / Bid(P-tick)
            prev_price = round(price - tick_size, 8)
            prev_data = float_levels.get(prev_price)
            if prev_data:
                prev_bid = prev_data["sell_volume"]  # sell_volume = bid side
                if prev_bid > MIN_VOL:
                    ratio = buy_vol / prev_bid
                    if ratio >= R_NORM:
                        strength = (3 if ratio >= R_HEAVY else
                                   2 if ratio >= R_STRONG else 1)
                        imb_compact.append([row_index, 1, 1, strength])  # buy, diagonal
            
            # 4) Diagonal SELL imbalance: Bid(P) / Ask(P+tick)
            next_price = round(price + tick_size, 8)
            next_data = float_levels.get(next_price)
            if next_data:
                next_ask = next_data["buy_volume"]  # buy_volume = ask side
                if next_ask > MIN_VOL:
                    ratio = sell_vol / next_ask
                    if ratio >= R_NORM:
                        strength = (3 if ratio >= R_HEAVY else
                                   2 if ratio >= R_STRONG else 1)
                        imb_compact.append([row_index, 0, 1, strength])  # sell, diagonal
                        
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            continue
    
    return imb_compact


class DB:
    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None

    async def init(self) -> None:
        dsn = _build_dsn()
        self.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def fetch_last_bars(self, timeframe: str, limit: int = 500) -> List[Dict[str, Any]]:
        assert self.pool is not None
        sql = f"""
            SELECT bucket, open, high, low, close, total_volume
            FROM {TABLE}
            WHERE timeframe = $1 AND symbol = $2
            ORDER BY bucket DESC
            LIMIT $3
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, timeframe, SYMBOL, limit)
        # reverse to ascending time
        out: List[Dict[str, Any]] = []
        for r in reversed(rows):
            bar = {
                "time": int(r["bucket"]),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
            }
            v = r.get("total_volume")
            if v is not None:
                try:
                    bar["volume"] = float(v)
                except Exception:
                    pass
            out.append(bar)
        return out

    async def fetch_tail_bars(self, timeframe: str, n: int = 5) -> List[Dict[str, Any]]:
        assert self.pool is not None
        sql = f"""
            SELECT bucket, open, high, low, close, total_volume
            FROM {TABLE}
            WHERE timeframe = $1 AND symbol = $2
            ORDER BY bucket DESC
            LIMIT $3
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, timeframe, SYMBOL, n)
        # keep order descending in return
        out: List[Dict[str, Any]] = []
        for r in rows:
            bar = {
                "time": int(r["bucket"]),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
            }
            v = r.get("total_volume")
            if v is not None:
                try:
                    bar["volume"] = float(v)
                except Exception:
                    pass
            out.append(bar)
        return out

    async def fetch_footprint_cols(self, timeframe: str, limit: int = 300) -> List[Dict[str, Any]]:
        assert self.pool is not None
        sql = f"""
            SELECT bucket, price_levels
            FROM {TABLE}
            WHERE timeframe = $1 AND symbol = $2
            ORDER BY bucket DESC
            LIMIT $3
        """
        
        # Add connection timeout and retry logic
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Use shorter timeout for connection acquisition
                async with self.pool.acquire() as conn:
                    rows = await asyncio.wait_for(
                        conn.fetch(sql, timeframe, SYMBOL, limit), 
                        timeout=5.0  # 5 second timeout
                    )
                break  # Success, exit retry loop
            except (asyncio.TimeoutError, OSError, ConnectionError, Exception) as e:
                print(f"Database connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)  # Wait 1 second before retry
                else:
                    print(f"All database attempts failed. Returning empty data.")
                    return []  # Return empty list if all attempts fail
        out: List[Dict[str, Any]] = []
        # parse price_levels JSON string in Python to avoid DB-specific JSON operators
        import json, math
        rows_list = list(reversed(rows))
        for r in rows_list:
            t = int(r["bucket"])
            raw = r["price_levels"] or "{}"
            try:
                levels = json.loads(raw)
            except Exception:
                try:
                    levels = json.loads(raw.replace("'", '"'))
                except Exception:
                    levels = {}
            if not isinstance(levels, dict) or not levels:
                # even if no levels, try include empty imb
                imb_compact: List[List[int]] = []
                out.append({"t": t, "p0": None, "tick": None, "rows": [], "imb": imb_compact})
                continue
            prices = []
            for ps in levels.keys():
                try:
                    prices.append(float(ps))
                except Exception:
                    pass
            if not prices:
                out.append({"t": t, "p0": None, "tick": None, "rows": [], "imb": []})
                continue
            # infer tick
            s = sorted(set(round(p, 8) for p in prices))
            diffs = [round(s[i]-s[i-1], 8) for i in range(1,len(s)) if (s[i]-s[i-1])>0]
            tick = min(diffs) if diffs else 0.01
            for c in [0.0001,0.001,0.005,0.01,0.02,0.05,0.1,0.2,0.5,1.0]:
                if abs(tick-c) <= c*0.25:
                    tick = c; break
            pmin, pmax = min(prices), max(prices)
            p0 = math.floor(pmin/tick)*tick
            pN = math.ceil(pmax/tick)*tick
            n = int(round((pN-p0)/tick)) + 1
            bids = [0.0]*n; asks=[0.0]*n
            for ps, vals in levels.items():
                try:
                    price = float(ps)
                    bv = float(vals.get("buy_volume", 0.0) or 0.0)
                    sv = float(vals.get("sell_volume", 0.0) or 0.0)
                except Exception:
                    continue
                idx = int(round((price-p0)/tick))
                if 0 <= idx < n:
                    bids[idx]+=bv; asks[idx]+=sv
            rows_comp = []
            for i in range(n):
                b=bids[i]; a=asks[i]
                if b>0 or a>0:
                    rows_comp.append([i, round(b,6), round(a,6)])

            # Compute imbalances from price_levels data
            imb_compact = compute_imbalances_for_levels(levels, tick, p0)

            out.append({"t": t, "p0": round(p0,8), "tick": tick, "rows": rows_comp, "imb": imb_compact})
        return out

    async def fetch_delta_cols(self, timeframe: str, limit: int = 300) -> List[Dict[str, Any]]:
        """Fetch delta columns by computing delta = buy_volume - sell_volume per price level.
        
        Returns: [{ t, p0, tick, rows: [[row_index, delta], ...] }, ...]
        """
        assert self.pool is not None
        sql = f"""
            SELECT bucket, price_levels
            FROM {TABLE}
            WHERE timeframe = $1 AND symbol = $2
            ORDER BY bucket DESC
            LIMIT $3
        """
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.pool.acquire() as conn:
                    rows = await asyncio.wait_for(
                        conn.fetch(sql, timeframe, SYMBOL, limit), 
                        timeout=5.0
                    )
                break
            except (asyncio.TimeoutError, OSError, ConnectionError, Exception) as e:
                print(f"Delta fetch attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    print(f"All delta fetch attempts failed. Returning empty data.")
                    return []
        
        out: List[Dict[str, Any]] = []
        rows_list = list(reversed(rows))
        for r in rows_list:
            t = int(r["bucket"])
            raw = r["price_levels"] or "{}"
            try:
                levels = json.loads(raw)
            except Exception:
                try:
                    levels = json.loads(raw.replace("'", '"'))
                except Exception:
                    levels = {}
            if not isinstance(levels, dict) or not levels:
                out.append({"t": t, "p0": None, "tick": None, "rows": []})
                continue
            
            prices = []
            for ps in levels.keys():
                try:
                    prices.append(float(ps))
                except Exception:
                    pass
            if not prices:
                out.append({"t": t, "p0": None, "tick": None, "rows": []})
                continue
            
            # Infer tick (same logic as footprint)
            s = sorted(set(round(p, 8) for p in prices))
            diffs = [round(s[i]-s[i-1], 8) for i in range(1,len(s)) if (s[i]-s[i-1])>0]
            tick = min(diffs) if diffs else 0.01
            for c in [0.0001,0.001,0.005,0.01,0.02,0.05,0.1,0.2,0.5,1.0]:
                if abs(tick-c) <= c*0.25:
                    tick = c
                    break
            
            pmin, pmax = min(prices), max(prices)
            p0 = math.floor(pmin/tick)*tick
            pN = math.ceil(pmax/tick)*tick
            n = int(round((pN-p0)/tick)) + 1
            deltas = [0.0]*n
            
            for ps, vals in levels.items():
                try:
                    price = float(ps)
                    bv = float(vals.get("buy_volume", 0.0) or 0.0)
                    sv = float(vals.get("sell_volume", 0.0) or 0.0)
                    delta = bv - sv
                except Exception:
                    continue
                idx = int(round((price-p0)/tick))
                if 0 <= idx < n:
                    deltas[idx] += delta
            
            rows_comp = []
            for i in range(n):
                d = deltas[i]
                if abs(d) > 1e-6:  # Only include non-zero deltas
                    rows_comp.append([i, round(d, 6)])
            
            out.append({"t": t, "p0": round(p0,8), "tick": tick, "rows": rows_comp})
        return out


class BarsNotifier:
    """LISTEN/NOTIFY-based notifier with poller fallback.

    Usage: await BarsNotifier(db).start(timeframes=["1m","3m"]) and then read events via queue per tf.
    """

    def __init__(self, db: DB) -> None:
        self.db = db
        self.conn: Optional[asyncpg.Connection] = None
        self.listen_channels: List[str] = []
        self.queues: Dict[str, asyncio.Queue] = {}
        self._task: Optional[asyncio.Task] = None
        self._fallback_tasks: List[asyncio.Task] = []
        self._stop = asyncio.Event()

    async def start(self, timeframes: List[str]) -> None:
        # Create queues
        for tf in timeframes:
            self.queues.setdefault(tf, asyncio.Queue(maxsize=1000))
        # Try LISTEN
        try:
            dsn = _build_dsn()
            self.conn = await asyncpg.connect(dsn)
            for tf in timeframes:
                ch = f"bars_{tf}"
                self.listen_channels.append(ch)
                await self.conn.add_listener(ch, self._on_notify)
            self._task = asyncio.create_task(self._listen_loop())
        except Exception:
            # Fallback to pollers
            await self._start_pollers(timeframes)

    async def _listen_loop(self) -> None:
        # Keep the connection alive until stop
        assert self.conn is not None
        try:
            while not self._stop.is_set():
                await asyncio.sleep(0.25)
        finally:
            try:
                for ch in self.listen_channels:
                    await self.conn.remove_listener(ch, self._on_notify)
            except Exception:
                pass
            try:
                await self.conn.close()
            except Exception:
                pass

    def _on_notify(self, connection: asyncpg.Connection, pid: int, channel: str, payload: str) -> None:
        # Expected channel name: bars_<tf>
        try:
            tf = channel.split("_",1)[1]
        except Exception:
            return
        q = self.queues.get(tf)
        if not q:
            return
        try:
            q.put_nowait({"type":"notify","payload":payload})
        except asyncio.QueueFull:
            # Drop if overwhelmed; Broadcaster will reconcile
            pass

    async def _start_pollers(self, timeframes: List[str]) -> None:
        # Poll tail as a fallback
        for tf in timeframes:
            task = asyncio.create_task(self._poll_loop(tf))
            self._fallback_tasks.append(task)

    async def _poll_loop(self, tf: str) -> None:
        # Simple poller: push a tick every 200ms
        q = self.queues[tf]
        while not self._stop.is_set():
            try:
                await q.put({"type":"poll"})
            except Exception:
                pass
            await asyncio.sleep(0.2)

    def queue_for(self, tf: str) -> asyncio.Queue:
        return self.queues[tf]

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
        for t in self._fallback_tasks:
            t.cancel()
        self._fallback_tasks.clear()
