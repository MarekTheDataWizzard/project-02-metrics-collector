from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, field_validator
from threading import Lock
from collections import defaultdict, deque
import datetime, math, time

from prometheus_client import Counter, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST

app = FastAPI(title="Metrics Collector")

# Static + templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---------- In-memory store (with labels + recent series) ----------
class MetricsStore:
    def __init__(self, series_window=300, series_max=500):
        self._lock = Lock()
        # aggregation with labels: (event, service, status) -> metrics
        self._counts = defaultdict(int)
        self._sum = defaultdict(float)
        self._n = defaultdict(int)
        self._min = defaultdict(lambda: math.inf)
        self._max = defaultdict(lambda: -math.inf)
        # recent series per event label for FE sparklines:
        # key: (event, service, status) -> deque[(ts_sec, value)]
        self._series = defaultdict(lambda: deque(maxlen=series_max))
        self._series_window = series_window  # seconds to keep when exporting

    def track(self, event: str, value: float | None, service: str | None, status: str | None):
        key = (event, service or "", status or "")
        ts = time.time()
        with self._lock:
            self._counts[key] += 1
            if value is not None:
                self._sum[key] += value
                self._n[key] += 1
                if value < self._min[key]: self._min[key] = value
                if value > self._max[key]: self._max[key] = value
                self._series[key].append((ts, value))

    def snapshot(self):
        with self._lock:
            out = []
            for (event, service, status), count in self._counts.items():
                n = self._n[(event, service, status)]
                avg = (self._sum[(event, service, status)] / n) if n else None
                vmin = self._min[(event, service, status)] if self._min[(event, service, status)] != math.inf else None
                vmax = self._max[(event, service, status)] if self._max[(event, service, status)] != -math.inf else None
                out.append({
                    "event": event, "service": service, "status": status,
                    "count": count, "samples": n, "avg": avg, "min": vmin, "max": vmax
                })
            return out

    def series(self):
        cutoff = time.time() - self._series_window
        with self._lock:
            out = {}
            for key, dq in self._series.items():
                series = [(ts, v) for (ts, v) in dq if ts >= cutoff]
                if series:
                    event, service, status = key
                    out.setdefault(event, []).append({
                        "service": service, "status": status,
                        "points": series
                    })
            return out

STORE = MetricsStore()

# ---------- Prometheus metrics with labels ----------
REGISTRY = CollectorRegistry()
EVENT_COUNT = Counter(
    "mc_events_total",
    "Tracked events (labelled).",
    labelnames=["event", "service", "status"],
    registry=REGISTRY,
)
EVENT_LATENCY = Histogram(
    "mc_event_latency_seconds",
    "Observed latency (seconds) when value is provided.",
    labelnames=["event", "service", "status"],
    registry=REGISTRY,
)

# ---------- Models ----------
class TrackPayload(BaseModel):
    event: str
    value: float | None = None    # latency or any numeric metric
    service: str | None = None    # e.g., "payments-api"
    status: str | None = None     # e.g., "ok" | "error" | "timeout"

    @field_validator("event")
    @classmethod
    def validate_event(cls, v: str):
        v = v.strip()
        if not v:
            raise ValueError("event must be a non-empty string")
        return v

# ---------- Routes ----------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "server_time": datetime.datetime.utcnow().isoformat() + "Z"},
    )

@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "metrics-collector"}

@app.post("/track")
def track(payload: TrackPayload):
    STORE.track(payload.event, payload.value, payload.service, payload.status)
    EVENT_COUNT.labels(event=payload.event, service=payload.service or "", status=payload.status or "").inc()
    if payload.value is not None:
        EVENT_LATENCY.labels(event=payload.event, service=payload.service or "", status=payload.status or "").observe(payload.value)
    return {"ok": True}

@app.get("/metrics")
def prom_metrics():
    data = generate_latest(REGISTRY)
    return PlainTextResponse(data.decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

@app.get("/api/metrics.json")
def metrics_json():
    return JSONResponse(
        {
            "server_time": datetime.datetime.utcnow().isoformat() + "Z",
            "rows": STORE.snapshot(),      # table view with labels
        }
    )

@app.get("/api/series.json")
def series_json():
    # recent time-series for FE sparklines (seconds since epoch + value)
    return JSONResponse(
        {
            "server_time": datetime.datetime.utcnow().isoformat() + "Z",
            "series": STORE.series(),      # {event: [{service,status,points:[(ts,val),...]}, ...]}
        }
    )
