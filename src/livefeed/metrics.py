"""Prometheus metrics, served on ``/metrics`` by every instance."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

_FAST = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)

EVENTS_PUBLISHED = Counter("livefeed_events_published_total", "Events appended", ["kind"])
DUPLICATE_PUBLISHES = Counter(
    "livefeed_duplicate_publishes_total", "Publishes answered from an existing event_id"
)
PUBLISH_SECONDS = Histogram(
    "livefeed_publish_seconds", "Append + commit + pub/sub publish", buckets=_FAST
)
PUBLISH_SHED = Counter("livefeed_publish_shed_total", "Publishes rejected with 503 (overload)")
RATE_LIMITED = Counter("livefeed_rate_limited_total", "Publishes rejected with 429")
BUS_PUBLISH_ERRORS = Counter(
    "livefeed_bus_publish_errors_total", "Failed pub/sub publishes (repaired by resync)"
)
BUS_RECONNECTS = Counter("livefeed_bus_reconnects_total", "Pub/sub reconnections")

CONNECTIONS = Gauge("livefeed_ws_connections", "Open WebSocket connections")
CHANNELS = Gauge("livefeed_channels", "Streams with a local channel on this instance")
DELIVERED = Counter("livefeed_events_delivered_total", "Events enqueued to WebSocket clients")
FRAMES_SENT = Counter("livefeed_frames_sent_total", "WebSocket frames sent")
GAP_REPAIRS = Counter("livefeed_gap_repairs_total", "Events fetched from PostgreSQL to fill gaps")
BUS_DUPLICATES = Counter("livefeed_bus_duplicates_total", "Pub/sub messages already delivered")
INBOX_OVERFLOWS = Counter("livefeed_inbox_overflows_total", "Pub/sub messages dropped (inbox full)")
REPLAYED = Counter("livefeed_events_replayed_total", "Events sent from PostgreSQL on (re)connect")
SNAPSHOTS = Counter("livefeed_snapshots_total", "Snapshots sent", ["reason"])
DISCONNECTS = Counter("livefeed_disconnects_total", "Server-initiated closes", ["code"])
