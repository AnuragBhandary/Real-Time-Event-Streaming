-- livefeed schema. PostgreSQL is the source of truth for every event and its order; Redis
-- pub/sub only carries a best-effort copy for low-latency fan-out.

CREATE TABLE api_keys (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT             NOT NULL,
    prefix        TEXT             NOT NULL UNIQUE,
    secret_hash   BYTEA            NOT NULL,
    rate_per_s    DOUBLE PRECISION NOT NULL,
    burst         INTEGER          NOT NULL,
    created_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    revoked_at    TIMESTAMPTZ
);

-- One row per stream (e.g. one live match). last_seq is the per-stream sequence counter;
-- locking this row serialises appends to one stream while different streams proceed in
-- parallel. state is the materialised snapshot as of last_seq, updated in the same
-- transaction as the events, so (state, last_seq) is always consistent.
CREATE TABLE streams (
    id            TEXT        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_.:-]{1,128}$'),
    kind          TEXT        NOT NULL,
    owner_key_id  BIGINT      NOT NULL REFERENCES api_keys (id),
    last_seq      BIGINT      NOT NULL DEFAULT 0,
    state         JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The ordered log. (stream_id, seq) is both the identity and the replay index; event_id is the
-- producer's idempotency key, so a retried publish can never create a second sequence number.
CREATE TABLE events (
    stream_id   TEXT        NOT NULL REFERENCES streams (id) ON DELETE CASCADE,
    seq         BIGINT      NOT NULL CHECK (seq > 0),
    event_id    UUID        NOT NULL,
    type        TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_id, seq),
    CONSTRAINT events_idempotency UNIQUE (stream_id, event_id)
);
