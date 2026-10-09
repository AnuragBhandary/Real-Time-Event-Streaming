-- Listing "recently active streams of a kind" (the web app's match list) without a full scan.
CREATE INDEX streams_kind_recent ON streams (kind, updated_at DESC);
