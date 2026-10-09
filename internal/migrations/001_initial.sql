BEGIN;
CREATE TABLE IF NOT EXISTS tasks (
    id uuid PRIMARY KEY,
    payload text NOT NULL CHECK (octet_length(payload) <= 65536),
    status text NOT NULL CHECK (status IN ('pending','processing','completed','canceled')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL CHECK (updated_at >= created_at),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    result text,
    error text,
    lease_token uuid,
    lease_until timestamptz,
    CHECK ((status='processing' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (status<>'processing' AND lease_token IS NULL AND lease_until IS NULL)),
    CHECK ((status='completed' AND (result IS NULL)<>(error IS NULL))
        OR (status<>'completed' AND result IS NULL AND error IS NULL))
);
CREATE TABLE IF NOT EXISTS task_events (
    event_id uuid PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES tasks(id),
    version bigint NOT NULL,
    name text NOT NULL,
    occurred_at timestamptz NOT NULL,
    UNIQUE (task_id,version)
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id uuid PRIMARY KEY REFERENCES task_events(event_id),
    task_id uuid NOT NULL REFERENCES tasks(id),
    available_at timestamptz NOT NULL,
    published_at timestamptz,
    lease_token uuid,
    lease_until timestamptz,
    CHECK ((lease_token IS NULL)=(lease_until IS NULL))
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox(available_at,event_id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS task_events_history ON task_events(task_id,occurred_at);
COMMIT;
