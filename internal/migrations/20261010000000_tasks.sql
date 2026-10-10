BEGIN;
CREATE TABLE tasks (
    id uuid PRIMARY KEY,
    name text NOT NULL DEFAULT '' CHECK (octet_length(name) <= 256),
    payload text NOT NULL CHECK (octet_length(payload) <= 1024),
    version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    -- 0 = incomplete, 1 = completed; processing is is_running with status 0.
    status smallint NOT NULL DEFAULT 0 CHECK (status IN (0,1)),
    is_running boolean NOT NULL DEFAULT false,
    is_submitted boolean NOT NULL DEFAULT false,
    -- Executions without an outcome (network, deadline or stale claim).
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry timestamptz NOT NULL,
    result text,
    error text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL CHECK (updated_at >= created_at),
    CHECK (NOT (is_running AND status = 1)),
    -- Completed exactly when an outcome is recorded.
    CHECK ((status = 1) = (result IS NOT NULL OR error IS NOT NULL))
);
-- Keyset pagination for GET /tasks.
CREATE INDEX tasks_created_at_id_idx ON tasks (created_at DESC, id DESC);
-- Publisher scan of due, unsubmitted tasks.
CREATE INDEX tasks_unsubmitted ON tasks (next_retry, created_at, id) WHERE is_submitted = false AND error IS NULL;
-- Sweeper scan of submitted tasks whose message may have expired unclaimed.
CREATE INDEX tasks_submitted_pending ON tasks (updated_at) WHERE is_submitted = true AND status = 0;
-- Sweeper scan of running claims gone stale (ReleaseStale).
CREATE INDEX tasks_running ON tasks (updated_at) WHERE is_running = true;
COMMIT;
