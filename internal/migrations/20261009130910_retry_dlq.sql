BEGIN;

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS execution_id uuid;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS replay_cycle integer NOT NULL DEFAULT 0 CHECK (replay_cycle >= 0);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS not_before timestamptz;
UPDATE tasks SET execution_id=id WHERE execution_id IS NULL;
UPDATE tasks SET not_before=created_at WHERE not_before IS NULL;
ALTER TABLE tasks ALTER COLUMN execution_id SET NOT NULL;
ALTER TABLE tasks ALTER COLUMN not_before SET NOT NULL;

ALTER TABLE outbox ADD COLUMN IF NOT EXISTS execution_id uuid;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS destination text NOT NULL DEFAULT 'work' CHECK (destination IN ('work','dead-letter'));
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS failure jsonb;
-- Old work envelopes have no execution_id and are rejected by current workers.
-- Reissue only those pre-migration rows for live tasks; the NULL predicate makes
-- this safe if the migration is rerun after a partially managed deployment.
UPDATE outbox o SET
    execution_id=t.execution_id,
    destination=CASE WHEN t.status IN ('pending','processing') THEN 'work' ELSE o.destination END,
    failure=CASE WHEN t.status IN ('pending','processing') THEN NULL ELSE o.failure END,
    published_at=CASE WHEN t.status IN ('pending','processing') THEN NULL ELSE o.published_at END,
    lease_token=CASE WHEN t.status IN ('pending','processing') THEN NULL ELSE o.lease_token END,
    lease_until=CASE WHEN t.status IN ('pending','processing') THEN NULL ELSE o.lease_until END,
    available_at=CASE WHEN t.status IN ('pending','processing') THEN now() ELSE o.available_at END
FROM tasks t WHERE o.task_id=t.id AND o.execution_id IS NULL;
ALTER TABLE outbox ALTER COLUMN execution_id SET NOT NULL;

CREATE TABLE IF NOT EXISTS task_attempts (
    lease_token uuid PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES tasks(id),
    execution_id uuid NOT NULL,
    attempt integer NOT NULL CHECK (attempt > 0),
    retry_count integer NOT NULL CHECK (retry_count >= 0),
    replay_cycle integer NOT NULL CHECK (replay_cycle >= 0),
    worker_id text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome text,
    error text,
    category text,
    code text,
    UNIQUE(task_id,attempt),
    CHECK ((finished_at IS NULL) = (outcome IS NULL))
);
CREATE INDEX IF NOT EXISTS task_attempts_history ON task_attempts(task_id, replay_cycle, attempt);
INSERT INTO task_attempts(lease_token,task_id,execution_id,attempt,retry_count,replay_cycle,worker_id,started_at)
SELECT lease_token,id,execution_id,attempts,retry_count,replay_cycle,'migration',updated_at
FROM tasks WHERE status='processing' ON CONFLICT(lease_token) DO NOTHING;

CREATE TABLE IF NOT EXISTS task_dead_letters (
    message_id text PRIMARY KEY,
    source_event_id uuid NOT NULL,
    task_id uuid NOT NULL REFERENCES tasks(id),
    execution_id uuid NOT NULL,
    retry_count integer NOT NULL,
    replay_cycle integer NOT NULL,
    attempts integer NOT NULL,
    failed_at timestamptz NOT NULL,
    category text NOT NULL,
    code text NOT NULL,
    detail text NOT NULL,
    worker_id text NOT NULL,
    original_payload bytea NOT NULL,
    original_hash text NOT NULL,
    original_truncated boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS task_dead_letters_task ON task_dead_letters(task_id, failed_at DESC);

CREATE TABLE IF NOT EXISTS task_replay_receipts (
    message_id text PRIMARY KEY REFERENCES task_dead_letters(message_id),
    fingerprint text NOT NULL,
    operator text NOT NULL,
    reason text NOT NULL,
    task_id uuid NOT NULL REFERENCES tasks(id),
    execution_id uuid NOT NULL,
    replayed_at timestamptz NOT NULL
);

COMMIT;
