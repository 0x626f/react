CREATE TABLE IF NOT EXISTS {{table}} (
    namespace text NOT NULL,
    id text NOT NULL,
    destination text NOT NULL,
    message_type text NOT NULL,
    aggregate_type text,
    aggregate_id text,
    ordering_key text,
    idempotency_key text,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload bytea NOT NULL,
    content_digest char(64) NOT NULL,
    state text NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    available_at timestamptz NOT NULL,
    lease_owner text,
    lease_token text,
    lease_until timestamptz,
    completed_lease_owner text,
    completed_lease_token text,
    completed_lease_version bigint,
    last_error_code text,
    last_error_message text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    delivered_at timestamptz,
    dead_at timestamptz,
    cancelled_at timestamptz,
    version bigint NOT NULL DEFAULT 1,
    PRIMARY KEY (namespace, id),
    CONSTRAINT react_outbox_state_check CHECK (state IN ('pending','leased','delivered','dead','cancelled')),
    CONSTRAINT react_outbox_attempts_check CHECK (attempts >= 0 AND max_attempts > 0),
    CONSTRAINT react_outbox_digest_check CHECK (content_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT react_outbox_lease_shape_check CHECK (
        (state = 'leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (state <> 'leased' AND lease_owner IS NULL AND lease_token IS NULL AND lease_until IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS {{idempotency_index}}
    ON {{table}} (namespace, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS {{claim_index}}
    ON {{table}} (namespace, state, available_at, created_at, id)
    WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS {{lease_index}}
    ON {{table}} (namespace, lease_until, id) WHERE state = 'leased';
CREATE INDEX IF NOT EXISTS {{destination_index}}
    ON {{table}} (namespace, destination, state, created_at, id);
CREATE INDEX IF NOT EXISTS {{terminal_index}}
    ON {{table}} (namespace, state, COALESCE(delivered_at, dead_at, cancelled_at), id)
    WHERE state IN ('delivered','dead','cancelled');
