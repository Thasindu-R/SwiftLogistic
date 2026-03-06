-- =============================================
-- SwiftTrack – PostgreSQL schema initialisation
-- =============================================

--  Users 
CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    username        VARCHAR(50)  UNIQUE NOT NULL,
    email           VARCHAR(100) UNIQUE NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    full_name       VARCHAR(100) NOT NULL DEFAULT '',
    phone           VARCHAR(20)  NOT NULL DEFAULT '',
    role            VARCHAR(20)  NOT NULL DEFAULT 'client',  -- client | driver | admin
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

--  Orders 
CREATE TABLE IF NOT EXISTS orders (
    id                  SERIAL PRIMARY KEY,
    order_id            VARCHAR(36)  UNIQUE NOT NULL,  -- UUID
    client_id           INTEGER      NOT NULL REFERENCES users(id),
    assigned_driver_id  INTEGER      REFERENCES users(id),
    status              VARCHAR(30)  NOT NULL DEFAULT 'pending',
    pickup_address      TEXT         NOT NULL,
    delivery_address    TEXT         NOT NULL,
    package_description TEXT         NOT NULL DEFAULT '',
    package_weight      REAL         NOT NULL DEFAULT 0.0,
    priority            VARCHAR(10)  NOT NULL DEFAULT 'normal',
    recipient_name      VARCHAR(100) NOT NULL DEFAULT '',
    recipient_phone     VARCHAR(20)  NOT NULL DEFAULT '',
    estimated_cost      REAL,
    assignment_type     VARCHAR(20),  -- 'auto' or 'manual'
    notes               TEXT         NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_client  ON orders(client_id);
CREATE INDEX IF NOT EXISTS idx_orders_status  ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_driver  ON orders(assigned_driver_id);

--  Tracking events 
CREATE TABLE IF NOT EXISTS tracking_events (
    id          SERIAL PRIMARY KEY,
    order_id    VARCHAR(36)  NOT NULL,
    event_type  VARCHAR(50)  NOT NULL,
    description TEXT         NOT NULL DEFAULT '',
    location    VARCHAR(200) NOT NULL DEFAULT '',
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    driver_id   INTEGER,
    timestamp   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracking_order ON tracking_events(order_id);

-- ── Delivery manifests (driver daily routes) 
CREATE TABLE IF NOT EXISTS delivery_manifests (
    id          SERIAL PRIMARY KEY,
    manifest_id VARCHAR(36)  UNIQUE NOT NULL,
    driver_id   INTEGER      NOT NULL REFERENCES users(id),
    date        DATE         NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'pending',
    route_data  TEXT,  -- JSON blob from ROS
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── Delivery items (individual packages on a manifest) ──────
CREATE TABLE IF NOT EXISTS delivery_items (
    id                 SERIAL PRIMARY KEY,
    manifest_id        VARCHAR(36)  NOT NULL REFERENCES delivery_manifests(manifest_id),
    order_id           VARCHAR(36)  NOT NULL,
    sequence           INTEGER      NOT NULL DEFAULT 0,
    status             VARCHAR(20)  NOT NULL DEFAULT 'pending',
    proof_of_delivery  TEXT,
    signature_data     TEXT,
    failure_reason     VARCHAR(200),
    notes              TEXT         NOT NULL DEFAULT '',
    delivered_at       TIMESTAMPTZ
);

-- ── Integration events (audit log for middleware calls) ──────
CREATE TABLE IF NOT EXISTS integration_events (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(36)  UNIQUE NOT NULL,
    order_id        VARCHAR(36),
    source_system   VARCHAR(30)  NOT NULL,  -- cms | ros | wms | order-service | tracking-service
    target_system   VARCHAR(30)  NOT NULL,
    event_type      VARCHAR(60)  NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending | success | failed | retrying
    request_data    TEXT,
    response_data   TEXT,
    error_message   TEXT,
    retry_count     INTEGER       NOT NULL DEFAULT 0,
    max_retries     INTEGER       NOT NULL DEFAULT 3,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_integration_order   ON integration_events(order_id);
CREATE INDEX IF NOT EXISTS idx_integration_status  ON integration_events(status);
CREATE INDEX IF NOT EXISTS idx_integration_source  ON integration_events(source_system);

-- ── Notifications ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS notifications (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER      NOT NULL REFERENCES users(id),
    title       VARCHAR(200) NOT NULL,
    message     TEXT         NOT NULL DEFAULT '',
    type        VARCHAR(30)  NOT NULL DEFAULT 'info',  -- info | success | warning | error
    is_read     BOOLEAN      NOT NULL DEFAULT FALSE,
    order_id    VARCHAR(36),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notifications_user  ON notifications(user_id);

-- ── Seed data: default admin user (password = "admin123") ───
-- bcrypt hash for "admin123"
INSERT INTO users (username, email, password_hash, full_name, role)
VALUES (
    'admin',
    'admin@swiftlogistics.lk',
    '$2b$12$FF8ix2UVatF/Rqijh8x7aeZPAww76plYihS6vUOY2x/Xu8Y4FLfa.',
    'System Admin',
    'admin'
) ON CONFLICT (username) DO NOTHING;

-- Seed driver (password = "driver123")
INSERT INTO users (username, email, password_hash, full_name, phone, role)
VALUES (
    'driver',
    'driver@swiftlogistics.lk',
    '$2b$12$26NNFThHLhyiuF3L50eKUeZde5TfTgXUG/f0gxsLGvm0wqn0qCog2',
    'Kamal Perera',
    '+94771111111',
    'driver'
) ON CONFLICT (username) DO NOTHING;

-- Seed client (password = "client123")
INSERT INTO users (username, email, password_hash, full_name, phone, role)
VALUES (
    'client',
    'client@example.com',
    '$2b$12$8RpkToBfIDFdjtDBJ9fo8eAERwPRFAlPxatX16UygxqAMUEdYFKy6',
    'ABC Online Store',
    '+94772222222',
    'client'
) ON CONFLICT (username) DO NOTHING;

-- ══════════════════════════════════════════════════════════════
-- Async Processing & Reliability Tables
-- ══════════════════════════════════════════════════════════════

-- ── Saga Records (Distributed Transaction State) ─────────────
CREATE TABLE IF NOT EXISTS saga_records (
    id              SERIAL PRIMARY KEY,
    saga_id         VARCHAR(36)  UNIQUE NOT NULL,
    order_id        VARCHAR(36)  NOT NULL,
    state           VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending | in_progress | completed | compensating | compensated | failed
    steps_json      TEXT         NOT NULL DEFAULT '[]',
    order_data_json TEXT,
    error_message   TEXT,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saga_order    ON saga_records(order_id);
CREATE INDEX IF NOT EXISTS idx_saga_state    ON saga_records(state);
CREATE INDEX IF NOT EXISTS idx_saga_started  ON saga_records(started_at);

-- ── Saga Status History (State Transitions) ──────────────────
CREATE TABLE IF NOT EXISTS saga_status_history (
    id          SERIAL PRIMARY KEY,
    saga_id     VARCHAR(36)  NOT NULL,
    order_id    VARCHAR(36)  NOT NULL,
    step_name   VARCHAR(30),
    from_state  VARCHAR(20),
    to_state    VARCHAR(20)  NOT NULL,
    details     TEXT,
    timestamp   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saga_history_saga   ON saga_status_history(saga_id);
CREATE INDEX IF NOT EXISTS idx_saga_history_order  ON saga_status_history(order_id);
CREATE INDEX IF NOT EXISTS idx_saga_history_time   ON saga_status_history(timestamp);

-- ── Integration Event Logs (Enhanced Audit) ──────────────────
CREATE TABLE IF NOT EXISTS integration_event_logs (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(36)  UNIQUE NOT NULL,
    correlation_id  VARCHAR(36),
    order_id        VARCHAR(36),
    saga_id         VARCHAR(36),
    source_system   VARCHAR(50)  NOT NULL,
    target_system   VARCHAR(50)  NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',
    severity        VARCHAR(20)  NOT NULL DEFAULT 'info',
    request_data    TEXT,
    response_data   TEXT,
    error_message   TEXT,
    error_code      VARCHAR(50),
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    max_retries     INTEGER      NOT NULL DEFAULT 3,
    duration_ms     INTEGER,
    metadata_json   TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_log_order       ON integration_event_logs(order_id);
CREATE INDEX IF NOT EXISTS idx_event_log_saga        ON integration_event_logs(saga_id);
CREATE INDEX IF NOT EXISTS idx_event_log_correlation ON integration_event_logs(correlation_id);
CREATE INDEX IF NOT EXISTS idx_event_log_status      ON integration_event_logs(status);
CREATE INDEX IF NOT EXISTS idx_event_log_target      ON integration_event_logs(target_system);
CREATE INDEX IF NOT EXISTS idx_event_log_created     ON integration_event_logs(created_at);

-- ── Audit Trail Logs (Compliance & Debugging) ────────────────
CREATE TABLE IF NOT EXISTS audit_trail_logs (
    id          SERIAL PRIMARY KEY,
    trail_id    VARCHAR(36)  UNIQUE NOT NULL,
    order_id    VARCHAR(36)  NOT NULL,
    actor_type  VARCHAR(30)  NOT NULL,  -- system | user | admin | driver
    actor_id    VARCHAR(50),
    actor_name  VARCHAR(100),
    action      VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50)  NOT NULL,  -- order | tracking | billing | route | saga
    entity_id   VARCHAR(36),
    old_value   TEXT,
    new_value   TEXT,
    details     TEXT,
    ip_address  VARCHAR(45),
    user_agent  VARCHAR(255),
    timestamp   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_order      ON audit_trail_logs(order_id);
CREATE INDEX IF NOT EXISTS idx_audit_actor      ON audit_trail_logs(actor_type, actor_id);
CREATE INDEX IF NOT EXISTS idx_audit_action     ON audit_trail_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_entity     ON audit_trail_logs(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp  ON audit_trail_logs(timestamp);

-- ── Dead Letter Queue Records (Failed Message Tracking) ──────
CREATE TABLE IF NOT EXISTS dlq_records (
    id                  SERIAL PRIMARY KEY,
    message_id          VARCHAR(36)  UNIQUE NOT NULL,
    original_queue      VARCHAR(100) NOT NULL,
    original_exchange   VARCHAR(100),
    original_routing_key VARCHAR(100),
    payload_json        TEXT         NOT NULL,
    error_reason        TEXT,
    retry_count         INTEGER      NOT NULL DEFAULT 0,
    max_retries         INTEGER      NOT NULL DEFAULT 3,
    first_failure_at    TIMESTAMPTZ  NOT NULL,
    last_failure_at     TIMESTAMPTZ  NOT NULL,
    processed           BOOLEAN      NOT NULL DEFAULT FALSE,
    processed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_queue       ON dlq_records(original_queue);
CREATE INDEX IF NOT EXISTS idx_dlq_processed   ON dlq_records(processed);
CREATE INDEX IF NOT EXISTS idx_dlq_created     ON dlq_records(created_at);

-- ── File Uploads (Proof of Delivery, Signatures) ─────────────
CREATE TABLE IF NOT EXISTS file_uploads (
    id                  SERIAL PRIMARY KEY,
    file_id             VARCHAR(36)  UNIQUE NOT NULL,
    original_filename   VARCHAR(255) NOT NULL,
    stored_filename     VARCHAR(255) NOT NULL,
    category            VARCHAR(50)  NOT NULL,  -- proof_of_delivery | signature | package_photo
    content_type        VARCHAR(100) NOT NULL,
    file_size           INTEGER      NOT NULL,
    checksum            VARCHAR(64)  NOT NULL,
    order_id            VARCHAR(36),
    user_id             INTEGER,
    width               INTEGER,
    height              INTEGER,
    thumbnail_path      VARCHAR(500),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_file_order     ON file_uploads(order_id);
CREATE INDEX IF NOT EXISTS idx_file_user      ON file_uploads(user_id);
CREATE INDEX IF NOT EXISTS idx_file_category  ON file_uploads(category);
CREATE INDEX IF NOT EXISTS idx_file_created   ON file_uploads(created_at);
