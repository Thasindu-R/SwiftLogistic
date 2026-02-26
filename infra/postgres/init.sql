-- ============================================================
-- SwiftTrack – PostgreSQL schema initialisation
-- Executed automatically when the postgres container starts
-- for the first time (mounted as init script).
-- ============================================================

-- ── Users ───────────────────────────────────────────────────
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

-- ── Orders ──────────────────────────────────────────────────
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
    notes               TEXT         NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_client  ON orders(client_id);
CREATE INDEX IF NOT EXISTS idx_orders_status  ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_driver  ON orders(assigned_driver_id);

-- ── Tracking events ─────────────────────────────────────────
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

-- ── Delivery manifests (driver daily routes) ────────────────
CREATE TABLE IF NOT EXISTS delivery_manifests (
    id          SERIAL PRIMARY KEY,
    manifest_id VARCHAR(36)  UNIQUE NOT NULL,
    driver_id   INTEGER      NOT NULL REFERENCES users(id),
    date        DATE         NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'pending',
    route_data  TEXT,  -- JSON blob from Route Optimisation System
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

-- Seed driver
INSERT INTO users (username, email, password_hash, full_name, phone, role)
VALUES (
    'driver1',
    'driver1@swiftlogistics.lk',
    '$2b$12$FF8ix2UVatF/Rqijh8x7aeZPAww76plYihS6vUOY2x/Xu8Y4FLfa.',
    'Kamal Perera',
    '+94771111111',
    'driver'
) ON CONFLICT (username) DO NOTHING;

-- Seed second driver
INSERT INTO users (username, email, password_hash, full_name, phone, role)
VALUES (
    'driver2',
    'driver2@swiftlogistics.lk',
    '$2b$12$FF8ix2UVatF/Rqijh8x7aeZPAww76plYihS6vUOY2x/Xu8Y4FLfa.',
    'Nimal Silva',
    '+94773333333',
    'driver'
) ON CONFLICT (username) DO NOTHING;

-- Seed client
INSERT INTO users (username, email, password_hash, full_name, phone, role)
VALUES (
    'client1',
    'client1@example.com',
    '$2b$12$FF8ix2UVatF/Rqijh8x7aeZPAww76plYihS6vUOY2x/Xu8Y4FLfa.',
    'ABC Online Store',
    '+94772222222',
    'client'
) ON CONFLICT (username) DO NOTHING;

-- Seed second client
INSERT INTO users (username, email, password_hash, full_name, phone, role)
VALUES (
    'client2',
    'client2@example.com',
    '$2b$12$FF8ix2UVatF/Rqijh8x7aeZPAww76plYihS6vUOY2x/Xu8Y4FLfa.',
    'XYZ Retail',
    '+94774444444',
    'client'
) ON CONFLICT (username) DO NOTHING;
