-- Schema for the order POC. Runs once, on first start of an empty volume.

CREATE TABLE customers (
    customer_id   VARCHAR(32)    PRIMARY KEY,
    name          VARCHAR(128)   NOT NULL,
    tier          VARCHAR(16)    NOT NULL CHECK (tier IN ('GOLD', 'SILVER', 'BRONZE')),
    credit_limit  NUMERIC(12, 2) NOT NULL              -- in USD
);

-- Latest exchange rates, fed by NiFi polling the public Frankfurter (ECB) API -> Kafka fx.rates -> Camel.
CREATE TABLE fx_rates (
    currency      CHAR(3)        PRIMARY KEY,
    rate_per_usd  NUMERIC(14, 6) NOT NULL,              -- 1 USD = rate_per_usd units of currency
    as_of         DATE           NOT NULL,
    source        VARCHAR(32)    NOT NULL,              -- seed | frankfurter
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- Fallback rates so the POC works offline; overwritten as soon as the live feed arrives.
INSERT INTO fx_rates (currency, rate_per_usd, as_of, source) VALUES
    ('USD', 1.000000, DATE '2026-01-01', 'seed'),
    ('EUR', 0.900000, DATE '2026-01-01', 'seed'),
    ('GBP', 0.760000, DATE '2026-01-01', 'seed'),
    ('INR', 95.000000, DATE '2026-01-01', 'seed');

CREATE TABLE orders (
    order_id       UUID           PRIMARY KEY,          -- idempotency key, minted by the gateway
    customer_id    VARCHAR(32)    NOT NULL REFERENCES customers (customer_id),
    customer_name  VARCHAR(128)   NOT NULL,             -- enriched by Camel
    customer_tier  VARCHAR(16)    NOT NULL,             -- enriched by Camel
    product        VARCHAR(128)   NOT NULL,
    quantity       INT            NOT NULL CHECK (quantity > 0),
    amount         NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    currency       CHAR(3)        NOT NULL,
    amount_usd     NUMERIC(12, 2) NOT NULL,             -- converted by Camel using fx_rates
    fx_rate        NUMERIC(14, 6) NOT NULL,             -- units of currency per 1 USD at processing time
    priority       VARCHAR(8)     NOT NULL,
    status         VARCHAR(16)    NOT NULL,             -- APPROVED | REJECTED
    lane           VARCHAR(16)    NOT NULL,             -- EXPRESS | STANDARD | NONE
    reason         VARCHAR(256),
    source         VARCHAR(32)    NOT NULL,             -- web-ui | partner-api | file-drop
    submitted_by   VARCHAR(128),
    received_at    TIMESTAMPTZ    NOT NULL,
    processed_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX orders_customer_idx ON orders (customer_id);
CREATE INDEX orders_processed_idx ON orders (processed_at DESC);

INSERT INTO customers (customer_id, name, tier, credit_limit) VALUES
    ('C-100', 'Acme Corp',        'GOLD',   50000.00),
    ('C-200', 'Globex Ltd',       'SILVER', 10000.00),
    ('C-300', 'Initech',          'BRONZE',  1000.00);

-- Backing table for Camel's JdbcMessageIdRepository (idempotent consumer EIP).
-- Created here because Camel's auto-create probes with a failing SELECT, which Postgres dislikes inside a transaction.
CREATE TABLE camel_messageprocessed (
    processorname VARCHAR(255) NOT NULL,
    messageid     VARCHAR(100) NOT NULL,
    createdat     TIMESTAMP    NOT NULL,
    PRIMARY KEY (processorname, messageid)
);
