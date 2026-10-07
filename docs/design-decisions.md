# Design decisions

Short ADRs. Each one gives the decision, why it was made, and what it costs. The "Production" notes say what would change outside a POC.

---

## ADR-001: One flow, every layer

**Decision:** Build a single business flow (order intake) that passes through every component, rather than a separate demo per technology.

**Why:** Interviewers and reviewers ask "how do these fit together?", not "can you call the Kafka API?". One flow makes each tool's responsibility and its boundaries visible.

**Cost:** The domain is deliberately thin (three customers, two lanes).

---

## ADR-002: Asynchronous write path (`202 Accepted` + poll or SSE)

**Decision:** `POST /api/orders` returns `202` with an `orderId` as soon as NiFi accepts the message. The result is read later via `GET /api/orders/{id}` or pushed over SSE.

**Why:** Ingestion stays available when processing is slow or down, because Kafka buffers. e2e scenario 11 proves this: the service is stopped and orders are still accepted. A synchronous design would couple the gateway's availability to Camel, Postgres and Keycloak.

**Cost:** Clients must handle "accepted but later rejected". That is why the UI tracks each order's stages and listens for `dead-letter` events.

---

## ADR-003: The gateway mints `orderId`, which becomes the idempotency key

**Decision:** The gateway generates a UUID per order. File-drop partners supply their own UUID per row. Camel's idempotent consumer (`JdbcMessageIdRepository`) and the `orders.order_id` primary key (`ON CONFLICT DO NOTHING`) both key on it.

**Why:** Kafka delivery is at-least-once, so redeliveries and partner resends will happen. Two layers of defence: the repository skips the work, and the primary key guarantees one row even if the repository is bypassed. If processing fails and the message lands in the DLT, Camel removes the key, so a DLT replay is not treated as a duplicate.

**Cost:** The processed-message table grows. Production would purge old keys on a schedule.

---

## ADR-004: Kafka key is `customerId`, not `orderId`

**Decision:** NiFi publishes to `orders.raw` with key `customerId`, and Camel uses the same key for `orders.processed`.

**Why:** Ordering only holds within a partition. Keying by customer means all orders for one customer are processed in order, which matters once a credit check depends on earlier orders. Keying by `orderId` would spread load more evenly but lose that guarantee.

**Cost:** Hot customers produce hot partitions. With three demo customers, one of the three partitions stays empty, and you can see it in `kafka-get-offsets`.

---

## ADR-005: NiFi at the edge, Camel in the middle

**Decision:**
- NiFi handles protocol and format adaptation: HTTP intake, CSV file pickup, CSV to JSON conversion, polling a public API, and structural validation.
- Camel handles business integration patterns: dedupe, enrich, route, error handling.

**Why:**

| | NiFi | Camel |
|---|---|---|
| Strength | Many source protocols, visual flow ops can change, provenance, backpressure | Code-first EIPs, unit-testable, versioned with the service |
| Weakness | Business logic in processors is hard to test and review | No built-in ops UI for data in flight |

Ops can add a new partner feed in NiFi without a service release. Business rules change through code review.

**Cost:** Two integration tools to run and learn. If there were only one source (HTTP), NiFi would be overkill: the gateway could publish to Kafka directly.

---

## ADR-006: Camel runs inside the Spring Boot service

**Decision:** The Camel routes and the read API share one Spring Boot 3.5 application (`camel-spring-boot-starter`).

**Why:** Shared datasource and config, one deployable, and a common enterprise pattern. Memory also matters: the whole stack has to fit in 8 GB of Docker Desktop.

**Cost:** The read and write sides scale together. **Production:** split into an `order-processor` (Camel, scaled to the partition count) and an `order-query` service.

---

## ADR-007: Keycloak issuer vs in-network URLs

**Decision:**
- Keycloak runs with `KC_HOSTNAME=http://localhost:8180` and `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`.
- Both resource servers fetch the JWKS from `http://keycloak:8080/...` and validate `iss` against the public URL explicitly.
- Neither uses `issuer-uri` discovery.

**Why:** The browser and the containers reach Keycloak under different hostnames. Without this, tokens issued to the browser carry `iss=localhost:8180`, while services that discover via `keycloak:8080` expect `iss=keycloak:8080`, so every token fails validation. This is the most common Docker plus OIDC trap.

**Cost:** The issuer is configured in two places. In production there is one DNS name for Keycloak, and this goes away.

---

## ADR-008: The gateway is a BFF for the browser and a gateway for machines

**Decision:**
- Browser login uses authorization code + PKCE. Vert.x keeps the tokens in its server-side session and the browser only holds a session cookie.
- Machine clients send `Authorization: Bearer`.
- Both paths go through the same role checks.

**Why:** Tokens in browser storage are exposed to XSS, which is why OAuth 2.1 and current guidance recommend the BFF pattern for SPAs. Machine clients need stateless bearer auth.

**Cost:**
- The session store is in-memory (`LocalSessionStore`), so the gateway is not horizontally scalable as-is.
- Tokens are not refreshed, so users log in again after 30 minutes.

**Production:**
- Use a clustered or Redis session store.
- Refresh tokens with `user.refresh()`.
- Enable CSRF protection for cookie-authenticated POSTs. It is not enabled in this POC.

---

## ADR-009: Token passthrough for reads; Spring validates again

**Decision:** The gateway forwards the caller's own access token to the integration service. That service validates signature, issuer, audience and role itself. Its port 8081 is published so this can be tested.

**Why:**
- **Defence in depth:** a request that bypasses the gateway is still rejected. e2e scenario 1 checks this.
- **Identity:** the downstream service sees the real user.

**Alternatives:**
- Token exchange (RFC 8693) to down-scope the token per service.
- The gateway using its own client-credentials token, which loses the user identity.

Either would be the next step if services called each other further down.

---

## ADR-010: Validation in three places, each with a different job

| Layer | Checks | Why there |
|---|---|---|
| Gateway | Shape: id format, types, ranges, enums | Fail fast with a `400` the caller can fix; garbage never enters the pipeline |
| NiFi | Structure: required ids present, parseable | File drops never pass the gateway, so NiFi is their first checkpoint |
| Camel | Business: customer exists, currency supported, credit limit | Needs reference data; this is the only layer that has it |

---

## ADR-011: Error handling: classify, then retry or park

**Decision:**
- Poison and business errors (`JsonProcessingException`, `InvalidOrderException`, `UnknownCustomerException`) go straight to `orders.raw.DLT`.
- Everything else (database or broker trouble) is redelivered three times with exponential backoff (1 s, 2 s, 4 s), then parked.
- DLT records carry `x-error-type`, `x-error-message` and `x-source-topic` headers, plus the **original** message (`useOriginalMessage`) so a replay is exact.

**Why:** Retrying a malformed message wastes time and blocks the partition. Not retrying a transient outage loses orders that would have succeeded.

**Known trade-off:** During a long database outage, every consumed order ends up in the DLT (e2e scenario 12). For infrastructure outages, a better production design is to **pause the consumer** (Camel's `breakOnFirstError` or a circuit breaker on the route) and let Kafka hold the backlog. The DLT should be kept for genuinely bad messages. A replay tool (DLT → `orders.raw`) is also missing.

---

## ADR-012: Public data source: Frankfurter FX rates, via NiFi, as a compacted topic

**Decision:**
- NiFi's InvokeHTTP polls `https://api.frankfurter.dev/v1/latest?base=USD` every 60 s and publishes the payload to `fx.rates`, a compacted topic keyed `USD`.
- A separate Camel route upserts the rates into `fx_rates`.
- Seed rates in `init.sql` keep the system working offline.

**Why:**
- The source is free, needs no API key, is backed by the ECB, and adds a real enrichment step (currency conversion before the credit check).
- The compacted topic keeps only the latest snapshot.
- The separate route has its own error handler, so a bad rate snapshot never lands in the orders DLT.

**Cost:** The ECB publishes once per business day, so polling every minute is more than needed. It just makes the feed visible during a demo.

---

## ADR-013: Generated data instead of a second public source

**Decision:** A dependency-free Java simulator (`generator/`) acts as two partners:
- an OAuth2 client-credentials API client, one order every 5 s;
- a CSV file dropper, one batch of 5 orders every 45 s.

About 10% of records are deliberately bad (unknown customer, missing customerId, resent rows).

**Why:** Public order data does not exist. Generated traffic gives continuous load and exercises every error path, so the UI's live feed, the DLT and the dedupe logs always have something to show. The rates can be set with `GENERATOR_API_INTERVAL_MS` and `GENERATOR_FILE_INTERVAL_MS`; set either to `0` to disable that channel.

---

## ADR-014: Infrastructure is code, including the NiFi flow

**Decision:**
- Topics are created by `kafka-init`, and broker auto-create is off.
- The Keycloak realm is imported from JSON.
- The Postgres schema comes from `init.sql`.
- The NiFi flow is built by `provision_flow.py` through the NiFi REST API.

**Why:** `docker compose up` on a clean machine gives the same system every time, with no UI clicking. The script is readable and diff-able, and building through the REST API is the same approach a CI pipeline would use.

**Production:** NiFi Registry, or the Git-based flow registry in NiFi 2.x, with parameter contexts per environment instead of a provisioning script.

---

## ADR-015: Versions

| Choice | Version | Reason |
|---|---|---|
| Spring Boot | 3.5.x | The role asks for Boot 3. 3.5 is the last 3.x line; 4.x exists but would change the brief. |
| Camel | 4.14.x LTS | The LTS line built against Boot 3.5. Newer 4.18+ lines target Boot 4. |
| Vert.x | 5.2 | Current major. Futures-only API (no callbacks). |
| Kafka | 4.3 | KRaft only, no ZooKeeper. |
| NiFi | 2.12 | NiFi 2.x: Java 21, Kafka 3 connection service. HTTPS is mandatory. |
| Keycloak | 26.8 | Current. Uses the `KC_HOSTNAME` v2 options. |
| Java | 21 | LTS, and supports virtual threads should we switch Spring MVC to them. |

---

## Shortcuts taken on purpose (POC only)

| Shortcut | Production answer |
|---|---|
| NiFi ListenHTTP on :9090 has no auth. It is reachable only inside the Docker network (port not published); the gateway is the only HTTP entry. | mTLS between the gateway and NiFi, or the gateway publishing to Kafka directly |
| Kafka is PLAINTEXT, with a single broker and replication factor 1 | SASL/OAUTHBEARER or mTLS, topic ACLs per service, RF=3, `min.insync.replicas=2` |
| Keycloak in dev mode with H2 and published secrets | Production mode, Postgres-backed, secrets from a vault |
| Password grant enabled on `gateway-ui`, for test scripts only | Disabled; tests use client credentials or a headless browser |
| In-memory session store and rate limiter | Redis or a clustered store; rate limiting at an API gateway or WAF |
| No CSRF token on cookie-authenticated POST | `CSRFHandler`, or a SameSite=strict cookie plus an origin check |
| No tracing | Micrometer Tracing / OpenTelemetry, with W3C trace context in HTTP headers and Kafka record headers |
| NiFi single-user auth, self-signed certificate | OIDC login to NiFi via Keycloak, proper certificates |
