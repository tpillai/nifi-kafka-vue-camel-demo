# Order Pipeline POC: Vert.x · NiFi · Kafka · Camel · Spring Boot 3 · Keycloak · Postgres

A minimal but complete integration platform. One order flows through every layer:

```
Browser (Vue) ─┐                      ┌─> Kafka orders.raw ─> Camel (Spring Boot 3) ─> Postgres
Partner API  ──┼─> Vert.x gateway ─> NiFi ┤                        │  dedupe, enrich, route, DLT
Partner CSV  ──┼──────────────────> NiFi ┘                        └─> Kafka orders.processed ─> gateway ─> SSE ─> browser
Frankfurter FX API (public) ─> NiFi ─> Kafka fx.rates ─> Camel ─> fx_rates
                 Keycloak (OAuth2 / OIDC) guards the gateway and the API
```

- **Vert.x 5** gateway and BFF:
  - serves the Vue 3 UI
  - browser login with OIDC code flow + PKCE
  - checks Bearer JWTs for machine clients, plus roles, rate limiting and edge validation
  - returns async `202` responses
  - pushes Kafka events to the browser over SSE via the event bus
- **Apache NiFi 2** ingestion:
  - HTTP intake from the gateway
  - CSV file pickup, converted to JSON lines
  - polling of a public API
  - structural validation, provenance and backpressure
- **Kafka 4** (KRaft) backbone:
  - keyed by customer for per-customer ordering
  - dead-letter, invalid and compacted reference-data topics
- **Apache Camel 4** in **Spring Boot 3.5**:
  - idempotent consumer, content enricher (customer + FX), content-based router, wire tap, dead letter channel with retry/backoff
- **Spring Boot 3** read API: an OAuth2 resource server (re-validates the token) documented with springdoc/OpenAPI.
- **Keycloak 26:** realm with users, roles, a confidential BFF client and a client-credentials partner client.
- **Postgres 17:** orders, customers, FX rates, and the idempotency store.
- **Data sources:**
  - a live **public** FX feed (Frankfurter/ECB)
  - a **partner simulator** sending orders over the secured API and as CSV file drops, with about 10% deliberately bad records

## Run it

> **Full guide: [how2run.md](how2run.md)** covers prerequisites, a five-minute demo, CLI usage, the generator, resets and troubleshooting.

Needs Docker with about 6 GB of memory available.

```bash
docker compose up -d --build --wait
```

The first build downloads Maven dependencies, so allow about 5 minutes. Then:

| What | URL | Login |
|---|---|---|
| **UI** | http://localhost:8080 | `alice / alice` (submit + read), `bob / bob` (read only) |
| Gateway API docs | http://localhost:8080/docs.html | Bearer token, see below |
| Integration service API docs | http://localhost:8081/swagger-ui.html | Bearer token |
| NiFi | https://localhost:8443/nifi | `admin / adminadmin123` (self-signed certificate) |
| Keycloak admin | http://localhost:8180 | `admin / admin` |
| Postgres | `localhost:5433/orders` | `orders / orders` |
| Kafka | `localhost:9094` | none |

```bash
scripts/get-token.sh partner      # or alice / bob
scripts/e2e.sh                    # run all test scenarios (about 4 minutes)
docker compose logs -f integration-service generator
docker compose down -v            # stop and wipe all data
```

The generator starts sending traffic as soon as the stack is healthy. To slow it down or turn it off:

```bash
GENERATOR_API_INTERVAL_MS=0 GENERATOR_FILE_INTERVAL_MS=0 docker compose up -d generator
```

## Documentation

| Doc | Contents |
|---|---|
| [how2run.md](how2run.md) | Step-by-step run guide, demo script, troubleshooting |
| [docs/architecture.md](docs/architecture.md) | Component diagram, happy-path sequence, failure paths, NiFi flow, security model, data model, ports and start-up order |
| [docs/design-decisions.md](docs/design-decisions.md) | 15 ADRs (async 202, idempotency key, partition key, NiFi vs Camel, BFF, token passthrough, error classification, versions) plus the POC shortcuts and their production answers |
| [docs/test-scenarios.md](docs/test-scenarios.md) | 14 automated scenarios and manual walkthroughs (UI tour, NiFi backpressure and provenance, Kafka CLI, tokens) |
| [docs/api.md](docs/api.md) | REST endpoints, Kafka topic contracts and headers, partner CSV format |

## Repository layout

```
docker-compose.yml          all services, health-checked start-up order
gateway/                    Vert.x 5 gateway + Vue UI (webroot/) + hand-written openapi.yaml
integration-service/        Spring Boot 3.5 + Camel 4.14: routes/, api/, config/, persistence/
generator/                  partner simulator (JDK only)
infra/postgres/init.sql     schema and seed data
infra/keycloak/             realm export (users, roles, clients, audience mapper)
infra/nifi/provision_flow.py builds the NiFi flow through its REST API
scripts/e2e.sh              end-to-end scenarios
data/inbox/                 partner CSV drop folder (mounted into NiFi)
```

## Verified

On 2026-10-07, from a cold start (`docker compose down -v && docker compose up -d --build --wait`) on Docker Desktop with 8 GB and 8 CPUs, `scripts/e2e.sh` gave **61 passed, 0 failed**, covering all 14 scenarios including the resilience ones. Details are in [docs/test-scenarios.md](docs/test-scenarios.md).
