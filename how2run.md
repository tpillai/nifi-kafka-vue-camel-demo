# How to run the Order Pipeline POC

## 1. Prerequisites

| Need | Why | Check |
|---|---|---|
| Docker Desktop (or Docker Engine with Compose v2) | Runs every component | `docker compose version` |
| About 6 GB of memory for Docker (8 GB recommended) | NiFi, Kafka, Keycloak and two JVMs | Docker Desktop → Settings → Resources |
| Free host ports 8080, 8081, 8180, 8443, 9094, 5433 | Published service ports | `lsof -i :8080` etc. |
| Internet access on the first start | Pulls images and Maven dependencies. The FX feed also calls api.frankfurter.dev, but it falls back to seed rates if offline. | |
| `curl` and `jq` (optional) | Only for `scripts/get-token.sh` and `scripts/e2e.sh` | `jq --version` |

You do **not** need Java or Maven on your machine. Both services are built inside Docker (multi-stage builds on `maven:3.9-eclipse-temurin-21`).

## 2. Start everything

From the repository root:

```bash
docker compose up -d --build --wait
```

- `--build` compiles the gateway, the integration service and the generator.
- `--wait` returns once every service is healthy.
- The first run takes about 5–8 minutes (image pulls, Maven downloads, NiFi start-up). Later runs take about 2–4 minutes.

The services start in this order, enforced by health checks:

1. postgres, kafka and keycloak start.
2. `kafka-init` creates the topics and exits.
3. nifi starts, then `nifi-setup` builds the NiFi flow through its REST API and exits.
4. integration-service, gateway and generator start.

Check the state:

```bash
docker compose ps -a
```

The expected end state:
- `kafka-init` and `nifi-setup` show **Exited (0)**. They are one-shot jobs, so this is normal.
- Every other service shows **Up (healthy)**, apart from the generator, which has no health check.

## 3. Open it

| What | URL | Login |
|---|---|---|
| **Web UI** | http://localhost:8080 | `alice / alice` (submit + read) or `bob / bob` (read only) |
| Gateway API docs (Swagger UI) | http://localhost:8080/docs.html | Paste a token from `scripts/get-token.sh` |
| Integration service API docs | http://localhost:8081/swagger-ui.html | Same |
| NiFi | https://localhost:8443/nifi | `admin / adminadmin123`; accept the self-signed certificate |
| Keycloak admin console | http://localhost:8180 | `admin / admin` (realm `orders-poc`) |
| Postgres | `localhost:5433`, database `orders` | `orders / orders` |
| Kafka (from the host) | `localhost:9094` | none |

## 4. A five-minute demo

1. **Log in:** open http://localhost:8080, click **Log in** and sign in as `alice / alice`. You are redirected through Keycloak (code flow + PKCE) and back.
2. **Happy path:** submit the default order (C-100, 120.50 EUR).
   - The three stage chips turn green: Gateway 202 → NiFi → Kafka → Camel → Postgres.
   - The order appears in the table, converted to USD with the live FX rate.
3. **Dead letter:** choose `C-999 · unknown customer` and submit. The last chip turns red and a `DLT` entry appears in the live feed.
4. **Credit rejection:** choose `C-300` (BRONZE, 1000 USD limit), set the amount to 5000 USD and submit. The status is `REJECTED`; hover the row for the reason.
5. **Background traffic:** watch the live feed. The generator sends an API order every 5 s (`partner-api`) and a CSV batch every 45 s (`file-drop`), including a few deliberately bad records.
6. **Authorisation:** log out, log in as `bob / bob` and submit. You get `403 … lacks role orders-write`.
7. **NiFi:**
   - Open https://localhost:8443/nifi and go into **Order Ingestion**.
   - Stop **Publish to orders.raw** and watch its queue fill, then start it again and watch the queue drain.
   - Use **Data Provenance** to follow one record.

## 5. Call the API from the command line

```bash
# Tokens
TOKEN=$(scripts/get-token.sh alice)        # or bob, or partner (client credentials)

# Submit (asynchronous: 202 + orderId)
curl -s -X POST http://localhost:8080/api/orders \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"customerId":"C-100","product":"Widget","quantity":2,"amount":120.50,"currency":"EUR"}'

# Read the result (404 until Camel has processed it, usually within a second)
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/orders/<orderId> | jq

# Live event stream
curl -N -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/events
```

Partner file drop: put a CSV in `data/inbox/` (format in [docs/api.md](docs/api.md#partner-csv-format-file-drop)). NiFi picks it up within 5 seconds.

## 6. Run the test scenarios

```bash
scripts/e2e.sh                       # all 14 scenarios, about 4 minutes; stops/starts containers in steps 11-12
SKIP_RESILIENCE=1 scripts/e2e.sh     # skip the container-stopping scenarios
```

The script prints PASS or FAIL for each check and exits non-zero if anything fails. Each scenario is described in [docs/test-scenarios.md](docs/test-scenarios.md).

The rate-limit check (step 13) uses up alice's 30-per-minute allowance. Wait a minute before submitting as alice again.

## 7. Control the data generator

```bash
# Slower traffic
GENERATOR_API_INTERVAL_MS=20000 GENERATOR_FILE_INTERVAL_MS=120000 docker compose up -d generator

# Turn it off
docker compose stop generator

# Watch it
docker compose logs -f generator
```

## 8. Logs and inspection

```bash
docker compose logs -f integration-service     # Camel routes: received, AUDIT, APPROVED/REJECTED, Duplicate, DLT
docker compose logs -f gateway
docker compose logs nifi-setup                 # NiFi flow provisioning

# Kafka
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic orders.raw.DLT --from-beginning --property print.headers=true --timeout-ms 5000

# Database
docker compose exec postgres psql -U orders -d orders \
  -c "select source, status, lane, count(*) from orders group by 1,2,3"
```

## 9. Stop, restart, reset

The stack uses no named volumes, so data lives only as long as the containers do.

```bash
docker compose stop          # stop, keep all data
docker compose start         # resume where you left off
docker compose down -v       # remove containers and their volumes: full reset (DB, Kafka, NiFi flow rebuilt on next up)
```

After changing Java code, rebuild just that service:

```bash
docker compose up -d --build gateway             # or integration-service, generator
```

To rebuild the NiFi flow after editing `infra/nifi/provision_flow.py`:

```bash
docker compose rm -sfv nifi nifi-setup && docker compose up -d --wait
```

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `port is already allocated` | Another process uses 8080, 8081, 8180, 8443, 9094 or 5433 | Stop it, or change the left side of `ports:` in `docker-compose.yml`. If you change 8080 or 8180, also update `PUBLIC_URL`, `KC_HOSTNAME`, the `OIDC_ISSUER` values and the redirect URIs in the realm JSON. |
| A container is restarted or `Exited (137)` | Out of memory | Give Docker more memory (8 GB recommended) |
| `nifi-setup` exited with 1 | NiFi was not ready in time, or the flow is invalid | `docker compose logs nifi-setup`, then `docker compose up -d nifi-setup` to retry |
| Gateway returns `503 Ingestion unavailable` | NiFi is still starting or the flow is not running | Wait for `nifi-setup` to finish with "Flow started" |
| Login loops or `Invalid token` | Browser opened via `127.0.0.1` instead of `localhost`, so the issuer doesn't match | Always use `http://localhost:8080` |
| FX rates show `source = seed` | NiFi cannot reach api.frankfurter.dev (offline or proxy) | Orders still work with the seed rates. Check the bulletins on the **Poll Frankfurter FX rates** processor in NiFi. |
| `Invalid SNI` errors from NiFi after editing compose | NiFi's certificate was generated for an old hostname and kept in a volume | `docker compose rm -sfv nifi nifi-setup && docker compose up -d` |
| e2e step 13 fails right after a previous run | alice's rate-limit window is still full | Wait 60 s and rerun |

## 11. Building outside Docker (optional)

Needs JDK 21 and Maven 3.9. Run each service against the dockerised infrastructure, after stopping its container:

```bash
docker compose stop integration-service gateway
(cd integration-service && mvn spring-boot:run)                       # uses localhost:5433, :9094, :8180 defaults
(cd gateway && mvn package && NIFI_INGEST_URL=http://localhost:9090/orders java -jar target/gateway.jar)
```

NiFi's ListenHTTP (port 9090) is not published to the host by design. To submit orders from a locally run gateway, add `"9090:9090"` to the `nifi` ports in `docker-compose.yml` first.
