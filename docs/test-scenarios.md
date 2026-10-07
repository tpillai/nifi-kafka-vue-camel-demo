# Test scenarios

All automated scenarios are in [`scripts/e2e.sh`](../scripts/e2e.sh). Run them against a running stack:

```bash
scripts/e2e.sh                     # everything, about 4 minutes
SKIP_RESILIENCE=1 scripts/e2e.sh   # skip the ones that stop containers
```

Requirements: `bash`, `curl`, `jq`, `docker compose`. The generator keeps adding traffic during the run, and the checks only look at the orders they create themselves.

**Last full run on a cold start:**
- `docker compose down -v && docker compose up -d --build --wait`, then `scripts/e2e.sh`.
- The first run gave 54 passed and 1 failed. The failure was the redelivery log assertion, which grepped for text that Camel prints on a wrapped line.
- That assertion has since been fixed. A cold-start re-run is recorded in the README.

## Automated (e2e.sh)

| # | Scenario | What it proves | Layers |
|---|---|---|---|
| 0 | Stack health, tokens for alice, bob, partner | Everything started; Keycloak realm imported | all |
| 1a | No token → `401`; garbage token → `401` | JWT verification at the gateway | Vert.x, Keycloak |
| 1b | bob (read-only) GET → `200`, POST → `403` | Role-based authorisation from realm roles | Vert.x |
| 1c | Call the Spring API directly without a token → `401`; with a token → `200`; POST → `403` | Defence in depth: the service does not trust the network | Spring Security |
| 1d | `/login` → `302` to Keycloak with `code_challenge_method=S256` | Browser login is code flow + PKCE | Vert.x, Keycloak |
| 2 | Bad customerId, quantity 0, JPY, non-JSON → `400` | Edge validation; nothing reaches NiFi | Vert.x |
| 3 | alice orders 120.50 EUR for C-100 → `202`, then the order appears: APPROVED, EXPRESS, enriched name, `source=web-ui`, `submittedBy=alice`, `amountUsd = amount / fxRate`, row in Postgres, event on `orders.processed` | The full happy path through every layer | all |
| 4 | SILVER + HIGH → EXPRESS; SILVER + NORMAL → STANDARD; BRONZE 5000 USD → REJECTED with reason; partner orders tagged `partner-api` | Content-based router; credit check on converted amount | Camel |
| 5 | Unknown customer C-999 → `202`, then in `orders.raw.DLT` with `x-error-type:UnknownCustomerException`, no DB row, API `404` | Business errors skip retries and go to the DLT with error headers | Camel, Kafka |
| 6 | Malformed JSON produced straight to `orders.raw` → DLT with `JsonParseException` | Poison messages do not block the partition | Camel |
| 7 | Re-produce an already-processed order twice → "Duplicate order … ignored", still one row | Idempotent consumer under at-least-once delivery | Camel, Postgres |
| 8 | CSV with 2 valid rows and 1 row missing customerId dropped into `data/inbox` → 2 rows processed as `file-drop`, 1 in `orders.invalid`, not in Postgres | File ingestion, CSV→JSON, split, structural validation | NiFi |
| 9 | `fx_rates.source = frankfurter`; `/api/fx-rates` through the gateway | Public API ingestion and reference-data enrichment | NiFi, Kafka, Camel |
| 10 | Open `/api/events`, submit an order, receive `event: processed` with that id | Kafka → event bus → SSE push | Vert.x |
| 11 | Stop integration-service, submit → still `202`, consumer lag > 0, restart → order processed | Kafka decouples ingestion from processing; nothing lost | Kafka, Camel |
| 12 | Stop Postgres, submit → 3 redeliveries with backoff → DLT with `CannotCreateTransactionException` | Transient errors are retried before parking | Camel |
| 13 | Burst of POSTs → `429` | Per-caller rate limiting | Vert.x |

## Manual walkthroughs

These demo well and are easiest to explain live.

### A. The UI tour (2 minutes)
1. Open http://localhost:8080 and log in as **alice / alice**.
2. Submit the default order (C-100, 120.50 EUR). The three stage chips turn green, and the order appears in the table with its USD amount.
3. Choose **C-999** and submit. The last chip turns red ("Camel → DLT"), and a `DLT` line appears in the live feed.
4. Choose **C-300** with amount 5000 USD. The order is `REJECTED`; hover the row to see the reason.
5. Watch the feed: generator orders arrive every 5 s (`partner-api`), and CSV batches about every 45 s (`file-drop`), including the occasional `REJECTED BY NIFI` row.
6. Log out, then log in as **bob / bob** and submit. You get `403 … lacks role orders-write`.

### B. NiFi backpressure and provenance
1. Open https://localhost:8443/nifi and log in as **admin / adminadmin123**. Accept the self-signed certificate.
2. Open the **Order Ingestion** group and right-click **Publish to orders.raw** → Stop.
3. Watch the queue in front of it grow as generator traffic arrives.
4. Right-click the queue → List queue, and open a FlowFile to see its attributes (`order.id`, `customer.id`) and content.
5. Start the processor again. The queue drains and the orders appear in the UI. Kafka and Camel never knew there was a pause.
6. Global menu → **Data Provenance**: search by FlowFile attribute or filename and open the lineage graph to follow one order from RECEIVE to SEND.
7. To see ListenHTTP's `503` backpressure, set the queue's object threshold to 5 while the publisher is stopped, then submit from the UI. The gateway answers `503 Ingestion is busy`.

### C. Kafka from the command line
```bash
# Topic offsets
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092

# Watch processed orders live
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic orders.processed --property print.key=true

# Dead letters with error headers
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic orders.raw.DLT --from-beginning --property print.headers=true --timeout-ms 5000

# Consumer lag of the Camel route
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group integration-service
```

### D. Token inspection
```bash
scripts/get-token.sh partner | cut -d. -f2 | base64 -d 2>/dev/null | jq '{iss, aud, azp, realm_access}'
```
The token has `iss = http://localhost:8180/realms/orders-poc`, `aud` includes `orders-api`, and `realm_access.roles` lists the roles. Paste the token into Swagger UI at http://localhost:8081/swagger-ui.html (Authorize) to call the read API directly.

### E. Swap the FX source offline
Block outbound traffic, or stop the **Poll Frankfurter FX rates** processor. Orders keep converting with the last known rates; `source` stays `frankfurter`, or `seed` on a fresh database.

## Not covered (would be next)

- Load test: throughput versus partition count and consumer concurrency (Camel `consumersCount`).
- Schema evolution with a Schema Registry (Avro with BACKWARD compatibility) instead of free-form JSON.
- DLT replay tool and alerting on DLT growth or consumer lag.
- Camel route unit tests with `CamelSpringBootTest` and `AdviceWith`, and Testcontainers-based integration tests.
- Token expiry and refresh during a long browser session.
