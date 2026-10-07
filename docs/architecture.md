# Architecture

One business flow, **order intake**, runs through every layer. An order can come from three places:

| Channel | Who | Path into the pipeline |
|---|---|---|
| Web UI | A person logged in through Keycloak (alice, bob) | Browser → Vert.x gateway (session) → NiFi ListenHTTP |
| Partner API | A machine client (`partner-client`, OAuth2 client credentials), simulated by the generator | Generator → Vert.x gateway (Bearer JWT) → NiFi ListenHTTP |
| Partner file drop | A partner dropping CSV batches (stands in for SFTP), simulated by the generator | `./data/inbox/*.csv` → NiFi GetFile |

A fourth feed carries **public reference data**. NiFi polls the [Frankfurter API](https://frankfurter.dev) (ECB exchange rates) every minute. Camel then uses those rates to convert EUR, GBP and INR orders to USD before checking credit limits.

## 1. Component view

```mermaid
flowchart TB
    subgraph clients[External sources]
        browser[Browser<br/>Vue 3 UI]
        partner[Partner API client<br/>generator, client credentials]
        files[Partner CSV drop<br/>generator writes ./data/inbox]
        fx[Frankfurter public API<br/>ECB FX rates]
    end

    subgraph edge[Edge]
        gw[Vert.x gateway :8080<br/>BFF + API gateway<br/>JWT check, roles, rate limit, SSE]
        kc[Keycloak :8180<br/>realm orders-poc<br/>code+PKCE, client credentials]
    end

    subgraph ingest[Ingestion - Apache NiFi :8443]
        listen[ListenHTTP :9090/orders]
        getfile[GetFile → ConvertRecord → SplitText]
        validate[EvaluateJsonPath → RouteOnAttribute]
        invoke[InvokeHTTP every 60 s]
    end

    subgraph backbone[Kafka backbone - KRaft]
        raw[(orders.raw<br/>3 partitions, key=customerId)]
        processed[(orders.processed)]
        dlt[(orders.raw.DLT)]
        invalid[(orders.invalid)]
        fxt[(fx.rates<br/>compacted)]
    end

    subgraph proc[Processing - Spring Boot 3 + Camel :8081]
        camel[Camel routes<br/>idempotent consumer, enricher,<br/>content-based router, wire tap, DLC]
        api[Order query API<br/>resource server, OpenAPI]
    end

    pg[(Postgres :5433<br/>orders, customers, fx_rates)]

    browser -- session cookie --> gw
    partner -- Bearer JWT --> gw
    gw -. login redirect / token exchange / JWKS .- kc
    api -. JWKS .- kc
    gw -- POST order JSON --> listen
    files --> getfile
    listen --> validate
    getfile --> validate
    validate -- valid --> raw
    validate -- invalid --> invalid
    fx --> invoke --> fxt
    raw --> camel
    fxt --> camel
    camel --> pg
    camel --> processed
    camel -- poison / business error / retries exhausted --> dlt
    gw -- GET with user token --> api
    api --> pg
    processed --> gw
    dlt --> gw
    invalid --> gw
    gw -- SSE --> browser
```

**Who owns what**

| Component | Responsibility | Does *not* do |
|---|---|---|
| Vert.x gateway | Authentication (session or Bearer), coarse authorisation (roles), edge validation, rate limiting, minting `orderId`, async `202`, proxying reads, pushing events | Business rules, persistence |
| NiFi | Protocol and format adaptation (HTTP, files, CSV→JSON, polling a public API), structural validation, provenance, backpressure | Business logic |
| Kafka | Durable buffer that decouples ingestion from processing, ordering per customer, replay | Transformation |
| Camel (in Spring Boot) | Integration patterns: dedupe, enrich, route, error handling, publish results | Serving HTTP reads |
| Spring Boot API | Read model over Postgres, re-validating the JWT (defence in depth) | Writes |
| Keycloak | Issues tokens, holds users, roles and clients | |

## 2. Happy path (sequence)

```mermaid
sequenceDiagram
    autonumber
    actor U as alice (browser)
    participant GW as Vert.x gateway
    participant KC as Keycloak
    participant NF as NiFi
    participant K as Kafka
    participant C as Camel route
    participant DB as Postgres
    participant API as Spring API

    U->>GW: GET /login
    GW->>U: 302 to Keycloak (code_challenge, S256)
    U->>KC: credentials
    KC->>U: 302 /callback?code=...
    U->>GW: GET /callback?code
    GW->>KC: code + code_verifier + client secret (back channel)
    KC-->>GW: access + id token (kept in server-side session)

    U->>GW: POST /api/orders {C-100, 120.50 EUR}
    GW->>GW: verify JWT locally (JWKS), role orders-write, rate limit, shape checks
    GW->>GW: mint orderId (UUID), add submittedBy / source / receivedAt
    GW->>NF: POST :9090/orders
    NF-->>GW: 200
    GW-->>U: 202 Accepted {orderId}
    NF->>NF: EvaluateJsonPath, RouteOnAttribute (valid)
    NF->>K: orders.raw key=C-100
    K->>C: consume (group integration-service)
    C->>DB: idempotency check (camel_messageprocessed)
    C->>DB: enrich: customer (GOLD, limit) + fx_rates (EUR)
    C->>C: route: within limit and GOLD, so APPROVED / EXPRESS
    C->>DB: INSERT orders ... ON CONFLICT DO NOTHING
    C->>K: orders.processed key=C-100
    K->>GW: consume (group gateway-host)
    GW-->>U: SSE event: processed
    U->>GW: GET /api/orders (session)
    GW->>API: GET /api/orders, Authorization: Bearer user token
    API->>API: validate JWT again (sig, iss, aud, role)
    API->>DB: SELECT
    API-->>GW: 200 JSON
    GW-->>U: 200 JSON
```

## 3. Failure paths

```mermaid
flowchart LR
    in[Message on orders.raw] --> parse{JSON parses?}
    parse -- no --> dltA[DLT immediately<br/>x-error-type=JsonParseException]
    parse -- yes --> val{Business-valid?<br/>UUID, qty, amount, priority}
    val -- no --> dltB[DLT immediately<br/>InvalidOrderException]
    val -- yes --> dup{orderId seen before?}
    dup -- yes --> drop[Log + skip<br/>no second row]
    dup -- no --> enrich{Customer and currency known?}
    enrich -- no --> dltC[DLT immediately<br/>UnknownCustomerException]
    enrich -- DB down --> retry[Redeliver 3x<br/>1 s, 2 s, 4 s backoff] --> dltD[DLT<br/>CannotCreateTransactionException]
    enrich -- yes --> route{Amount USD > credit limit?}
    route -- yes --> rej[REJECTED, lane NONE]
    route -- no --> exp{HIGH priority or GOLD?}
    exp -- yes --> e[APPROVED, EXPRESS]
    exp -- no --> s[APPROVED, STANDARD]
    rej & e & s --> save[(Postgres)] --> out[orders.processed]
```

Before Kafka, failures are handled at the edge:

| Where | Failure | Outcome |
|---|---|---|
| Gateway | No or invalid token | `401` problem+json |
| Gateway | Missing role | `403` |
| Gateway | Bad shape (customerId format, quantity, currency) | `400`, nothing is sent downstream |
| Gateway | More than 30 orders per minute per caller | `429` with `Retry-After` |
| Gateway | NiFi down or ListenHTTP applying backpressure | `503`, caller retries |
| NiFi | CSV row without `orderId` or `customerId`, unparseable CSV, non-JSON | Routed to `orders.invalid`, visible in provenance |
| NiFi | Kafka unavailable | PublishKafka `failure` loops back with a 5 s penalty. The queue fills and backpressure stops upstream processors, so nothing is lost. |

## 4. NiFi flow (process group "Order Ingestion")

```mermaid
flowchart TB
    L[ListenHTTP<br/>:9090 /orders] --> E
    G[GetFile<br/>/opt/nifi/inbox/*.csv] --> CR[ConvertRecord<br/>CSVReader → JSON lines] --> ST[SplitText<br/>1 line per FlowFile] --> E
    E[EvaluateJsonPath<br/>order.id, customer.id → attributes] --> R{RouteOnAttribute<br/>both non-empty?}
    R -- valid --> P1[PublishKafka<br/>orders.raw, key=customer.id]
    R -- unmatched --> P2[PublishKafka<br/>orders.invalid]
    E -- failure --> P2
    CR -- failure --> P2
    I[InvokeHTTP GET Frankfurter<br/>every 60 s] -- Response --> P3[PublishKafka<br/>fx.rates, key=USD]
    P1 -- failure --> P1
    P2 -- failure --> P2
    P3 -- failure --> P3
```

`infra/nifi/provision_flow.py` builds this flow through the REST API on first start. Open https://localhost:8443/nifi (admin / adminadmin123) to watch queues, open provenance, or stop a processor and see backpressure build.

## 5. Security model

```mermaid
flowchart LR
    subgraph Keycloak realm orders-poc
        r1[role orders-read]
        r2[role orders-write]
        alice((alice)) --> r1 & r2
        bob((bob)) --> r1
        sa((service-account-partner-client)) --> r1 & r2
        c1[client gateway-ui<br/>confidential, code + PKCE]
        c2[client partner-client<br/>client credentials]
    end
    c1 & c2 -- audience mapper --> aud[aud = orders-api]
```

| Check | Gateway (Vert.x) | Integration service (Spring) |
|---|---|---|
| Signature | JWKS fetched from `keycloak:8080` at start-up | JWKS from `keycloak:8080` |
| `iss` | must equal `http://localhost:8180/realms/orders-poc` | same |
| `aud` | must contain `orders-api` | same (custom validator) |
| `exp` | yes (5 s leeway) | yes |
| Roles | `orders-write` for POST, `orders-read` for GET and SSE | `ROLE_orders-read` for GET, everything else denied |

Keycloak runs with `KC_HOSTNAME=http://localhost:8180` and `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`. The issuer is therefore the browser-facing URL, while containers can still reach the token and JWKS endpoints at `keycloak:8080`. See ADR-007.

## 6. Data model

```mermaid
erDiagram
    customers ||--o{ orders : places
    customers {
        varchar customer_id PK
        varchar name
        varchar tier "GOLD SILVER BRONZE"
        numeric credit_limit "USD"
    }
    orders {
        uuid order_id PK "idempotency key"
        varchar customer_id FK
        varchar customer_name "enriched"
        varchar customer_tier "enriched"
        numeric amount
        char currency
        numeric fx_rate "per 1 USD, at processing time"
        numeric amount_usd
        varchar status "APPROVED REJECTED"
        varchar lane "EXPRESS STANDARD NONE"
        varchar source "web-ui partner-api file-drop"
        timestamptz received_at
        timestamptz processed_at
    }
    fx_rates {
        char currency PK
        numeric rate_per_usd
        date as_of
        varchar source "seed frankfurter"
    }
    camel_messageprocessed {
        varchar processorname PK
        varchar messageid PK
        timestamp createdat
    }
```

## 7. Runtime and ports

| Service | Image or build | Host port | Memory |
|---|---|---|---|
| gateway | `./gateway` (Vert.x 5.2, Java 21) | 8080 | 512 MB limit |
| integration-service | `./integration-service` (Spring Boot 3.5, Camel 4.14 LTS, Java 21) | 8081 | 512 MB limit |
| generator | `./generator` (plain JDK 21) | none | 256 MB limit |
| nifi + nifi-setup | `apache/nifi:2.12.0`, `python:3.13-alpine` | 8443 | 1 GB heap |
| kafka + kafka-init | `apache/kafka:4.3.1` (KRaft, no ZooKeeper) | 9094 | 512 MB heap |
| keycloak | `quay.io/keycloak/keycloak:26.8.0` (dev mode) | 8180 | 512 MB heap |
| postgres | `postgres:17-alpine` | 5433 | none |

Start-up order is enforced by health checks:

```mermaid
flowchart LR
    pg[postgres] --> is[integration-service]
    k[kafka] --> ki[kafka-init topics] --> is
    ki --> ns[nifi-setup]
    kc[keycloak] --> is --> gw[gateway] --> gen[generator]
    n[nifi] --> ns --> gen
    kc --> gw
```
