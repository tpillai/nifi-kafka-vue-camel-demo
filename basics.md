# The Basics: What This Project Is, In Plain English

> The detailed docs are in `README.md` and `docs/`. This file is the "explain it like I'm new" version.

---

## 1. The one-sentence version

This is a **pretend online ordering system**. Orders come in from a few places, get checked, get priced in US dollars, get approved or rejected, get saved to a database, and show up live on a web page.

Its real purpose is to **show how several popular integration tools work together**. The business side is deliberately tiny: 3 customers and 2 delivery "lanes".

---

## 2. The restaurant analogy

Think of the system as a restaurant kitchen:

| Real tool | Restaurant role | What it actually does here |
|---|---|---|
| **Vue web page** | The menu and the table | The screen where you place an order and watch it get processed |
| **Vert.x gateway** | The front door and the waiter | The single entry point. Checks who you are, does quick sanity checks on the order, and passes it on |
| **Keycloak** | The bouncer with the guest list | Handles logins and hands out "tokens" (digital ID badges) that say who you are and what you're allowed to do |
| **NiFi** | The receiving dock | Accepts orders from different delivery methods (web, CSV files, an external website) and turns them all into one standard format |
| **Kafka** | The ticket rail in the kitchen | A queue of order tickets. If the cooks are busy or away, tickets wait safely on the rail and none get lost |
| **Camel (inside Spring Boot)** | The head chef | Does the real work: ignores duplicates, looks up the customer, converts currency, decides approve/reject and fast/normal lane |
| **Postgres** | The filing cabinet | The database that stores customers, exchange rates and finished orders |
| **Generator** | Fake customers | A small program that keeps sending sample orders (some deliberately bad) so there is always something happening |

---

## 3. The journey of one order

Here is what happens when **alice** orders 2 Widgets for 120.50 EUR:

```
 You (browser)
   │  1. Click "Submit"
   ▼
 Vert.x gateway ── "Are you logged in? Are you allowed to order? Does this look sane?"
   │  2. Gives the order an ID and replies "202 Accepted, we've got it"
   ▼
 NiFi ── "Is it shaped right? Does it have a customerId?"
   │  3. Drops it onto Kafka
   ▼
 Kafka topic "orders.raw" ── the order waits in line
   │  4. Camel picks it up
   ▼
 Camel (the head chef)
   │  5. Seen this order ID before?      → yes: ignore it (duplicate)
   │  6. Look up customer C-100           → "Acme Corp", GOLD tier
   │  7. Convert 120.50 EUR to USD        → about 134.68 USD
   │  8. Over the credit limit?           → no → APPROVED
   │  9. GOLD customer or HIGH priority?  → yes → EXPRESS lane
   ▼
 Postgres ── the order is saved
   │
   ▼
 Kafka topic "orders.processed" ── "this order is done!"
   │
   ▼
 Vert.x gateway ── pushes the news to your browser live
   │
   ▼
 Your screen updates: ✅ APPROVED · EXPRESS · $134.68
```

**Why reply "202 Accepted" first and not wait for the result?** The gateway doesn't wait for the kitchen. It says "got it" right away, and you find out the result a moment later. That means orders can still be taken even when the kitchen (Camel or the database) is down: Kafka just holds the tickets until it is back.

---

## 4. Three ways orders come in

1. **The website**: a logged-in user (alice) types an order.
2. **Partner API**: another company's computer sends orders directly, using its own machine login (`partner-client`). The generator pretends to be this partner and sends one every 5 seconds.
3. **CSV file drop**: a partner drops a spreadsheet-style `.csv` file into the `data/inbox/` folder. NiFi notices it, splits it into individual orders and sends them on. The generator drops a batch about every 45 seconds.

There is also a fourth, non-order input:

4. **Live exchange rates**: every 60 seconds NiFi asks a free public website (Frankfurter, which uses European Central Bank data) "what's 1 USD worth in EUR, GBP and INR?" and stores the answer, so currency conversion uses real rates.

---

## 5. The people (test logins)

| Who | Password | Can do |
|---|---|---|
| **alice** | alice | Read **and** submit orders |
| **bob** | bob | Read only. Submitting gives `403 Forbidden` |
| **partner-client** | (machine secret) | A computer, not a person. Submits and reads orders |
| **admin** (NiFi) | adminadmin123 | Sees the NiFi flow screen |
| **admin** (Keycloak) | admin | Manages users and roles |

---

## 6. The pretend business rules

**Customers** (stored in Postgres):

| ID | Name | Tier | Credit limit |
|---|---|---|---|
| C-100 | Acme Corp | GOLD | $50,000 |
| C-200 | Globex Ltd | SILVER | $10,000 |
| C-300 | Initech | BRONZE | $1,000 |

**Rules Camel applies:**
- Order worth more (in USD) than the customer's credit limit → **REJECTED**.
- Otherwise, if the customer is GOLD **or** the order is HIGH priority → **APPROVED, EXPRESS lane**.
- Otherwise → **APPROVED, STANDARD lane**.

**Currencies allowed:** USD, EUR, GBP, INR.

---

## 7. What happens when things go wrong

The project deliberately shows how a real system copes with bad data and outages:

| Problem | Who catches it | What happens |
|---|---|---|
| Not logged in, or a fake token | Gateway | `401 Unauthorized` |
| Logged in but not allowed (bob submitting) | Gateway | `403 Forbidden` |
| Obviously bad input (quantity 0, currency JPY, not JSON) | Gateway | `400 Bad Request`, never goes further |
| Too many orders too fast | Gateway | `429 Too Many Requests` (rate limit) |
| CSV row missing a customer ID | NiFi | Sent to the **`orders.invalid`** pile |
| Unknown customer (e.g. C-999) | Camel | Sent to the **dead-letter pile** (`orders.raw.DLT`) with a note saying why. It is not retried, because retrying wouldn't help |
| Garbage text sent straight into Kafka | Camel | Also goes to the dead-letter pile, so it doesn't block everything behind it |
| Same order arrives twice | Camel | The second copy is ignored ("duplicate"), so there is still only one row in the database |
| Database is down | Camel | Retries 3 times, waiting longer each time (1s, 2s, 4s), then puts the order on the dead-letter pile |
| Camel is completely stopped | Kafka | Orders pile up safely in Kafka and get processed when Camel comes back. Nothing is lost |

> **Dead-letter pile (DLT, "dead letter topic")**: a separate Kafka queue for messages that couldn't be processed. Think of it as the "returned mail" tray. Someone can look at it, fix the cause, and replay the messages later.

---

## 8. Kafka "topics" (the queues)

A topic is just a named queue of messages.

| Topic | What's in it |
|---|---|
| `orders.raw` | New orders waiting to be processed |
| `orders.processed` | Finished orders (approved or rejected) |
| `orders.raw.DLT` | Orders Camel couldn't handle (the returned-mail tray) |
| `orders.invalid` | Records NiFi rejected because they were badly shaped |
| `fx.rates` | Latest exchange rates |

Orders are grouped by **customer ID**, so all of one customer's orders are handled in the order they arrived.

---

## 9. Why use so many tools?

Fair question. In a real company, each one solves a different problem:

- **NiFi** is good at *getting data in* from many different sources (HTTP, files, websites) and has a visual screen operations staff can change without writing code.
- **Camel** is good at *business logic*: the rules live in Java code that can be reviewed and tested.
- **Kafka** *decouples* the two, so either side can be slow or down without breaking the other.
- **Keycloak** means no service has to build its own login system.
- **Vert.x** is a lightweight, fast front door that can also push live updates to the browser.

The project's own docs admit that if you only had one input source (HTTP), NiFi would be overkill. See `docs/design-decisions.md`.

---

## 10. Security in one paragraph

You log in through Keycloak. For the **browser**, the gateway keeps your token on the server and gives your browser only a cookie, which is safer than storing tokens in the browser. **Machines** send their token with every request. The gateway checks the token and your role (`orders-read` or `orders-write`). When the gateway asks the Spring Boot service for data, it passes your token along, and **Spring checks it again**. So even if someone sneaks past the gateway, they still can't read data without a valid token. This is called "defence in depth".

---

## 11. Folder map

```
gateway/               The front door (Vert.x) + the web page (index.html) + API docs
integration-service/   The head chef (Spring Boot + Camel) + read-only API
  └─ routes/           ← the actual order-processing logic lives here
generator/             The fake-customer program
infra/postgres/        Database tables + the 3 starter customers
infra/keycloak/        Users, roles and login settings
infra/nifi/            Script that builds the NiFi flow automatically
data/inbox/            Drop CSV files here and NiFi picks them up
scripts/               Get a login token; run all the tests
docs/                  The detailed, grown-up documentation
docker-compose.yml     Starts everything with one command
```

**If you only read one code file**, read `integration-service/src/main/java/com/example/orders/routes/OrderRoutes.java`. It is the whole order-processing recipe in about 100 lines.

---

## 12. How to run it

You need Docker (about 8 GB of memory allocated to it).

```bash
docker compose up -d --build --wait    # start everything (first time takes ~5 min)
```

Then open:

| What | Where | Login |
|---|---|---|
| The web page | http://localhost:8080 | alice / alice |
| NiFi flow screen | https://localhost:8443/nifi | admin / adminadmin123 (accept the certificate warning) |
| Keycloak | http://localhost:8180 | admin / admin |
| API docs | http://localhost:8080/docs.html | needs a token |

Useful commands:

```bash
scripts/get-token.sh alice        # print a login token for alice (or bob, or partner)
scripts/e2e.sh                    # run every test scenario (~4 min)
docker compose logs -f integration-service    # watch the head chef work
docker compose down -v            # stop everything and wipe all data
```

**A good first experiment:** log in as alice, submit an order for **C-999** (a customer that doesn't exist), and watch it land in the dead-letter pile in the live feed. Then try **C-300** for 5000 USD and watch it get REJECTED for being over the credit limit.

---

## 13. Mini glossary

| Term | Plain meaning |
|---|---|
| **Token / JWT** | A digital ID badge proving who you are and what you can do. Expires after a while |
| **OAuth2 / OIDC** | The standard rules for how login and tokens work |
| **PKCE** | An extra security step in browser login so a stolen login code is useless |
| **BFF (Backend-for-Frontend)** | The gateway holds your tokens on the server instead of in your browser |
| **202 Accepted** | "Got your request, still working on it" |
| **Topic** | A named queue in Kafka |
| **Partition** | A topic is split into lanes so work can happen in parallel |
| **Consumer lag** | How many messages are waiting that nobody has processed yet |
| **Idempotent** | Doing it twice has the same effect as doing it once (duplicates are harmless) |
| **DLT / Dead letter** | The "returned mail" queue for messages that failed |
| **Enrich** | Add missing info to a message (e.g. look up the customer's name) |
| **EIP (Enterprise Integration Patterns)** | A well-known catalogue of recipes for moving messages between systems. Camel implements them |
| **SSE (Server-Sent Events)** | The server pushes live updates to the browser without the page refreshing |
| **Backpressure** | When a downstream step is full, the upstream step slows down instead of overflowing |
| **Provenance** | NiFi's history of exactly where each piece of data came from and went |
| **POC** | Proof of concept: a demo, not a production system |
