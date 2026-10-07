#!/usr/bin/env bash
# End-to-end scenarios against a running stack (docker compose up -d).
#   scripts/e2e.sh                # all scenarios
#   SKIP_RESILIENCE=1 scripts/e2e.sh   # skip the ones that stop containers (about 2 minutes faster)
#
# Needs: bash, curl, jq, docker compose. Run from the repository root.
set -uo pipefail
cd "$(dirname "$0")/.."

GW=http://localhost:8080
SVC=http://localhost:8081
PASS=0; FAIL=0; FAILED=()

green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
step()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pass()  { green "  PASS  $*"; PASS=$((PASS + 1)); }
fail()  { red   "  FAIL  $*"; FAIL=$((FAIL + 1)); FAILED+=("$*"); }
check() { local desc=$1 expected=$2 actual=$3; if [[ "$actual" == "$expected" ]]; then pass "$desc"; else fail "$desc (expected $expected, got $actual)"; fi; }

token() { scripts/get-token.sh "$1"; }
status() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
psql_q() { docker compose exec -T postgres psql -U orders -d orders -tAc "$1"; }
kafka() { docker compose exec -T kafka "$@"; }

submit() {  # submit <token> <json> -> sets BODY, HTTP_CODE and ORDER_ID (no subshell, so the globals stick)
  local out; out=$(curl -s -w '\n%{http_code}' -X POST "$GW/api/orders" -H "Authorization: Bearer $1" \
    -H 'Content-Type: application/json' -d "$2")
  HTTP_CODE=$(tail -n1 <<<"$out"); BODY=$(sed '$d' <<<"$out")
  ORDER_ID=$(jq -r '.orderId // empty' <<<"$BODY" 2>/dev/null)
}

wait_order() {  # wait_order <token> <orderId> [seconds] -> prints order JSON or nothing
  local deadline=$((SECONDS + ${3:-30}))
  while (( SECONDS < deadline )); do
    local body; body=$(curl -s -H "Authorization: Bearer $1" "$GW/api/orders/$2")
    if [[ $(jq -r '.orderId // empty' <<<"$body" 2>/dev/null) == "$2" ]]; then echo "$body"; return 0; fi
    sleep 1
  done
  return 1
}

in_topic() {  # in_topic <topic> <needle> [seconds]
  local deadline=$((SECONDS + ${3:-30}))
  while (( SECONDS < deadline )); do
    if kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic "$1" \
        --from-beginning --timeout-ms 4000 --property print.headers=true 2>/dev/null | grep -F -- "$2"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

produce_raw() {  # produce_raw <key> <value>  (bypasses gateway and NiFi, like a misbehaving producer)
  printf '%s|%s\n' "$1" "$2" | kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
    --topic orders.raw --property parse.key=true --property key.separator='|' >/dev/null 2>&1
}

# ---------------------------------------------------------------------------------------------
step "0. Stack is up"
check "gateway health"               200 "$(status $GW/health)"
check "integration-service health"   200 "$(status $SVC/actuator/health)"
check "keycloak realm discovery"     200 "$(status http://localhost:8180/realms/orders-poc/.well-known/openid-configuration)"
check "nifi UI reachable (https)"    200 "$(status -k https://localhost:8443/nifi/)"
check "gateway OpenAPI spec served"         200 "$(status $GW/openapi.yaml)"
check "gateway Swagger UI page served"       200 "$(status $GW/docs.html)"
check "service OpenAPI spec generated"       200 "$(status $SVC/v3/api-docs)"
check "service spec declares the keycloak bearer scheme" bearer "$(curl -s $SVC/v3/api-docs | jq -r '.components.securitySchemes.keycloak.scheme')"
check "service Swagger UI served"            200 "$(status -L $SVC/swagger-ui.html)"
ALICE=$(token alice); BOB=$(token bob); PARTNER=$(token partner)
[[ -n "$ALICE" && -n "$BOB" && -n "$PARTNER" ]] && pass "tokens issued for alice, bob, partner-client" || fail "token issuance"

# ---------------------------------------------------------------------------------------------
step "1. Security: OAuth2 / JWT at the gateway and again at the service"
check "no token -> 401"                            401 "$(status $GW/api/orders)"
check "garbage token -> 401"                       401 "$(status -H 'Authorization: Bearer abc.def.ghi' $GW/api/orders)"
check "bob (orders-read only) can read -> 200"     200 "$(status -H "Authorization: Bearer $BOB" $GW/api/orders)"
check "bob cannot submit -> 403"                   403 "$(status -X POST -H "Authorization: Bearer $BOB" -H 'Content-Type: application/json' -d '{}' $GW/api/orders)"
check "service called directly without token -> 401 (defence in depth)" 401 "$(status $SVC/api/orders)"
check "service called directly with partner token -> 200"                200 "$(status -H "Authorization: Bearer $PARTNER" $SVC/api/orders)"
check "service rejects writes it does not offer -> 403"                  403 "$(status -X POST -H "Authorization: Bearer $PARTNER" $SVC/api/orders)"
check "browser session: /login redirects to Keycloak" 302 "$(status $GW/login)"
loc=$(curl -s -o /dev/null -w '%{redirect_url}' $GW/login)
[[ "$loc" == *"code_challenge_method=S256"* ]] && pass "login uses PKCE (S256)" || fail "login uses PKCE (redirect: $loc)"

# ---------------------------------------------------------------------------------------------
step "2. Edge validation at the gateway (cheap checks, no Kafka traffic)"
submit "$ALICE" '{"customerId":"nope","product":"x","quantity":1,"amount":1}'; check "bad customerId format -> 400" 400 "$HTTP_CODE"
submit "$ALICE" '{"customerId":"C-100","product":"x","quantity":0,"amount":1}'; check "quantity 0 -> 400" 400 "$HTTP_CODE"
submit "$ALICE" '{"customerId":"C-100","product":"x","quantity":1,"amount":1,"currency":"JPY"}'; check "unsupported currency -> 400" 400 "$HTTP_CODE"
submit "$ALICE" 'not json'; check "non-JSON body -> 400" 400 "$HTTP_CODE"

# ---------------------------------------------------------------------------------------------
step "3. Happy path through every layer: UI user -> Vert.x -> NiFi -> Kafka -> Camel -> Postgres -> API"
submit "$ALICE" '{"customerId":"C-100","product":"Widget","quantity":2,"amount":120.50,"currency":"EUR"}'; body=$BODY
check "submit -> 202 Accepted" 202 "$HTTP_CODE"
HAPPY_ID=$(jq -r .orderId <<<"$body")
order=$(wait_order "$ALICE" "$HAPPY_ID") && pass "order $HAPPY_ID processed and readable via gateway" || fail "order $HAPPY_ID never processed"
check "status APPROVED"                 APPROVED  "$(jq -r .status <<<"$order")"
check "GOLD customer -> EXPRESS lane"   EXPRESS   "$(jq -r .lane <<<"$order")"
check "enriched customer name"          "Acme Corp" "$(jq -r .customerName <<<"$order")"
check "source recorded as web-ui"       web-ui    "$(jq -r .source <<<"$order")"
check "submittedBy taken from token"    alice     "$(jq -r .submittedBy <<<"$order")"
rate=$(jq -r .fxRate <<<"$order"); usd=$(jq -r .amountUsd <<<"$order")
expected=$(awk -v a=120.50 -v r="$rate" 'BEGIN { printf "%.2f", a / r }')
check "EUR converted to USD with stored FX rate ($rate)" "$expected" "$(printf '%.2f' "$usd")"
check "row exists in Postgres" 1 "$(psql_q "select count(*) from orders where order_id='$HAPPY_ID'")"
in_topic orders.processed "$HAPPY_ID" >/dev/null && pass "event published to orders.processed" || fail "event missing from orders.processed"

# ---------------------------------------------------------------------------------------------
step "4. Content-based routing in Camel"
submit "$PARTNER" '{"customerId":"C-200","product":"Gadget","quantity":1,"amount":50,"priority":"HIGH"}'; id=$ORDER_ID
o=$(wait_order "$PARTNER" "$id"); check "SILVER + HIGH priority -> EXPRESS" EXPRESS "$(jq -r .lane <<<"$o")"
check "partner client recorded as partner-api" partner-api "$(jq -r .source <<<"$o")"
submit "$PARTNER" '{"customerId":"C-200","product":"Gadget","quantity":1,"amount":50}'; id=$ORDER_ID
o=$(wait_order "$PARTNER" "$id"); check "SILVER + NORMAL -> STANDARD" STANDARD "$(jq -r .lane <<<"$o")"
submit "$PARTNER" '{"customerId":"C-300","product":"Server rack","quantity":1,"amount":5000,"currency":"USD"}'; id=$ORDER_ID
o=$(wait_order "$PARTNER" "$id"); check "BRONZE over 1000 USD credit limit -> REJECTED" REJECTED "$(jq -r .status <<<"$o")"
[[ $(jq -r .reason <<<"$o") == *"exceeds credit limit"* ]] && pass "rejection reason recorded" || fail "rejection reason missing"

# ---------------------------------------------------------------------------------------------
step "5. Dead letter channel: business errors are not retried"
submit "$ALICE" '{"customerId":"C-999","product":"Widget","quantity":1,"amount":10}'; body=$BODY
check "unknown customer passes edge validation -> 202" 202 "$HTTP_CODE"
DLT_ID=$(jq -r .orderId <<<"$body")
line=$(in_topic orders.raw.DLT "$DLT_ID")
[[ "$line" == *"x-error-type:UnknownCustomerException"* ]] && pass "parked in orders.raw.DLT with x-error-type header" || fail "unknown customer not in DLT ($line)"
[[ "$line" == *"x-source-topic:orders.raw"* ]] && pass "DLT record carries x-source-topic header" || fail "x-source-topic header missing ($line)"
sleep 2; check "not written to Postgres" 0 "$(psql_q "select count(*) from orders where order_id='$DLT_ID'")"
check "API answers 404 for it" 404 "$(status -H "Authorization: Bearer $ALICE" $GW/api/orders/$DLT_ID)"

step "6. Poison message written straight to Kafka (bypassing gateway and NiFi)"
POISON="poison-$RANDOM"
produce_raw C-100 "{this is not json $POISON"
line=$(in_topic orders.raw.DLT "$POISON")
[[ "$line" == *"x-error-type:JsonParseException"* ]] && pass "malformed JSON parked in DLT, consumer not blocked" || fail "poison message not in DLT ($line)"

# ---------------------------------------------------------------------------------------------
step "7. Idempotent consumer: Kafka redelivers the same order"
dup=$(jq -c '{orderId, customerId, product, quantity, amount, currency, priority, source, submittedBy, receivedAt}' <<<"$order")
produce_raw C-100 "$dup"; produce_raw C-100 "$dup"
dup_seen=0
for _ in $(seq 1 20); do
  docker compose logs --since 2m integration-service 2>/dev/null | grep -q "Duplicate order $HAPPY_ID ignored" && { dup_seen=1; break; }
  sleep 1
done
check "duplicate detected and logged by Camel" 1 "$dup_seen"
check "still exactly one row for $HAPPY_ID" 1 "$(psql_q "select count(*) from orders where order_id='$HAPPY_ID'")"

# ---------------------------------------------------------------------------------------------
step "8. Partner file drop: CSV -> NiFi (GetFile, ConvertRecord, SplitText) -> Kafka"
F1=$(uuidgen | tr 'A-Z' 'a-z'); F2=$(uuidgen | tr 'A-Z' 'a-z'); F3=$(uuidgen | tr 'A-Z' 'a-z')
cat > data/inbox/.e2e.tmp <<CSV
orderId,customerId,product,quantity,amount,currency,priority,source,submittedBy,receivedAt
$F1,C-100,Bracket,3,30.00,GBP,NORMAL,file-drop,e2e,2026-10-07T10:00:00Z
$F2,C-200,Gizmo,1,15.00,INR,HIGH,file-drop,e2e,2026-10-07T10:00:00Z
$F3,,Orphan,1,15.00,USD,NORMAL,file-drop,e2e,2026-10-07T10:00:00Z
CSV
mv data/inbox/.e2e.tmp "data/inbox/e2e-$(date +%s).csv"
o=$(wait_order "$ALICE" "$F1" 40) && pass "CSV row 1 processed" || fail "CSV row 1 not processed"
check "CSV row recorded as file-drop" file-drop "$(jq -r .source <<<"$o")"
wait_order "$ALICE" "$F2" 20 >/dev/null && pass "CSV row 2 processed" || fail "CSV row 2 not processed"
in_topic orders.invalid "$F3" >/dev/null && pass "row without customerId rejected by NiFi to orders.invalid" || fail "invalid CSV row not in orders.invalid"
check "invalid row never reached Postgres" 0 "$(psql_q "select count(*) from orders where order_id='$F3'")"

# ---------------------------------------------------------------------------------------------
step "9. Public data source: Frankfurter FX feed via NiFi InvokeHTTP -> fx.rates -> Camel"
src=$(psql_q "select source from fx_rates where currency='EUR'")
if [[ "$src" == "frankfurter" ]]; then pass "EUR rate comes from the live feed"
else fail "EUR rate source is '$src' (no internet from the NiFi container?)"; fi
check "fx-rates exposed through the gateway" 200 "$(status -H "Authorization: Bearer $BOB" $GW/api/fx-rates)"

# ---------------------------------------------------------------------------------------------
step "10. Server-sent events: Kafka -> Vert.x event bus -> SSE"
sse_file=$(mktemp)
curl -sN -H "Authorization: Bearer $BOB" "$GW/api/events" > "$sse_file" &
sse_pid=$!
sleep 2
submit "$ALICE" '{"customerId":"C-100","product":"Live","quantity":1,"amount":9}'; id=$ORDER_ID
for _ in $(seq 1 20); do grep -q "$id" "$sse_file" && break; sleep 1; done
kill $sse_pid 2>/dev/null; wait $sse_pid 2>/dev/null
grep -q "event: processed" "$sse_file" && grep -q "$id" "$sse_file" && pass "processed event pushed over SSE" || fail "SSE event not received"
rm -f "$sse_file"

# ---------------------------------------------------------------------------------------------
if [[ -z "${SKIP_RESILIENCE:-}" ]]; then
  step "11. Resilience: Kafka buffers while the consumer is down"
  docker compose stop integration-service >/dev/null 2>&1
  submit "$ALICE" '{"customerId":"C-200","product":"Buffered","quantity":1,"amount":20}'; body=$BODY
  check "gateway still accepts orders while Camel is down -> 202" 202 "$HTTP_CODE"
  BUF_ID=$(jq -r .orderId <<<"$body")
  sleep 3
  check "nothing in Postgres yet" 0 "$(psql_q "select count(*) from orders where order_id='$BUF_ID'")"
  lag=$(kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group integration-service 2>/dev/null \
        | awk '$2=="orders.raw" {s+=$6} END {print s+0}')
  (( lag > 0 )) && pass "consumer lag on orders.raw is $lag while the service is down" || fail "expected consumer lag > 0 (got $lag)"
  docker compose start integration-service >/dev/null 2>&1
  wait_order "$ALICE" "$BUF_ID" 120 >/dev/null && pass "buffered order processed after restart" || fail "buffered order lost"

  step "12. Resilience: database outage -> Camel retries with backoff, then DLT"
  docker compose stop postgres >/dev/null 2>&1
  submit "$ALICE" '{"customerId":"C-100","product":"DbDown","quantity":1,"amount":20}'; body=$BODY
  DBDOWN_ID=$(jq -r .orderId <<<"$body")
  line=$(in_topic orders.raw.DLT "$DBDOWN_ID" 90)
  [[ -n "$line" ]] && pass "after 3 redeliveries the order is parked in the DLT ($(grep -o 'x-error-type:[A-Za-z]*' <<<"$line"))" \
    || fail "order not in DLT after database outage"
  docker compose start postgres >/dev/null 2>&1
  retries=$(docker compose logs --since 3m integration-service 2>/dev/null | grep -c "DeadLetterChannel.*Failed delivery")
  (( retries >= 3 )) && pass "redelivery attempts with backoff visible in the logs ($retries)" || fail "redelivery attempts not logged ($retries)"
  for _ in $(seq 1 30); do [[ $(status $SVC/actuator/health) == 200 ]] && break; sleep 2; done
fi

# ---------------------------------------------------------------------------------------------
step "13. Rate limiting at the gateway (per caller, per minute)"
got429=0
for _ in $(seq 1 40); do
  submit "$ALICE" '{"customerId":"bad"}'
  if [[ "$HTTP_CODE" == 429 ]]; then got429=1; break; fi
done
check "burst of requests eventually gets 429" 1 "$got429"

# ---------------------------------------------------------------------------------------------
printf '\n\033[1mResult: %d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
for f in ${FAILED[@]+"${FAILED[@]}"}; do red "  - $f"; done   # bash 3.2 + set -u safe
(( FAIL == 0 ))
