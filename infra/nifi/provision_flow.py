"""
Builds the NiFi ingestion flow through the NiFi REST API. Idempotent: if the process group already
exists it is left alone (delete it in the NiFi UI and rerun this script to rebuild it).

Flow (process group "Order Ingestion"):

  ListenHTTP :9090/orders  ─┐                                       ┌─ valid ──> PublishKafka orders.raw     (key = customerId)
                            ├─> EvaluateJsonPath ─> RouteOnAttribute┤
  GetFile *.csv ─> ConvertRecord (CSV->JSON lines) ─> SplitText ─┘    └─ invalid ─> PublishKafka orders.invalid

  InvokeHTTP (Frankfurter FX API, every 60 s) ─> PublishKafka fx.rates (key = USD)

Only the Python standard library is used so this runs in a stock python image.
"""
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

NIFI = os.environ.get("NIFI_API", "https://nifi:8443/nifi-api")
USER = os.environ.get("NIFI_USERNAME", "admin")
PASSWORD = os.environ.get("NIFI_PASSWORD", "adminadmin123")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
FX_URL = os.environ.get("FX_URL", "https://api.frankfurter.dev/v1/latest?base=USD&symbols=EUR,GBP,INR")
FX_SCHEDULE = os.environ.get("FX_SCHEDULE", "60 sec")
PG_NAME = "Order Ingestion"

CTX = ssl._create_unverified_context()  # NiFi's generated self-signed certificate
TOKEN = None


def call(method, path, body=None, form=None):
    headers = {}
    data = None
    if TOKEN:
        headers["Authorization"] = "Bearer " + TOKEN
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(NIFI + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, context=CTX, timeout=30) as resp:
            raw = resp.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:500]}") from None
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def wait_for_nifi():
    global TOKEN
    for attempt in range(90):
        try:
            TOKEN = call("POST", "/access/token", form={"username": USER, "password": PASSWORD})
            call("GET", "/flow/about")
            print("NiFi is up")
            return
        except Exception as e:  # noqa: BLE001 - keep polling whatever the failure
            TOKEN = None
            print(f"waiting for NiFi ({attempt}): {str(e)[:120]}")
            time.sleep(5)
    sys.exit("NiFi did not become ready")


def new(revision_version=0):
    return {"revision": {"version": revision_version}}


def controller_service(pg, name, type_, props):
    entity = call("POST", f"/process-groups/{pg}/controller-services",
                  {**new(), "component": {"type": type_, "name": name, "properties": props}})
    return entity["id"]


def enable_service(svc_id):
    entity = call("GET", f"/controller-services/{svc_id}")
    call("PUT", f"/controller-services/{svc_id}/run-status",
         {"revision": entity["revision"], "state": "ENABLED"})
    for _ in range(30):
        if call("GET", f"/controller-services/{svc_id}")["component"]["state"] == "ENABLED":
            return
        time.sleep(1)
    entity = call("GET", f"/controller-services/{svc_id}")
    sys.exit(f"Controller service {entity['component']['name']} did not enable: "
             f"{entity['component'].get('validationErrors')}")


def processor(pg, name, type_, x, y, props=None, auto_terminate=(), schedule=None, penalty=None):
    config = {"properties": props or {}, "autoTerminatedRelationships": list(auto_terminate)}
    if schedule:
        config["schedulingPeriod"] = schedule
    if penalty:
        config["penaltyDuration"] = penalty
    entity = call("POST", f"/process-groups/{pg}/processors", {
        **new(),
        "component": {"type": type_, "name": name, "position": {"x": x, "y": y}, "config": config},
    })
    return entity["id"]


def connect(pg, src, dst, relationships, name=None, bends=None):
    call("POST", f"/process-groups/{pg}/connections", {
        **new(),
        "component": {
            "name": name or "",
            "source": {"id": src, "groupId": pg, "type": "PROCESSOR"},
            "destination": {"id": dst, "groupId": pg, "type": "PROCESSOR"},
            "selectedRelationships": relationships,
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
            "bends": bends or [],
        },
    })


def main():
    wait_for_nifi()
    root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
    existing = call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]
    for group in existing:
        if group["component"]["name"] == PG_NAME:
            print(f"Process group '{PG_NAME}' already exists; nothing to do")
            return

    pg = call("POST", f"/process-groups/{root}/process-groups",
              {**new(), "component": {"name": PG_NAME, "position": {"x": 0, "y": 0}}})["id"]
    print("Created process group", pg)

    kafka = controller_service(pg, "Kafka (kafka:9092)", "org.apache.nifi.kafka.service.Kafka3ConnectionService",
                               {"bootstrap.servers": KAFKA,
                                # NiFi 2.x defaults to SSL; the POC broker listens in plaintext on the internal network.
                                "security.protocol": "PLAINTEXT"})
    csv_reader = controller_service(pg, "Partner CSV reader", "org.apache.nifi.csv.CSVReader", {
        "Schema Access Strategy": "csv-header-derived",
        "Treat First Line as Header": "true",
    })
    json_writer = controller_service(pg, "JSON lines writer", "org.apache.nifi.json.JsonRecordSetWriter", {
        "Output Grouping": "output-oneline",
        "Suppress Null Values": "always-suppress",
    })
    for svc in (kafka, csv_reader, json_writer):
        enable_service(svc)

    def publisher(name, topic, key, x, y):
        return processor(pg, name, "org.apache.nifi.kafka.processors.PublishKafka", x, y, {
            "Kafka Connection Service": kafka,
            "Topic Name": topic,
            "Kafka Key": key,
            "acks": "all",
            "Transactions Enabled": "false",
            "Failure Strategy": "Route to Failure",
        }, auto_terminate=["success"], penalty="5 sec")

    # Channel 1: orders forwarded by the Vert.x gateway (already authenticated there).
    listen = processor(pg, "Receive orders from gateway", "org.apache.nifi.processors.standard.ListenHTTP", 0, 0, {
        "Listening Port": "9090",
        "Base Path": "orders",
    })

    # Channel 2: partner CSV files dropped into a shared folder (stand-in for SFTP).
    get_file = processor(pg, "Pick up partner CSV files", "org.apache.nifi.processors.standard.GetFile", 500, -300, {
        "Input Directory": "/opt/nifi/inbox",
        "File Filter": r"[^\.].*\.csv",
        "Polling Interval": "5 sec",
        "Keep Source File": "false",
    })
    convert = processor(pg, "CSV to JSON lines", "org.apache.nifi.processors.standard.ConvertRecord", 500, -100, {
        "Record Reader": csv_reader,
        "Record Writer": json_writer,
    })
    split = processor(pg, "One order per FlowFile", "org.apache.nifi.processors.standard.SplitText", 500, 100, {
        "Line Split Count": "1",
    }, auto_terminate=["original"])

    # Shared validation and publishing.
    extract = processor(pg, "Extract routing attributes", "org.apache.nifi.processors.standard.EvaluateJsonPath",
                        0, 300, {
                            "Destination": "flowfile-attribute",
                            "order.id": "$.orderId",
                            "customer.id": "$.customerId",
                        }, auto_terminate=["unmatched"])
    route = processor(pg, "Structural validation", "org.apache.nifi.processors.standard.RouteOnAttribute", 0, 550, {
        "valid": "${order.id:isEmpty():not():and(${customer.id:isEmpty():not()})}",
    })
    publish_raw = publisher("Publish to orders.raw", "orders.raw", "${customer.id}", 0, 800)
    publish_invalid = publisher("Publish to orders.invalid", "orders.invalid", "${filename}", 500, 800)

    # Channel 3: public reference data, the Frankfurter API (ECB exchange rates).
    fx_poll = processor(pg, "Poll Frankfurter FX rates", "org.apache.nifi.processors.standard.InvokeHTTP",
                        1000, 0, {
                            "HTTP Method": "GET",
                            "HTTP URL": FX_URL,
                        }, auto_terminate=["Original", "Failure", "No Retry", "Retry"], schedule=FX_SCHEDULE)
    publish_fx = publisher("Publish to fx.rates", "fx.rates", "USD", 1000, 300)

    connect(pg, listen, extract, ["success"])
    connect(pg, get_file, convert, ["success"])
    connect(pg, convert, split, ["success"])
    connect(pg, convert, publish_invalid, ["failure"], name="unparseable CSV")
    connect(pg, split, extract, ["splits"])
    connect(pg, split, publish_invalid, ["failure"])
    connect(pg, extract, route, ["matched"])
    connect(pg, extract, publish_invalid, ["failure"], name="not JSON")
    connect(pg, route, publish_raw, ["valid"])
    connect(pg, route, publish_invalid, ["unmatched"], name="missing orderId/customerId")
    # Kafka unavailable: loop back and retry after the penalty; the queue applies backpressure upstream.
    connect(pg, publish_raw, publish_raw, ["failure"], name="retry",
            bends=[{"x": 400, "y": 850}, {"x": 400, "y": 950}])
    connect(pg, publish_invalid, publish_invalid, ["failure"], name="retry",
            bends=[{"x": 900, "y": 850}, {"x": 900, "y": 950}])
    connect(pg, fx_poll, publish_fx, ["Response"])
    connect(pg, publish_fx, publish_fx, ["failure"], name="retry",
            bends=[{"x": 1400, "y": 350}, {"x": 1400, "y": 450}])

    flow = call("GET", f"/flow/process-groups/{pg}")["processGroupFlow"]["flow"]
    invalid = [(p["component"]["name"], p["component"].get("validationErrors"))
               for p in flow["processors"] if p["component"].get("validationErrors")]
    if invalid:
        sys.exit(f"Invalid processors: {invalid}")

    call("PUT", f"/flow/process-groups/{pg}", {"id": pg, "state": "RUNNING"})
    print("Flow started")


if __name__ == "__main__":
    main()
