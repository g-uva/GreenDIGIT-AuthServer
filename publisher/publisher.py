import os, time, traceback, requests, json
from pymongo import MongoClient, errors
from datetime import datetime, timezone

MONGO_URI  = os.environ["MONGO_URI"]
DB         = os.environ.get("WATCH_DB","metricsdb")
COLL       = os.environ.get("WATCH_COLL","metrics")

WEBHOOK    = os.environ["WEBHOOK_URL"]
GD_BEARER_TOKEN      = os.environ.get("GD_BEARER_TOKEN","")
SITES_URL  = os.environ.get("SITES_URL","http://ci-calc:8011/load-sites")
RESULT_FORWARD_URL = os.environ.get("RESULT_FORWARD_URL","")

session = requests.Session()
headers = {"Content-Type": "application/json"}
if GD_BEARER_TOKEN:
    headers["Authorization"] = f"Bearer {GD_BEARER_TOKEN}"

fwd_headers = {"Content-Type": "application/json"}
if GD_BEARER_TOKEN:
    fwd_headers["Authorization"] = f"Bearer {GD_BEARER_TOKEN}"

def connect():
    while True:
        try:
            c = MongoClient(MONGO_URI, appname="mongo-stream-publisher", serverSelectionTimeoutMS=3000)
            c.admin.command("ping")
            print("Connected to MongoDB", flush=True)
            return c
        except Exception as e:
            print("Waiting for MongoDB:", e, flush=True)
            time.sleep(2)

def load_sites():
    """Return {site_name: {lat, lon, pue}} built from /load-sites."""
    try:
        r = session.get(SITES_URL, timeout=10)
        r.raise_for_status()
        arr = r.json()
        sites = {}
        for x in arr:
            name = x.get("site_name")
            lat, lon = x.get("latitude"), x.get("longitude")
            if name and lat is not None and lon is not None:
                sites[name] = {"lat": float(lat), "lon": float(lon), "pue": x.get("pue")}
        print(f"Loaded {len(sites)} sites from {SITES_URL}", flush=True)
        return sites
    except Exception as e:
        print("Failed to load sites:", e, flush=True)
        return {}

SITES = load_sites()

def to_iso_z(ts):
    if isinstance(ts, datetime):
        if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    return str(ts)

def to_ci_request(doc: dict) -> dict:
    b = (doc or {}).get("body", {})
    node = b.get("node")
    site = SITES.get(node)
    if not site:
        # try refresh once
        print(f"No site match for node='{node}'. Reloading sites...", flush=True)
        SITES.update(load_sites())
        site = SITES.get(node)
        if not site:
            raise ValueError(f"No site mapping for node '{node}'")
    payload = {
        "lat": site["lat"],
        "lon": site["lon"],
        "time": to_iso_z(b.get("ts")),
    }
    if site.get("pue") is not None:
        payload["pue"] = float(site["pue"])
    if "energy_kwh" in b:
        payload["energy_kwh"] = float(b["energy_kwh"])
    return payload

while True:
    client = connect()
    coll = client[DB][COLL]
    print(f"Watching {DB}.{COLL} for inserts → {WEBHOOK}", flush=True)
    try:
        with coll.watch([{"$match":{"operationType":"insert"}}], full_document="updateLookup") as stream:
            for change in stream:
                doc = change.get("fullDocument", {})
                try:
                    ci_payload = to_ci_request(doc)
                    r = session.post(WEBHOOK, json=ci_payload, headers=headers, timeout=15)
                    print(f"→ POST {WEBHOOK} -> {r.status_code}", flush=True)
                    if r.status_code >= 400:
                        print("Response body:", r.text[:400], flush=True)
                    elif RESULT_FORWARD_URL:
                        fr = session.post(RESULT_FORWARD_URL, data=r.text, headers=fwd_headers, timeout=15)
                        print(f"→ FORWARD {RESULT_FORWARD_URL} -> {fr.status_code}", flush=True)
                except Exception as e:
                    print("POST error:", e, flush=True)
    except errors.PyMongoError as e:
        print("Change stream error, will reconnect:", e, flush=True)
        time.sleep(2)
    except Exception:
        traceback.print_exc()
        time.sleep(2)
