
import os, time, traceback, threading, requests
from pymongo import MongoClient, errors
from datetime import datetime, timezone

MONGO_URI  = os.environ["MONGO_URI"]
DB         = os.environ.get("WATCH_DB","metricsdb")
COLL       = os.environ.get("WATCH_COLL","metrics")

WEBHOOK_URL         = os.environ["WEBHOOK_URL"]
GD_BEARER_TOKEN     = os.environ.get("GD_BEARER_TOKEN","")
SITES_URL           = os.environ.get("SITES_URL","http://ci-calc:8011/load-sites")
RESULT_FORWARD_URL  = os.environ.get("RESULT_FORWARD_URL","")

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
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    return str(ts)

def to_ci_request(doc: dict) -> dict:
    b = (doc or {}).get("body", {})
    node = b.get("node")
    site = SITES.get(node)
    if not site:
        print(f"No site match for node='{node}'. Reloading sites...", flush=True)
        SITES.update(load_sites())
        site = SITES.get(node)
        if not site:
            raise ValueError(f"No site mapping for node '{node}'")
    payload = {"lat": site["lat"], "lon": site["lon"]}
    ts = b.get("ts")
    if isinstance(ts, datetime):
        payload["time"] = to_iso_z(ts)
    elif isinstance(ts, str) and ts.strip():
        payload["time"] = ts.strip()
    if site.get("pue") is not None:
        payload["pue"] = float(site["pue"])
    if "energy_kwh" in b:
        try:
            payload["energy_kwh"] = float(b["energy_kwh"])
        except Exception:
            pass
    return payload

def watch_inserts(coll):
    try:
        print(f"Watching {DB}.{COLL} for inserts → {WEBHOOK_URL}", flush=True)
        with coll.watch([{"$match":{"operationType":"insert"}}], full_document="updateLookup") as stream:
            for change in stream:
                doc = change.get("fullDocument", {}) or {}
                metric_id = doc.get("_id")
                try:
                    ci_payload = to_ci_request(doc)
                    ci_payload["metric_id"] = str(metric_id)
                    r = session.post(WEBHOOK_URL, json=ci_payload, headers=headers, timeout=20)
                    print(f"→ POST {WEBHOOK_URL} -> {r.status_code}", flush=True)
                    if not r.ok:
                        try:
                            print("Response body:", r.text[:400], flush=True)
                        except Exception:
                            pass
                except Exception as e:
                    print("POST error:", e, flush=True)
    except errors.PyMongoError as e:
        print("Insert stream error, will reconnect:", e, flush=True)
        time.sleep(2)
        watch_inserts(coll)
    except Exception:
        traceback.print_exc()
        time.sleep(2)
        watch_inserts(coll)

def watch_updates(coll):
    try:
        print(f"Watching {DB}.{COLL} for updates (cfp_ci_service) → {RESULT_FORWARD_URL}", flush=True)
        with coll.watch([{"$match":{"operationType":"update"}}], full_document="updateLookup") as stream2:
            for change in stream2:
                full_metric = change.get("fullDocument") or {}
                ci = full_metric.get("cfp_ci_service")
                if not ci or not RESULT_FORWARD_URL:
                    continue
                cim_payload = {
                    "publisher_email": full_metric.get("publisher_email","unknown@example.org"),
                    "job_id": str(full_metric.get("job_id", full_metric.get("_id"))),
                    "metrics": [{
                        "node": (full_metric.get("body") or {}).get("node", "unknown"),
                        "metric": (full_metric.get("body") or {}).get("metric", "unknown"),
                        "value": (full_metric.get("body") or {}).get("value", 0.0),
                        "timestamp": to_iso_z((full_metric.get("body") or {}).get("ts")),
                        "cfp_ci_service": ci
                    }]
                }
                try:
                    fr = session.post(RESULT_FORWARD_URL, json=cim_payload, headers=fwd_headers, timeout=20)
                    print("→ FORWARD (update)", RESULT_FORWARD_URL, "->", fr.status_code, flush=True)
                    if fr.status_code >= 400:
                        print("Response body:", fr.text[:400], flush=True)
                except Exception as e:
                    print("Forward error (update):", e, flush=True)
    except errors.PyMongoError as e:
        print("Update stream error, will reconnect:", e, flush=True)
        time.sleep(2)
        watch_updates(coll)
    except Exception:
        traceback.print_exc()
        time.sleep(2)
        watch_updates(coll)

def main():
    client = connect()
    coll = client[DB][COLL]
    t1 = threading.Thread(target=watch_inserts, args=(coll,), daemon=True)
    t2 = threading.Thread(target=watch_updates, args=(coll,), daemon=True)
    t1.start(); t2.start()
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
