
import os, time, traceback, threading, requests
from pymongo import MongoClient, errors
from datetime import datetime, timezone
from bson import ObjectId

MONGO_URI  = os.environ["MONGO_URI"]
DB         = os.environ.get("WATCH_DB","metricsdb")
COLL       = os.environ.get("WATCH_COLL","metrics")

CIM_INTERNAL_ENDPOINT         = os.environ["CIM_INTERNAL_ENDPOINT"]
GD_BEARER_TOKEN     = os.environ.get("GD_BEARER_TOKEN","")
# SITES_URL           = os.environ.get("SITES_URL","http://ci-calc:8011/load-sites")
KPI_INTERNAL_ENDPOINT  = os.environ.get("KPI_INTERNAL_ENDPOINT","")

session = requests.Session()
headers = {"Content-Type": "application/json"}
if GD_BEARER_TOKEN:
    headers["Authorization"] = f"Bearer {GD_BEARER_TOKEN}"

fwd_headers = {"Content-Type": "application/json"}
if GD_BEARER_TOKEN:
    fwd_headers["Authorization"] = f"Bearer {GD_BEARER_TOKEN}"

def jsonable(x):
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, datetime):
        return to_iso_z(x)
    if isinstance(x, dict):
        return {k: jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set)):
        return [jsonable(v) for v in x]
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    return str(x)

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

def to_ci_request(change: dict) -> dict:
    # If you use change streams with fullDocument:
    body = (change or {}).get("fullDocument", {}).get("body")
    if isinstance(body, dict):
        return body
    # If your insert stores the metrics directly:
    if isinstance((change or {}).get("fullDocument"), dict):
        return change["fullDocument"]
    raise ValueError("Could not extract metrics JSON from change event")

def watch_inserts(coll):
    try:
        print(f"Watching {DB}.{COLL} for inserts → {CIM_INTERNAL_ENDPOINT}", flush=True)
        with coll.watch([{"$match":{"operationType":"insert"}}], full_document="updateLookup") as stream:
            for change in stream:
                try:
                    # send the full metrics JSON directly to /transform-and-forward
                    payload = jsonable(to_ci_request(change))
                    r = session.post(CIM_INTERNAL_ENDPOINT, json=payload, headers=headers, timeout=20)
                    print(f"→ POST {CIM_INTERNAL_ENDPOINT} -> {r.status_code}", flush=True)
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
        print(f"Watching {DB}.{COLL} for updates (cfp_ci_service) → {KPI_INTERNAL_ENDPOINT}", flush=True)
        with coll.watch([{"$match":{"operationType":"update"}}], full_document="updateLookup") as stream2:
            for change in stream2:
                full_metric = change.get("fullDocument") or {}
                # ci = full_metric.get("cfp_ci_service")
                # if not ci or not KPI_INTERNAL_ENDPOINT:
                #     continue
                cim_payload = {
                    "publisher_email": full_metric.get("publisher_email","unknown@example.org"),
                    "job_id": str(full_metric.get("job_id", full_metric.get("_id"))),
                    "metrics": [{
                        "node": (full_metric.get("body") or {}).get("node", "unknown"),
                        "metric": (full_metric.get("body") or {}).get("metric", "unknown"),
                        "value": (full_metric.get("body") or {}).get("value", 0.0),
                        "timestamp": to_iso_z((full_metric.get("body") or {}).get("ts")),
                        # "cfp_ci_service": ci
                    }]
                }
                try:
                    fr = session.post(KPI_INTERNAL_ENDPOINT, json=cim_payload, headers=fwd_headers, timeout=20)
                    print("→ FORWARD (update)", KPI_INTERNAL_ENDPOINT, "->", fr.status_code, flush=True)
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
