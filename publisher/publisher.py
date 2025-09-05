import os, time, traceback, requests
from pymongo import MongoClient, errors
from bson import json_util

MONGO_URI  = os.environ["MONGO_URI"]
DB         = os.environ.get("WATCH_DB","metricsdb")
COLL       = os.environ.get("WATCH_COLL","metrics")
WEBHOOK    = os.environ["WEBHOOK_URL"]

def connect():
    while True:
        try:
            c = MongoClient(MONGO_URI, appname="mongo-stream-publisher", serverSelectionTimeoutMS=3000)
            c.admin.command("ping")
            print("✅ Connected to MongoDB", flush=True)
            return c
        except Exception as e:
            print("⏳ Waiting for MongoDB:", e, flush=True); time.sleep(2)

while True:
    client = connect()
    coll = client[DB][COLL]
    print(f"📡 Watching {DB}.{COLL} for inserts → {WEBHOOK}", flush=True)
    try:
        with coll.watch([{"$match":{"operationType":"insert"}}], full_document="updateLookup") as stream:
            for change in stream:
                doc = change.get("fullDocument", {})
                try:
                    payload = json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS)
                    r = requests.post(WEBHOOK, data=payload, headers={"Content-Type": "application/json"}, timeout=5)
                    print(f"→ POST {WEBHOOK} -> {r.status_code}", flush=True)
                except Exception as e:
                    print("POST error:", e, flush=True)
    except errors.PyMongoError as e:
        print("Change stream error, will reconnect:", e, flush=True)
        time.sleep(2)
    except Exception:
        traceback.print_exc(); time.sleep(2)
