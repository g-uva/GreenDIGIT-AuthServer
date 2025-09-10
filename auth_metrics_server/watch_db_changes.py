import os, time, signal
from pymongo import MongoClient
from metrics_reader import get_cursor, save_cursor, get_metrics_since
import mongodb_to_sql as xport

MONGO_URI = os.getenv("MONGO_URI", "mongodb://metrics-db:27017/")
DB = os.getenv("METRICS_DB_NAME", "metricsdb")
COL = os.getenv("METRICS_COLLECTION", "metrics")
PROCESSOR = os.getenv("PROCESSOR_NAME", "kv_exporter")
BATCH_SECONDS = float(os.getenv("BATCH_SECONDS", "2.0"))

stop = False
def _stop(*_): 
    global stop; stop = True
signal.signal(signal.SIGTERM, _stop); signal.signal(signal.SIGINT, _stop)

def main():
    client = MongoClient(MONGO_URI)
    col = client[DB][COL]
    # watch inserts only
    with col.watch([{"$match": {"operationType": "insert"}}], full_document="updateLookup") as stream:
        last_tick = time.time()
        pending = 0
        while not stop:
            try:
                if stream.try_next():
                    pending += 1
                now = time.time()
                if pending and (now - last_tick >= BATCH_SECONDS):
                    # do an incremental export (cursor persisted)
                    inserted = xport.export_incremental()
                    pending = 0
                    last_tick = now
                elif not pending:
                    time.sleep(0.2)
            except Exception as e:
                # brief backoff on transient errors
                time.sleep(1.0)

if __name__ == "__main__":
    main()
