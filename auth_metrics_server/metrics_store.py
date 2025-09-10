# metrics_store.py
# Purpose: create the DB/collection (with index) and provide a single function to store metrics.

import os
from datetime import datetime, timezone
from typing import Any, Dict

from pymongo import MongoClient, ASCENDING, InsertOne
from pymongo.errors import PyMongoError
from pymongo.write_concern import WriteConcern
from typing import List

# MONGO_URI = os.getenv("MONGO_URI", "mongodb://metrics-db:27017/")
MONGO_URI = "mongodb://metrics-db:27017/" # Hardcoded :D
# DB_NAME = os.getenv("METRICS_DB_NAME", "metricsdb")
DB_NAME = "metricsdb"
COLLECTION_NAME = os.getenv("METRICS_COLLECTION", "metrics")
INGEST_SESSIONS = "ingest_sessions"

_client = MongoClient(MONGO_URI)
_db = _client[DB_NAME]
_col = _db[COLLECTION_NAME]
# For bulk idempotency resume (in case of network blip, 502, etc.)
_sess = _db[INGEST_SESSIONS]
_sess.create_index(
    [("publisher_email", ASCENDING), ("idempotency_key", ASCENDING), ("seq", ASCENDING)],
    name="uq_pub_batch_seq", unique=True
)

def ensure_indexes() -> None:
    # writer/reader friendly
    _col.create_index([("timestamp", ASCENDING)], name="ix_timestamp")
    _col.create_index([("publisher_email", ASCENDING)], name="ix_publisher_email")
    # idempotency (pub, batch, seq) is globally unique
    _sess.create_index(
        [("publisher_email", ASCENDING), ("idempotency_key", ASCENDING), ("seq", ASCENDING)],
        name="uq_pub_batch_seq", unique=True
    )
    
# Must initialise the indexes.
ensure_indexes()

def store_metric(publisher_email: str, body: Any, timestamp_iso: str | None = None) -> Dict[str, Any]:
    """
    Insert one metric document.
    - publisher_email is the identity extracted from JWT (trusted).
    - body is the raw JSON payload from the request (dict/list/etc).
    - timestamp is set server-side (UTC) unless provided.
    Returns a minimal ack with inserted_id and timestamp.
    """

    if timestamp_iso is None:
        timestamp_iso = datetime.now(timezone.utc).isoformat()
    doc = {"timestamp": timestamp_iso, "publisher_email": publisher_email, "body": body}
    try:
        result = _col.insert_one(doc)
        return {"ok": True, "id": str(result.inserted_id), "timestamp": timestamp_iso, "publisher_email": publisher_email}
    except PyMongoError as e:
        return {"ok": False, "error": str(e)}

def store_metrics_bulk(publisher_email: str, bodies: List[dict], ts_iso: str | None = None) -> Dict[str, Any]:
    if ts_iso is None:
        ts_iso = datetime.now(timezone.utc).isoformat()
    ops = [InsertOne({"timestamp": ts_iso, "publisher_email": publisher_email, "body": b}) for b in bodies]
    try:
        col = _db.get_collection(COLLECTION_NAME, write_concern=WriteConcern(w="majority", j=True))
        res = col.bulk_write(ops, ordered=False, bypass_document_validation=True)
        return {"ok": True, "inserted": res.inserted_count}
    except PyMongoError as e:
        return {"ok": False, "error": str(e)}
