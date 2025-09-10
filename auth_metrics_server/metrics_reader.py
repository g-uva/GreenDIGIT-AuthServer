import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient, ASCENDING, ReturnDocument
from bson.objectid import ObjectId

"""
# All metrics (optionally filtered)
docs = get_all_metrics()                       # everything
docs = get_all_metrics("alice@example.org", 100)

# Incremental since last run
last_ts, last_id = get_cursor("kv_exporter")
docs, new_ts, new_id = get_metrics_since(last_ts, last_id)
# ...process docs...
save_cursor("kv_exporter", new_ts, new_id)
"""

MONGO_URI = os.getenv("MONGO_URI", "mongodb://metrics-db:27017/")
DB_NAME = os.getenv("METRICS_DB_NAME", "metricsdb")
COLLECTION_NAME = os.getenv("METRICS_COLLECTION", "metrics")
CURSORS_COLLECTION = os.getenv("CURSORS_COLLECTION", "cursors")

_client = MongoClient(MONGO_URI)
_db = _client[DB_NAME]
_col = _db[COLLECTION_NAME]
_cursors = _db[CURSORS_COLLECTION]

# Ensure helpful indexes
_col.create_index([("publisher_email", ASCENDING)], name="ix_publisher_email")
_col.create_index([("timestamp", ASCENDING)], name="ix_timestamp")

def _to_dict(doc) -> Dict[str, Any]:
    d = dict(doc)
    d["_id"] = str(d["_id"])
    return d

def get_all_metrics(publisher_email: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return all metrics (optionally filtered), newest first."""
    q: Dict[str, Any] = {}
    if publisher_email:
        q["publisher_email"] = publisher_email
    cursor = _col.find(q).sort("timestamp", -1)
    if limit:
        cursor = cursor.limit(int(limit))
    return [_to_dict(d) for d in cursor]

def get_metrics_since(
    since_ts_iso: Optional[str] = None,
    since_id: Optional[str] = None,
    publisher_email: Optional[str] = None,
    limit: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Return metrics created after a watermark (timestamp and/or ObjectId).
    Use both for robustness: timestamp can tie, ObjectId is monotonic.
    Returns: (docs, last_ts_iso, last_id)
    """
    q: Dict[str, Any] = {}
    if publisher_email:
        q["publisher_email"] = publisher_email

    # Build "greater than" conditions
    gt_clauses: List[Dict[str, Any]] = []
    if since_ts_iso:
        gt_clauses.append({"timestamp": {"$gt": since_ts_iso}})
    if since_id:
        gt_clauses.append({"_id": {"$gt": ObjectId(since_id)}})

    if gt_clauses:
        q["$or"] = gt_clauses

    cursor = _col.find(q).sort([("timestamp", 1), ("_id", 1)])
    if limit:
        cursor = cursor.limit(int(limit))

    docs = list(cursor)
    if not docs:
        return [], since_ts_iso, since_id

    last = docs[-1]
    last_ts_iso = last.get("timestamp")
    last_id = str(last["_id"])
    return ([_to_dict(d) for d in docs], last_ts_iso, last_id)

# --- Cursor helpers (per-processor watermark) ---

def get_cursor(processor_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Read last watermark for a processor (e.g., 'kv_exporter').
    Returns (last_ts_iso, last_id).
    """
    c = _cursors.find_one({"name": processor_name})
    if not c:
        return None, None
    return c.get("last_ts_iso"), c.get("last_id")

def save_cursor(processor_name: str, last_ts_iso: Optional[str], last_id: Optional[str]) -> Dict[str, Any]:
    """Upsert the watermark for a processor."""
    doc = {
        "name": processor_name,
        "last_ts_iso": last_ts_iso,
        "last_id": last_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    return _cursors.find_one_and_update(
        {"name": processor_name},
        {"$set": doc},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
