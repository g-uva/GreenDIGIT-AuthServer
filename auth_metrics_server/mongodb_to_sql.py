# mongodb_to_sql.py
import os
from typing import Any, Dict, Iterable, List, Tuple, Optional
from datetime import datetime
import json

from psycopg2 import connect
from psycopg2.extras import execute_values

from metrics_reader import get_cursor, save_cursor, get_metrics_since

# --- Config via env ---
PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@postgres:5432/postgres")
PROCESSOR_NAME = os.getenv("PROCESSOR_NAME", "kv_exporter")

# --- Flatten helpers ---
def flatten(d: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Flatten dict/list into dotted keys. Lists use numeric indices.
    e.g. {"labels":{"node":"n1"}, "arr":[10,20]} ->
         {"labels.node":"n1", "arr.0":10, "arr.1":20}
    """
    if out is None:
        out = {}
    if isinstance(d, dict):
        for k, v in d.items():
            flatten(v, f"{prefix}{k}." if prefix else f"{k}.", out)
    elif isinstance(d, list):
        for i, v in enumerate(d):
            flatten(v, f"{prefix}{i}.", out)
    else:
        out[prefix[:-1]] = d
    return out

# --- DDL (idempotent) ---
DDL = """
CREATE TABLE IF NOT EXISTS public.metrics_kv (
    id BIGSERIAL PRIMARY KEY,
    source_oid TEXT NOT NULL,                -- Mongo _id as string
    publisher_email TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    key TEXT NOT NULL,
    value_text TEXT NULL,
    value_numeric DOUBLE PRECISION NULL,
    value_json JSONB NULL,
    UNIQUE (source_oid, key)                 -- idempotency guard
);
CREATE INDEX IF NOT EXISTS ix_metrics_kv_email_ts ON public.metrics_kv (publisher_email, ts);
CREATE INDEX IF NOT EXISTS ix_metrics_kv_key ON public.metrics_kv (key);
"""

def ensure_schema():
    with connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()

def cast_value(v: Any) -> Tuple[Optional[str], Optional[float], Optional[str]]:
    """
    Decide how to store a value:
    - numbers -> value_numeric
    - strings/bools -> value_text
    - other (dict/list) -> value_json
    """
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return None, float(v), None
    if isinstance(v, (str, bool)) or v is None:
        return str(v), None, None
    return None, None, json.dumps(v, separators=(",", ":"))

def rows_from_metric(m: Dict[str, Any]) -> List[Tuple]:
    """
    Convert one Mongo metric doc into many KV rows for Postgres.
    Each row: (publisher_email, ts, key, value_text, value_numeric, value_json)
    """
    email = m["publisher_email"]
    ts = m["timestamp"]              # ISO 8601
    source_oid = m["_id"]            # already string in metrics_reader._to_dict
    body = m.get("body", {})
    flat = flatten(body)
    rows: List[Tuple] = []
    for k, v in flat.items():
        vt, vn, vj = cast_value(v)
        rows.append((source_oid, email, ts, k, vt, vn, vj))
    return rows

def export_incremental(limit: Optional[int] = None) -> int:
    """
    Export only new metrics since last watermark.
    Returns number of Postgres rows inserted.
    """
    last_ts, last_id = get_cursor(PROCESSOR_NAME)
    docs, new_ts, new_id = get_metrics_since(last_ts, last_id, limit=limit)

    if not docs:
        return 0

    all_rows: List[Tuple] = []
    for m in docs:
        all_rows.extend(rows_from_metric(m))

    ensure_schema()
    with connect(PG_DSN) as conn, conn.cursor() as cur:
          execute_values(
               cur,
               """
               INSERT INTO public.metrics_kv
                    (source_oid, publisher_email, ts, key, value_text, value_numeric, value_json)
               VALUES %s
               ON CONFLICT (source_oid, key) DO NOTHING
               """,
               all_rows,
               page_size=1000,
          )
          conn.commit()

    save_cursor(PROCESSOR_NAME, new_ts, new_id)
    return len(all_rows)

def export_full(publisher_email: Optional[str] = None, limit: Optional[int] = None) -> int:
    """
    Export all metrics (optionally filtered). Does not update cursor.
    """
    from metrics_reader import get_all_metrics
    docs = get_all_metrics(publisher_email=publisher_email, limit=limit)
    if not docs:
        return 0
    all_rows: List[Tuple] = []
    for m in docs:
        all_rows.extend(rows_from_metric(m))
    ensure_schema()
    with connect(PG_DSN) as conn, conn.cursor() as cur:
          execute_values(
               cur,
               """
               INSERT INTO public.metrics_kv
                    (source_oid, publisher_email, ts, key, value_text, value_numeric, value_json)
               VALUES %s
               ON CONFLICT (source_oid, key) DO NOTHING
               """,
               all_rows,
               page_size=1000,
          )
          conn.commit()
    return len(all_rows)

if __name__ == "__main__":
    inserted = export_incremental()
    print(f"Inserted {inserted} rows into Postgres.")
