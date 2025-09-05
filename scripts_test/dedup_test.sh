docker compose exec -T metrics-db mongosh --quiet <<'JS'
const dbm = db.getSiblingDB("metricsdb");
dbm.metrics.deleteMany({testDedup: true});
const doc = {
  publisher_email: "partner@example.org",
  idempotency_key: "abc123",
  seq: 1,
  body: { metric: "cpu.util", node: "edge-01", ts: new Date("2025-09-05T10:00:00Z"), val: 0.75 },
  testDedup: true
};
print("-- first insert --");
printjson(dbm.metrics.insertOne(doc));
print("-- duplicate insert (should fail with E11000) --");
try { dbm.metrics.insertOne(doc); } catch(e) { print(e.code, e.codeName, e.errmsg); }
JS
