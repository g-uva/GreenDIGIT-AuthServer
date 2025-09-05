# Of course, change the content everytime, otherwise it is just going to dedup :)
docker compose exec -T metrics-db mongosh --quiet <<'JS'
const dbm = db.getSiblingDB("metricsdb");
dbm.metrics.insertOne({
  publisher_email: "partner@example.org",
  idempotency_key: "xyz999",
  seq: 42,
  body: { metric: "cpu.util", node: "edge-02", ts: new Date(), val: 0.91 }
});
JS