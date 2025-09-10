# To check the logs:
# docker compose logs -f mongo-stream-publisher cim-service-mock

# To recreate just the publisher and CIM receiver.
# docker compose up -d --force-recreate --no-deps cim-service-mock mongo-stream-publisher

docker compose exec -T metrics-db mongosh --quiet <<'JS'
const dbm = db.getSiblingDB("metricsdb");
dbm.metrics.insertOne({
  publisher_email: "235@example.org",
  idempotency_key: new ObjectId().toHexString(),
  seq: 35,
  body: { metric: "cpu.util", node: "edge-02", ts: new Date(), val: 0.91 }
});
JS