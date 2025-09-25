## 🌱🌍♻️ GreenDIGIT WP6 Metric Submission and Authentication Server 

### Overview

This service provides a secure API for collecting and aggregating CIM metrics from authorised partners. Authentication is managed via a list of allowed emails and token-based access. The service is built with FastAPI and runs on a Uvicorn server, designed for easy integration and future extensibility.

*This work is funded from the European Union’s Horizon Europe research and innovation programme through the [GreenDIGIT project](https://greendigit-project.eu/), under the grant agreement No. [101131207](https://cordis.europa.eu/project/id/101131207)*.

<!-- ![GreenDIGIT Logo](auth_metrics_server/static/cropped-GD_logo.png)
![EU Logo](auth_metrics_server/static/EN-Funded-by-the-EU-POS-2.png) -->

<div style="display:flex;align-items:center;width:100%;">
  <img src="auth_metrics_server/static/EN-Funded-by-the-EU-POS-2.png" alt="EU Logo" width="250px">
  <img src="auth_metrics_server/static/cropped-GD_logo.png" alt="GreenDIGIT Logo" width="110px" style="margin-right:100px">
</div>

### Data Storage
- **Metrics Storage:**  
  Submitted metrics will be transformed and stored in a SQL-compatible format (PostgreSQL) and organised into appropriate namespaces for future querying and analysis.

### Deployment

- The service runs on a Uvicorn server (default port: `8080`).
- Endpoints will be reverse-proxied via Nginx in production.
- Docker support is available for easy deployment.

### Usage
#### Authentication
- Obtain a token via **POST /login** using form fields `email` and `password`. Your email must be registered beforehand, **and the password will be set on the first time you enter it**. In case this does not work (wrong password/unknown), please contact goncalo.ferreira@student.uva.nl or a.tahir2@uva.nl.
- Then include `Authorization: Bearer <token>` on all protected requests.
- Tokens expire after 1 day—in which case you must simply repeat the process again.

#### Login & Content submission
Below you can find a simple example of a `curl` script to submit the metrics. You use any platform for this effect, as long as the HTTP request is valid.
```sh
curl -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"cpu_watts": 11.2, "mem_bytes": 734003200}'
```

#### List metrics
Below you can find another `curl` script in order to list the metrics, if needed.
```sh
curl -X GET -H "Authorization: Bearer <TOKEN>" https://mc-a4.lab.uvalight.net/gd-cim-api/metrics/me
```

To check the FastAPI documentation, please visit: [mc-a4.lab.uvalight.net/gd-cim-api/docs](https://mc-a4.lab.uvalight.net/gd-cim-api/docs).

---

### User Registration & Authentication

- **Allowed Emails:**  
  Only users whose emails are listed in `allowed_emails.txt` (managed by UvA) can access the service. This file is maintained locally on the server and can only be modified by UvA administrators.

- **First Login & Password Setup:**  
  On their first login at the `/login` endpoint, users provide their email and set a password. If the email is in `allowed_emails.txt` and not yet registered, the password is securely stored in a local SQLite database.

- **Token Retrieval:**  
  After successful login, users receive a JWT token. This token must be included as a Bearer token in the Authorisation header for all subsequent API requests.

### API Endpoints
We use [FastAPI](https://fastapi.tiangolo.com/)—a simple Python RESTful API server, that follows the OpenAPI standards. Therefore, it also serves all the specifications as you would expect from any OpenAPI server (e.g., if you access `/docs` or `/redocs` you should see all HTTP Request methods).

- **`POST /login`**  
  Accepts email and password (form data). On first login, sets the password; on subsequent logins, authenticates the user and returns a JWT token.

- **`GET /token-ui`**  
  A simple HTML form for manual login and token retrieval.

---

**Submission API**

- **`POST /submit`**  
  Accepts a single JSON object containing metrics. Requires a valid Bearer token in the `Authorization` header. The submitted metrics are validated and stored.
```bash
curl -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"metric":"cpu.util","value":0.73,"ts":"2025-09-01T10:02:03Z","node":"compute-0"}'
```

<!-- - **`POST /submit/batch`**  
  Accepts a JSON **array** of metric objects and writes them in bulk. Requires `Idempotency-Key` and `Batch-Seq` headers to allow safe retries without duplicate inserts.
```bash
curl -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/batch \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Batch-Seq: 0" \
  --data-binary @input.json

```

- **`POST /submit/ndjson`**  
  Accepts newline-delimited JSON (**NDJSON**) via streaming, optionally compressed with `gzip`. This is the recommended endpoint for large-scale ingestion. Supports optional `Idempotency-Key` and `Batch-Seq` headers for idempotent, resumable uploads.

Example (plain NDJSON):
```bash
curl -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/batch \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Batch-Seq: 0" \
  --data-binary @input.json
```

Example (gzipped NDJSON + Idempotency):
```bash
curl -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/batch \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Batch-Seq: 0" \
  --data-binary @input.json
```

--- -->

**Payload size limits (important)**  
- MongoDB restricts a **single document to 16 MB**.  
- HTTP itself has no fixed size limit, but most servers and proxies enforce practical limits (often **1–100 MB per request**).  
- For **large datasets (hundreds of MBs to GBs)**, always split into chunks (NDJSON streaming or JSON array batches).  

<!-- ### JSON to NDJSON Chunks Helper
In order to help convert your JSON into digestable chunks, we've developed a helper that you can use to split your JSON file `submit_api/chunk_service/*.py`.

Basic conversion (auto-detects input format, writes chunks + manifest):
```sh
python submit_api/chunk_service/json_to_ndjson_chunks.py input.json submit_api/chunk_service/test_data/out_dir
```

Main options:
- `--chunk-size 10000` (default is 10k)
- `--gzip` to write `.ndjson.gz`
- `--input-format array|ndjson|auto` (default `auto`)
- `--idem-key <uuid>` (otherwise generated)
- `--prefix chunk` (file prefix, default `chunk`)
- `--start-seq 0` (first `X-Batch-Seq`)

Generate curl commands (but don’t run them):
```sh
python submit_api/chunk_service/json_to_ndjson_chunks.py submit_api/chunk_service/test_data/input.json submit_api/chunk_service/test_data/out_dir \
  --emit-curl \
  --endpoint https://mc-a4.lab.uvalight.net/gd-cim-api/submit/ndjson \
  --bearer "$TOKEN"
```

Execute uploads (requires `curl` installed):
```sh
# generate UUID and save
IDEM_KEY_LOC="submit_api/chunk_service/test_data/idem_key.txt"
uuidgen > $IDEM_KEY_LOC

# reuse later
IDEM=$(cat $IDEM_KEY_LOC)
echo "Using Idempotency-Key=$IDEM"

python submit_api/chunk_service/json_to_ndjson_chunks.py submit_api/chunk_service/test_data/input.json submit_api/chunk_service/test_data/out_dir \
  --idem-key "$IDEM" --exec-curl --verbose \
  --resume-from 50 \
  --endpoint https://mc-a4.lab.uvalight.net/gd-cim-api/submit/ndjson \
  --bearer "$TOKEN" \
  --log-file upload.log
```

```sh
# This will retrieve the the metrics by email:
docker exec -it submit_api-metrics-db-1 mongosh

# Get metrics per email
use metricsdb
db.ingest_sessions.find({ publisher_email: "goncalo.ferreira@student.uva.nl", idempotency_key: "57f8c2cd-d9ae-4d90-bd87-4cdcb0624a35" })

use metricsdb
# Count
db.metrics.countDocuments({ publisher_email: "goncalo.ferreira@student.uva.nl" })
# Show just the "body" field (the actual metric)
db.metrics.find({ publisher_email: "goncalo.ferreira@student.uva.nl" }, { _id: 0, body: 1 }).limit(10).pretty()


### DEV CLEANUP FOR A FRESH RUN (to test resume)
db.ingest_sessions.updateMany(
  { publisher_email: "goncalo.ferreira@student.uva.nl",
    idempotency_key: "57f8c2cd-d9ae-4d90-bd87-4cdcb0624a35",
    status: "in_progress"
  },
  { $set: { status: "stale" } }
)

# Delete all entries
db.ingest_sessions.deleteMany({ idempotency_key: "57f8c2cd-d9ae-4d90-bd87-4cdcb0624a35" })

# For the user to retrieve their metrics using the endpoint
curl -sS -H "Authorization: Bearer $TOKEN" \
  https://mc-a4.lab.uvalight.net/gd-cim-api/metrics/me | jq .

# Just the metrics' payload:
curl -sS -H "Authorization: Bearer $TOKEN" \
  https://mc-a4.lab.uvalight.net/gd-cim-api/metrics/me | jq '.[].body'
```

```sh
# This is the command used to start/resume the submission of chunks.
export TOKEN=$(cat submit_api/chunk_service/test_data/token_key.txt)
export IDEM=$(cat submit_api/chunk_service/test_data/idem_key.txt)
python -u submit_api/chunk_service/json_to_ndjson_chunks.py submit_api/chunk_service/test_data/input.json submit_api/chunk_service/test_data/out_dir \
  --idem-key "$IDEM" --exec-curl --auto-resume --verbose \
  --status-endpoint https://mc-a4.lab.uvalight.net/gd-cim-api/ingest/status \
  --endpoint        https://mc-a4.lab.uvalight.net/gd-cim-api/submit/ndjson \
  --bearer "$TOKEN"
```

### Batch/chunk tests (for dev, not end-user!)
#### A. Bring services up
```bash
# 1) Set env
export JWT_TOKEN='<your_token>'
export MONGO_URI='mongodb://localhost:27017/'

# 2) Start Mongo (your docker-compose or local mongod)
docker compose up -d

# 3) Run API
uvicorn login_server:app --host 0.0.0.0 --port 8000
```

#### B. Create a user and get a token
```bash
# touch allowed_emails.txt && echo 'you@example.org' >> allowed_emails.txt
curl -s -X POST -F 'username=goncalo.ferreira@student.uva.nl' -F 'password=goncalo' https://mc-a4.lab.uvalight.net/gd-cim-api/login
# Copy the JWT shown in the HTML response (or use /token-ui)
```

#### C. Small single JSON
```bash
TOKEN=$JWT_TOKEN
curl -s -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"cpu":0.7,"mem":1536}'
```

#### D. Test /submit/batch (array + idempotency)
```bash
curl -s -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/batch \
  -H "Authorization: Bearer $TOKEN"   -H "Content-Type: application/json" \
  -H "Idempotency-Key: 11111111-1111-1111-1111-111111111111" \
  -H "X-Batch-Seq: 0" \
  -d '[{"metric":"cpu","value":0.1},{"metric":"mem","value":2}]'
# => {"ok":true,"inserted":2,"next_expected_seq":1}

# Retry same request to verify de-dup
curl -s -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/batch \
  -H "Authorization: Bearer $TOKEN"   -H "Content-Type: application/json" \
  -H "Idempotency-Key: 11111111-1111-1111-1111-111111111111" \
  -H "X-Batch-Seq: 0" \
  -d '[{"metric":"cpu","value":0.1},{"metric":"mem","value":2}]'
# => {"ok":true,"inserted":0,"duplicate":true,"next_expected_seq":1}
```

#### E. Test /submit/ndjson (with and without gzip)
```bash
printf '%s\n' '{"metric":"cpu","v":0.11}' '{"metric":"cpu","v":0.12}' '{"metric":"mem","v":123}' > tiny.ndjson

# Plain
curl -s -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/ndjson \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/x-ndjson" \
     --data-binary @tiny.ndjson

# Gzipped
gzip -c tiny.ndjson | curl -s -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/ndjson \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/x-ndjson" \
     -H "Content-Encoding: gzip" \
     --data-binary @-
```

#### F. Gen and submit chunks
```bash
# Gen chunks
python submit_api/chunk_service/gen_input.py  # creates input.json and out_chunks/ with a manifest and .gz chunks

# This command generates and automatically executes the submission.
python submit_api/submit_api/chunk_service/json_to_ndjson_chunks.py input.json out_chunks \
  --gzip --exec-curl \
  --endpoint https://mc-a4.lab.uvalight.net/gd-cim-api/submit/ndjson \
  --bearer "$TOKEN"
```

#### G. Test with batch
```bash
curl -sS -X POST "https://mc-a4.lab.uvalight.net/gd-cim-api/submit/batch" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "X-Batch-Seq: 0" \
  --data-binary @input.json
```

### Internal Mongo -> CIM -> CI -> SQL API Publisher CNR
1. **Mongo -> CIM**: Test insert data (metrics JSON into MongoDB)
```bash
docker compose exec -T metrics-db mongosh --quiet <<'JS'
const dbm = db.getSiblingDB("metricsdb");
dbm.metrics.insertOne({
  publisher_email: "goncalo.ferreira@student.uva.nl",
  idempotency_key: new ObjectId().toHexString(),
  seq: 35,
  body: { metric: "cpu.util", node: "100IT", ts: new Date(), val: 0.91 }
});
JS
```

Some helpers to restart services.
> `docker compose logs -f mongo-stream-publisher cim-service-mock`
> `docker compose logs -f mongo-stream-publisher ci-calc`
> `docker compose up -d --force-recreate --no-deps cim-service-mock mongo-stream-publisher`

### Restart MetricsDB from scratch with healthchecks
```sh
# Pause the change-stream publisher
docker compose stop mongo-stream-publisher

# Quick replica-set heatlh snapshot
docker compose exec metrics-db mongosh --quiet --eval 'rs.status().members.map(m=>({name:m.name, state:m.stateStr, health:m.health}))'

# Count document before wipe
docker compose exec metrics-db mongosh "mongodb://metrics-db:27017,metrics-db-2:27017,metrics-db-3:27017/?replicaSet=rs0" --quiet --eval \
'db.getSiblingDB("metricsdb").metrics.countDocuments({})'

# OPTIONAL: peek at a few docs (no _id)
docker compose exec metrics-db mongosh --quiet --eval \
'db.getSiblingDB("metricsdb").metrics.find({}, {_id:0}).limit(5).toArray()'

# Drop the whole database (fastest clean slate)
docker compose exec metrics-db mongosh --quiet --eval 'db.getSiblingDB("metricsdb").dropDatabase()'

# Re-run your one-shot index job (safe to re-run)
docker compose run --rm mongo-indexes

# Verify indexes exist
docker compose exec metrics-db mongosh --quiet --eval \
'db.getSiblingDB("metricsdb").metrics.getIndexes().map(i=>i.name)'
``` -->


```sh
# See the DB & indexes exist
docker compose exec ci-retain-db mongosh --quiet --eval '
c=db.getSiblingDB("ci-retainment-db");
printjson(c.pending_ci.getIndexes());
'

# See pending invalid entries (if any)
docker compose exec ci-retain-db mongosh --quiet --eval '
db.getSiblingDB("ci-retainment-db").pending_ci.find({valid:false})
  .project({_id:0, lat:1, lon:1, request_time:1, creation_time:1}).limit(5).toArray()
'

# Verify that a doc landed on the retain-db
docker compose exec ci-retain-db-1 mongosh --quiet --eval '
c=db.getSiblingDB("ci-retainment-db");
printjson(c.pending_ci.getIndexes());
printjson(c.pending_ci.find({valid:false}).limit(1).toArray());
'
## or

docker compose exec ci-retain-db-1 mongosh --quiet --eval '
c=db.getSiblingDB("ci-retainment-db");
printjson(c.pending_ci.getIndexes());
printjson(c.pending_ci.find({valid:false}).limit(1).toArray());
'

# Verify the replica set.
docker compose exec ci-retain-db-1 mongosh --quiet --eval 'rs.status().members.map(m=>({name:m.name,state:m.stateStr,health:m.health}))'

# Initialising the retain DBs, in case it is not done by docker compose
docker compose exec ci-retain-db-1 mongosh --quiet --eval '
rs.initiate({_id:"rs0",members:[
 {_id:0,host:"ci-retain-db-1:27017"},
 {_id:1,host:"ci-retain-db-2:27017"},
 {_id:2,host:"ci-retain-db-3:27017"}
]})'

# Cleanup the retain DB in case something goes wrong.
docker compose exec ci-retain-db-1 mongosh --quiet --eval '
db.getSiblingDB("ci-retainment-db").pending_ci.deleteMany({})
'
```

CNR-Adapter service handy scripts:
```sh
curl -X GET http://sql-cnr-adapter:8033/entries/by-site/25
curl -X DELETE http://sql-cnr-adapter:8033/entries/by-site/25

# To pretty print:
curl -X GET http://sql-cnr-adapter:8033/entries/by-site/26 | jq

curl -s -X POST http://sql-cnr-adapter:8033/cnr-sql-adapter \
  -H 'Content-Type: application/json' \
  --data-binary @- <<'JSON'
{
  "site": "AEGIS01-IPB-SCL",
  "duration_s": 3590,
  "sites": { "site_type": "cloud" },
  "fact_site_event": {
    "site": "AEGIS01-IPB-SCL",
    "event_start_timestamp": "2025-01-01T12:00:00Z",
    "event_end_timestamp": "2025-01-01T13:00:00Z",
    "execunitfinished": true,
    "job_finished": true,
    "startexectime": "2025-01-01T12:00:05Z",
    "stopexectime": "2025-01-01T12:59:55Z",
    "execunitid": "compute-12345"
  },
  "detail_cloud": { "execunitid": "compute-12345" }
}
JSON
```

### Integration & Next Steps (Roadmap)
- [x] Separate DB from API, because I am guessing that if we rebuild using Docker, this will reset the DB @goncalo.
- [x] Step by step tutorial for: (1) run uvicorn locally, (2) running Dockerfile (server context), showing the endpoints (UI, OpenAPI, and others)
- [x] Integrate in the server :point_right: mc-a4.lab.uvalight.net using a reverse-proxy NGINX.
- [ ] Integrate `POST` service for CNR database—do this programmatically.
- [ ] Deploy and connect CIM service (transformation).
- [ ] Further discussions will determine the best approach for transforming and storing metrics, as well as any additional integration requirements.

## Contact & Questions
**Contact:**  
For questions or to request access, please contact the GreenDIGIT UvA team:
- Gonçalo Ferreira: goncalo.ferreira@student.uva.nl.
