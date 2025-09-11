gzip -c big_example_REPLACE.ndjson | curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/x-ndjson" \
  -H "Content-Encoding: gzip" \
  --data-binary @- \
  https://mc-a4.lab.uvalight.net/gd-cim-api/submit/ndjson

##### TEST ######

### A. Bring services up
# 1) Set env
export JWT_TOKEN='<your_token>'
export MONGO_URI='mongodb://localhost:27017/'

# 2) Start Mongo (your docker-compose or local mongod)
docker compose up -d

# 3) Run API
uvicorn login_server:app --host 0.0.0.0 --port 8000

### B. Create a user and get a token
# touch allowed_emails.txt && echo 'you@example.org' >> allowed_emails.txt
curl -s -X POST -F 'username=goncalo.ferreira@student.uva.nl' -F 'password=goncalo' https://mc-a4.lab.uvalight.net/gd-cim-api/login
# Copy the JWT shown in the HTML response (or use /token-ui)

### C. Small single JSON
TOKEN=$JWT_TOKEN
curl -s -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"cpu":0.7,"mem":1536}'


### D. Test/submit/batch (array+idempotency)
curl -s -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/batch \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 11111111-1111-1111-1111-111111111111" \
  -H "X-Batch-Seq: 0" \
  -d '[{"metric":"cpu","value":0.1},{"metric":"mem","value":2}]'
# => {"ok":true,"inserted":2,"next_expected_seq":1}

# Retry same request to verify de-dup
curl -s -X POST https://mc-a4.lab.uvalight.net/gd-cim-api/submit/batch \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 11111111-1111-1111-1111-111111111111" \
  -H "X-Batch-Seq: 0" \
  -d '[{"metric":"cpu","value":0.1},{"metric":"mem","value":2}]'
# => {"ok":true,"inserted":0,"duplicate":true,"next_expected_seq":1}

### E. Test submit NDJSON (with and without gzip)
printf '%s\n' \
'{"metric":"cpu","v":0.11}' \
'{"metric":"cpu","v":0.12}' \
'{"metric":"mem","v":123}' > tiny.ndjson

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

### F. Submit chunks
# Gen chunks
python submit_api/gen_input.py # it will create a out_chunks folder with a manifest and .gz chunks.

python submit_api/json_to_ndjson_chunks.py input.json out_chunks \
  --gzip --exec-curl \
  --endpoint https://mc-a4.lab.uvalight.net/gd-cim-api/submit/ndjson \
  --bearer "$TOKEN"

### G. Test with batch
curl -sS -X POST "$URL/gd-cim-api/submit/batch" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "X-Batch-Seq: 0" \
  --data-binary @input.json

