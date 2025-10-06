# USAccidents – usaccidents_app

FastAPI + SQLAlchemy 2.x + MySQL (pymysql) service that ingests Ohio OHGO incidents/roads.

## Quickstart (local)
```bash
python3.9 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit DATABASE_URL and OHGO_API_KEY
alembic upgrade head
./run_local.sh
```

## Endpoints
- `GET /healthz`
- `GET /incidents/latest?limit=25`
- `GET /incidents/changed_since?since=2025-10-01T00:00:00Z&limit=100`
- `POST /ingest/ohio/fetch?page_size=100`
- `POST /ingest/ohio/roads`

## Curl tests
```bash
curl -s http://127.0.0.1:8000/healthz
curl -s "http://127.0.0.1:8000/incidents/latest?limit=5" | jq .
curl -s "http://127.0.0.1:8000/incidents/changed_since?since=2025-10-01T00:00:00Z&limit=5" | jq .
curl -s -X POST "http://127.0.0.1:8000/ingest/ohio/fetch?page_size=50"
curl -s -X POST http://127.0.0.1:8000/ingest/ohio/roads
```
