# vitals-service

vitals-service — domain: ehr

- **Port:** 8307
- **Language:** Python 3.11 + Flask
- **Database:** `ehr` (Postgres, table `vitals`)
- **Event bus:** Kafka

## API

| Method    | Path                       |
|-----------|----------------------------|
| GET       | `/api/vitals/`          |
| POST      | `/api/vitals/`          |
| GET       | `/api/vitals/<id>`      |
| PUT/PATCH | `/api/vitals/<id>`      |
| DELETE    | `/api/vitals/<id>`      |
| GET       | `/health`                  |
| GET       | `/ready`                   |

## Events

**Publishes:** (none)
**Subscribes:** encounter.started, device.reading

## HTTP peer dependencies

- `patients-service`
- `encounters-service`
- `audit-log-service`

## Local dev

```bash
pip install -e ../../libs/py-healthcare-common
pip install -r requirements.txt
cp .env.example .env
(cd ../../infra && docker compose up -d postgres kafka kafka-init)
python -m app.main
```

## Tests

```bash
pytest
```
