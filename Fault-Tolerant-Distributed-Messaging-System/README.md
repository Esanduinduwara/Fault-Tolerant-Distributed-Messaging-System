# ⚡ StreamFlow — Fault-Tolerant Distributed Messaging App
### SE2062 · Distributed Systems · Solo Build

---

## 📐 Architecture

```
User → REST API (FastAPI)
           │
           ▼
     Kafka (Topic: user-messages)
           │
           ▼
     Consumer (saves to DB + leader-only stats job)
           │
           ▼
     MongoDB (messages, users, logs, stats)
           │
           ▼
     REST API (GET /messages, /stats, /logs)
```

| Container            | Port  | Purpose                        |
|----------------------|-------|--------------------------------|
| streamflow_kafka     | 9092  | Message broker                 |
| streamflow_zookeeper | 2181  | Kafka coordination             |
| streamflow_mongodb   | 27017 | Persistent data store          |
| streamflow_kafdrop   | 9000  | Kafka browser UI               |
| streamflow_api       | 8000  | REST API + Swagger UI          |
| streamflow_consumer  | —     | Background message processor   |

---

## ✅ Grading Coverage

| Area                     | Where Implemented                                              | % |
|--------------------------|----------------------------------------------------------------|---|
| Part 1 · Fault Tolerance | `message_producer.py` — DLQ fallback, acks=all, retries=10    | 20|
| Part 2 · Replication     | `message_consumer.py` — consumer group, manual offset commit  | 20|
| Part 3 · Time & Order    | `mongodb_handler.py` — unique index, sorted reads, ms timestamps | 20|
| Part 4 · Consensus       | `message_consumer.py` — MongoDB TTL leader-lock, leader job   | 20|
| Part 5 · Integration     | `docker-compose.yml` — Kafdrop, health checks, full stack     | 20|

---

## 🚀 QUICK START (Full Docker — Recommended for Demo)

### Prerequisites
- Docker Desktop running
- Python 3.8+ with venv (for local dev & tests)

### Step 1 — Clone / unzip and open in PyCharm

Open the `streamflow/` folder as a PyCharm project.

### Step 2 — Copy the env file

In PyCharm terminal or Windows PowerShell:
```powershell
copy env.example .env
```

### Step 3 — Build and start everything

```powershell
cd docker
docker compose up --build -d
```

Wait ~60 seconds for all services to become healthy. Check:
```powershell
docker compose ps
```
All 6 rows should show **Up** (or **Up (healthy)**).

### Step 4 — Open the UIs

| URL                              | What you see                    |
|----------------------------------|---------------------------------|
| http://localhost:8000/docs       | Swagger UI — all API endpoints  |
| http://localhost:8000/health     | Live health status              |
| http://localhost:9000            | Kafdrop — browse Kafka topics   |

---

## 🧪 STEP-BY-STEP TESTING GUIDE

### ─── STAGE 1 · Run Unit Tests (no Docker needed) ───────────────────────

```powershell
# From the streamflow/ project root, activate venv first
python -m venv venv
venv\Scripts\Activate.ps1          # PowerShell
# OR
venv\Scripts\activate.bat          # Command Prompt

pip install -r requirements.txt

# Run all tests
pytest
```

**Expected output — all green:**
```
tests/test_producer.py::TestFaultTolerantProducer::test_send_message_success         PASSED
tests/test_producer.py::TestFaultTolerantProducer::test_send_message_adds_timestamp  PASSED
tests/test_producer.py::TestFaultTolerantProducer::test_dlq_fallback_on_kafka_error  PASSED
tests/test_producer.py::TestFaultTolerantProducer::test_conversation_key_format      PASSED
tests/test_producer.py::TestFaultTolerantProducer::test_send_batch_returns_stats     PASSED
tests/test_producer.py::TestFaultTolerantProducer::test_dlq_accumulates_multiple_failures PASSED
tests/test_consumer.py::TestFaultTolerantConsumer::test_process_message_success      PASSED
tests/test_consumer.py::TestFaultTolerantConsumer::test_process_message_db_failure_returns_false PASSED
tests/test_consumer.py::TestFaultTolerantConsumer::test_process_message_logs_node_id PASSED
tests/test_consumer.py::TestFaultTolerantConsumer::test_leader_lock_acquired_when_free PASSED
tests/test_consumer.py::TestFaultTolerantConsumer::test_leader_lock_rejected_when_held PASSED
tests/test_consumer.py::TestFaultTolerantConsumer::test_leader_stats_job_writes_to_db PASSED
tests/test_api.py::TestHealth::test_health_returns_200                               PASSED
tests/test_api.py::TestHealth::test_health_has_required_keys                         PASSED
tests/test_api.py::TestMessages::test_get_all_messages_empty                         PASSED
tests/test_api.py::TestMessages::test_send_message_returns_201                       PASSED
tests/test_api.py::TestMessages::test_send_message_missing_field_returns_422         PASSED
tests/test_api.py::TestMessages::test_get_conversation_sorted_by_field               PASSED
tests/test_api.py::TestMessages::test_get_conversation_calls_db_with_correct_users   PASSED
tests/test_api.py::TestUsers::test_create_user_returns_201                           PASSED
tests/test_api.py::TestUsers::test_create_duplicate_user_returns_409                 PASSED
tests/test_api.py::TestUsers::test_get_all_users_empty                               PASSED
tests/test_api.py::TestUsers::test_get_user_not_found_returns_404                    PASSED
tests/test_api.py::TestUsers::test_get_existing_user                                 PASSED
tests/test_api.py::TestStatsAndLogs::test_stats_no_data_returns_message              PASSED
tests/test_api.py::TestStatsAndLogs::test_logs_returns_list                          PASSED

26 passed in X.XXs
```

---

### ─── STAGE 2 · Verify Docker Stack is Running ──────────────────────────

```powershell
cd docker
docker compose ps
```

All 6 services should be Up. If not:
```powershell
docker compose logs kafka       # check Kafka
docker compose logs mongodb     # check MongoDB
docker compose logs api         # check API
```

---

### ─── STAGE 3 · Test API via Browser (Swagger UI) ───────────────────────

Open **http://localhost:8000/docs**

**Test 1 — Health Check**
Click `GET /health` → Try it out → Execute
Expected response:
```json
{ "status": "healthy", "mongodb": "ok", "kafka": "ok" }
```

**Test 2 — Create Users**
Click `POST /users` → Try it out → paste body → Execute
```json
{ "userId": "u001", "username": "alice", "email": "alice@example.com" }
```
Then create Bob:
```json
{ "userId": "u002", "username": "bob", "email": "bob@example.com" }
```
Expected: `201 Created`

**Test 3 — Send a Message**
Click `POST /messages` → Try it out → paste body → Execute
```json
{
  "messageId": "m001",
  "fromUser": "alice",
  "toUser": "bob",
  "content": "Hello Bob! This is Alice."
}
```
Expected:
```json
{ "status": "sent", "messageId": "m001", "route": "kafka" }
```

Send a few more messages:
```json
{ "messageId": "m002", "fromUser": "bob",   "toUser": "alice", "content": "Hey Alice!" }
{ "messageId": "m003", "fromUser": "alice", "toUser": "bob",   "content": "How are you?" }
{ "messageId": "m004", "fromUser": "bob",   "toUser": "alice", "content": "Doing great!" }
```

**Test 4 — Read Conversation (sorted)**
Click `GET /messages/{user1}/{user2}` → Try it out
- user1 = `alice`, user2 = `bob`
Expected: messages sorted by timestamp ascending + `"sorted_by": "timestamp_asc"`

**Test 5 — Check Kafka in Kafdrop**
Open **http://localhost:9000**
- Click on topic `user-messages`
- You should see all 4 messages listed with their partition and offset

**Test 6 — Check Consumer Logs**
```powershell
docker compose logs consumer --tail=20
```
You should see lines like:
```
[consumer-1] ✅  msg=m001  partition=0  offset=0
[consumer-1] ✅  msg=m002  partition=0  offset=1
```

**Test 7 — Check Stats (Leader Job)**
Wait ~30 seconds, then:
Click `GET /stats` → Try it out → Execute
Expected:
```json
{ "message_count": 4, "user_count": 2, "computed_at": 1234567890000 }
```

**Test 8 — Check System Logs**
Click `GET /logs` → Try it out → Execute
Expected: list of audit log entries, one per processed message.

---

### ─── STAGE 4 · Fault Tolerance Demo ────────────────────────────────────

#### Demo A — Consumer Crash & Recovery (Part 1 & 2)

**Step 1: Stop the consumer**
```powershell
docker compose stop consumer
```

**Step 2: Send messages while consumer is DOWN**
```powershell
# In PowerShell — send 3 messages while consumer is stopped
curl -X POST http://localhost:8000/messages `
  -H "Content-Type: application/json" `
  -d '{"messageId":"m_crash_1","fromUser":"alice","toUser":"charlie","content":"Message during outage 1"}'

curl -X POST http://localhost:8000/messages `
  -H "Content-Type: application/json" `
  -d '{"messageId":"m_crash_2","fromUser":"alice","toUser":"charlie","content":"Message during outage 2"}'
```

Messages go to Kafka but are NOT yet in MongoDB (consumer is down).

**Step 3: Verify messages are in Kafka but NOT in MongoDB yet**
- Kafdrop (http://localhost:9000) → should show new messages in the topic
- API: `GET /messages/alice/charlie` → should return empty or only old messages

**Step 4: Restart the consumer**
```powershell
docker compose start consumer
docker compose logs consumer --tail=20
```
Watch the consumer process all queued messages and commit them.

**Step 5: Verify recovery**
```powershell
curl http://localhost:8000/messages/alice/charlie
```
Expected: both `m_crash_1` and `m_crash_2` are now in MongoDB — **no message loss!**

---

#### Demo B — DLQ Fallback when Kafka is Completely Down (Part 1)

**Step 1: Run the producer locally (outside Docker)**
```powershell
# Make sure venv is active
venv\Scripts\Activate.ps1
```

**Step 2: Stop Kafka**
```powershell
docker compose stop kafka
```

**Step 3: Run the producer — it will fail and write to DLQ**
```powershell
python -m src.producer.message_producer
```

Expected output:
```
🚨 Kafka error for msg_xxxxx: ...
⚠️  Message saved to DLQ (failed_messages_dlq.jsonl): msg_xxxxx
```

**Step 4: Inspect the DLQ file**
Open `failed_messages_dlq.jsonl` in PyCharm — it contains the failed messages as JSON.

**Step 5: Restart Kafka**
```powershell
docker compose start kafka
```

---

#### Demo C — Leader Election (Part 4)

**Step 1: Scale to 2 consumers**
```powershell
docker compose up --scale consumer=2 -d
```

**Step 2: Watch the logs**
```powershell
docker compose logs consumer --tail=30 -f
```

You will see:
- Both instances log their partition assignments (only ONE gets each partition)
- After ~25s, ONE instance logs `👑 Elected as leader — running stats job`
- The OTHER instance does NOT run the stats job (only the leader does)

**Step 3: Kill the leader**
```powershell
docker compose stop consumer
docker compose up --scale consumer=1 -d
docker compose logs consumer --tail=20 -f
```
After 30 seconds (TTL expires), the remaining consumer acquires the leader lock.

---

### ─── STAGE 5 · Local Development (without Docker API/Consumer) ─────────

Use this when you want to debug in PyCharm with breakpoints.

**Terminal 1 — Infrastructure only**
```powershell
cd docker
docker compose up zookeeper kafka mongodb kafdrop -d
```

**Terminal 2 — Consumer**
```powershell
cd ..   # back to streamflow/
venv\Scripts\Activate.ps1
python -m src.consumer.message_consumer
```

**Terminal 3 — API**
```powershell
venv\Scripts\Activate.ps1
python -m src.api.rest_api
```

**Terminal 4 — Producer (send test messages)**
```powershell
venv\Scripts\Activate.ps1
python -m src.producer.message_producer
```

---

## 📁 Project Structure

```
streamflow/
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py          ← all config/env vars
│   ├── producer/
│   │   ├── __init__.py
│   │   └── message_producer.py  ← Kafka producer + DLQ
│   ├── consumer/
│   │   ├── __init__.py
│   │   └── message_consumer.py  ← Kafka consumer + leader election
│   ├── database/
│   │   ├── __init__.py
│   │   └── mongodb_handler.py   ← MongoDB CRUD + indexes
│   ├── api/
│   │   ├── __init__.py
│   │   └── rest_api.py          ← FastAPI endpoints
│   └── utils/
│       ├── __init__.py
│       └── helpers.py           ← shared utilities
├── docker/
│   ├── docker-compose.yml       ← full stack (6 services)
│   ├── api.Dockerfile
│   └── consumer.Dockerfile
├── tests/
│   ├── __init__.py
│   ├── test_producer.py         ← 6 tests
│   ├── test_consumer.py         ← 6 tests
│   └── test_api.py              ← 14 tests
├── docs/
├── conftest.py                  ← pytest path setup
├── pytest.ini                   ← pytest config
├── requirements.txt
├── env.example                  ← copy to .env
├── .gitignore
└── README.md
```

---

## ⚠️ Common Issues & Fixes

| Problem | Fix |
|---------|-----|
| `docker compose ps` shows services not healthy | Wait 60s more; Kafka takes time to start |
| `pytest` fails with `ModuleNotFoundError` | Make sure venv is active and `pip install -r requirements.txt` was run |
| API returns `"kafka": "down"` in health | Kafka is still starting — wait and refresh |
| PowerShell curl syntax error | Use `Invoke-WebRequest` or install curl for Windows |
| Port 9092 / 27017 already in use | Stop old Docker containers: `docker compose down` |
| `venv\Scripts\Activate.ps1` blocked | Run: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser` |

---

## 🔗 Key URLs (when full Docker stack is running)

- **Swagger UI:**  http://localhost:8000/docs
- **Health:**      http://localhost:8000/health
- **All messages:**http://localhost:8000/messages
- **Stats:**       http://localhost:8000/stats
- **Logs:**        http://localhost:8000/logs
- **Kafdrop:**     http://localhost:9000
