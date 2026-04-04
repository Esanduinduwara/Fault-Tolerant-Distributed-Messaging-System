

---

```markdown
# 🚀 Fault-Tolerant Distributed Messaging App  
### SE2062 · Distributed Systems  

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

````

---

## 🧩 System Components

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

### 🔧 Prerequisites
- Docker Desktop running  
- Python 3.8+ with venv (for local dev & tests)  

---

### 📦 Step 1 — Clone / unzip and open in PyCharm

Open the `Fault-Tolerant-Distributed-Messaging-System` folder as a PyCharm project.

---

### 📄 Step 2 — Copy the env file

```powershell
copy env.example .env
````

---

### 🐳 Step 3 — Build and start everything

```powershell
cd docker
docker compose up --build -d
```

⏳ Wait ~60 seconds for all services to become healthy.

```powershell
docker compose ps
```

All 6 rows should show **Up** (or **Up (healthy)**).

---

### 🌐 Step 4 — Open the UIs

| URL                                                          | What you see                   |
| ------------------------------------------------------------ | ------------------------------ |
| [http://localhost:8000/docs](http://localhost:8000/docs)     | Swagger UI — all API endpoints |
| [http://localhost:8000/health](http://localhost:8000/health) | Live health status             |
| [http://localhost:9000](http://localhost:9000)               | Kafdrop — browse Kafka topics  |

---

# 🧪 STEP-BY-STEP TESTING GUIDE

---

## 🟢 STAGE 1 · Run Unit Tests (no Docker needed)

```powershell
python -m venv venv
venv\Scripts\Activate.ps1
# OR
venv\Scripts\activate.bat

pip install -r requirements.txt
pytest
```

### ✅ Expected output

```
26 passed in X.XXs
```

---

## 🟡 STAGE 2 · Verify Docker Stack is Running

```powershell
cd docker
docker compose ps
```

If issues:

```powershell
docker compose logs kafka
docker compose logs mongodb
docker compose logs api
```

---

## 🔵 STAGE 3 · Test API via Swagger UI

👉 Open: [http://localhost:8000/docs](http://localhost:8000/docs)

Click endpoints → **Try it out → Execute**

---

### 🧪 Test Sequence

#### 1️⃣ Health Check

`GET /health`

```json
{
  "status": "healthy",
  "mongodb": "ok",
  "kafka": "ok"
}
```

---

#### 2️⃣ Create User Alice

```json
POST /users
{
  "userId": "u001",
  "username": "alice",
  "email": "alice@streamflow.com"
}
```

---

#### 3️⃣ Create User Bob

---

#### 4️⃣ Duplicate User Test

```json
{ "detail": "User already exists" }
```

---

#### 5️⃣ Send Message (Kafka route)

```json
{
  "status": "sent",
  "messageId": "m001",
  "route": "kafka"
}
```

---

#### 📊 Kafdrop Check

[http://localhost:9000](http://localhost:9000) → `user-messages` topic

---

#### 6–7️⃣ More Messages

---

#### 8️⃣ Read Conversation

```json
{
  "messages": [...],
  "count": 3,
  "sorted_by": "timestamp_asc"
}
```

✔ Chronological ordering
✔ Millisecond timestamps

---

#### 9️⃣ Logs

`GET /logs`

✔ Shows processing audit trail

---

#### 🔟 Stats (after ~30s)

```json
{
  "message_count": 3,
  "user_count": 2,
  "computed_at": 1234567890000
}
```

✔ Leader-only job

---

#### 1️⃣1️⃣ Validation Test

Returns **422 error**

---

# ⚡ Fault Tolerance Demo Guide

---

## 🧪 Demo A — Consumer Crash & Recovery

```powershell
docker compose stop consumer
```

➡ Send messages → queued in Kafka

```powershell
docker compose start consumer
```

✔ Messages processed
✔ No loss

---

## 🧪 Demo B — DLQ (Kafka Down)

```powershell
docker compose stop kafka
python -m src.producer.message_producer
```

✔ Messages saved to:

```
failed_messages_dlq.jsonl
```

---

## 🧪 Demo C — Leader Election

```powershell
docker compose up --scale consumer=2 -d
```

✔ One leader only
✔ Others idle for stats job

---

# 📁 Project Structure

```
streamflow/
├── src/
│   ├── config/
│   ├── producer/
│   ├── consumer/
│   ├── database/
│   ├── api/
│   └── utils/
├── docker/
├── tests/
├── docs/
├── requirements.txt
├── env.example
└── README.md
```

---

# ⚠️ Common Issues & Fixes

| Problem              | Fix                     |
| -------------------- | ----------------------- |
| Services not healthy | Wait 60s                |
| pytest fails         | Activate venv           |
| Kafka down           | Wait more               |
| Ports in use         | `docker compose down`   |
| PowerShell issue     | Use `Invoke-WebRequest` |

---

# 🔗 Key URLs

* Swagger UI → [http://localhost:8000/docs](http://localhost:8000/docs)
* Health → [http://localhost:8000/health](http://localhost:8000/health)
* Messages → [http://localhost:8000/messages](http://localhost:8000/messages)
* Stats → [http://localhost:8000/stats](http://localhost:8000/stats)
* Logs → [http://localhost:8000/logs](http://localhost:8000/logs)
* Kafdrop → [http://localhost:9000](http://localhost:9000)

---

```


```
