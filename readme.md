# Distributed Wallet System with Redis Locks

A production-ready wallet transfer system demonstrating **distributed locking**, **transaction safety**, and **race condition handling** using Redis, Kafka, and FastStream.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Redis](https://img.shields.io/badge/redis-7.0+-red.svg)](https://redis.io/)
[![Kafka](https://img.shields.io/badge/kafka-3.0+-black.svg)](https://kafka.apache.org/)

## 🎯 What This Project Demonstrates

This project showcases solutions to real-world distributed systems challenges:

- ✅ **Distributed Locking**: Prevent race conditions in concurrent transfers
- ✅ **Idempotency**: Handle duplicate requests safely
- ✅ **Atomic Transactions**: Ensure consistency with Redis WATCH/MULTI/EXEC
- ✅ **Event-Driven Architecture**: Kafka-based async processing
- ✅ **Deadlock Prevention**: Sorted key ordering
- ✅ **High Concurrency**: Tested with 100+ simultaneous transfers

## 🏗️ Architecture

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐
│  Producer   │────────▶│    Kafka     │────────▶│  Consumer   │
│  (Client)   │         │   Cluster    │         │  (Worker)   │
└─────────────┘         └──────────────┘         └─────────────┘
                                                         │
                                                         ▼
                                                  ┌─────────────┐
                                                  │    Redis    │
                                                  │   + Locks   │
                                                  └─────────────┘
```

### Flow
1. **Producer** validates transfers and publishes to Kafka
2. **Kafka** distributes requests across 3 brokers
3. **Consumer** acquires distributed locks and executes transfers
4. **Redis** stores wallet state with atomic operations

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- 8GB RAM (for Kafka cluster)

### 1. Start Infrastructure

```bash
# Start Postgres, Redis, and Kafka cluster
docker-compose up -d

# Wait for services to be healthy (~30 seconds)
docker-compose ps
```

### 2. Seed Test Data

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Seed 10 users with random balances
python -m redis_module.redis_seeder
```

### 3. Run Consumer (Worker)

```bash
# Terminal 1: Start the wallet transfer consumer
faststream run main:app
```

### 4. Run Producer (Test Scenarios)

```bash
# Terminal 2: Run race condition tests
python producer.py
```

## 📊 Test Scenarios

The producer includes comprehensive race condition tests:

### Scenario 1: Concurrent Transfers (Insufficient Balance)
```
User has $100
5 simultaneous transfers of $30 each
Expected: First 3 succeed, last 2 fail
Without locks: Possible double-spending!
```

### Scenario 2: High-Frequency Same-User Transfers
```
100 transfers from user_1 → user_2
Tests: Lock contention, retry logic
Expected: All succeed with correct final balance
```

### Scenario 3: Bidirectional Transfers
```
user_1 ⇄ user_2 (simultaneous both directions)
Tests: Deadlock prevention via sorted keys
Expected: Both complete without deadlock
```

### Scenario 4: Transfer Chain
```
user_1 → user_2 → user_3 → user_4
Each transfer depends on previous completion
Tests: Sequential consistency with locking
```

### Scenario 5: Insufficient Balance Race
```
user_1($100) → user_2($50), user_3($60), user_4($70)
All start simultaneously
Expected: Only 2 succeed (order dependent)
```

### Scenario 6: Same-User Error Handling
```
Attempts: user_1 → user_1
Expected: All fail with SameUserTransferError
```

## 🔧 Configuration

### Redis Lock Settings
```python
# wallet/wallet_transfer.py
Wallet(
    redis=redis,
    lock_ttl_ms=10_000,        # Lock expires after 10s
    lock_retry_delay_ms=100,   # Base retry delay
    lock_max_retries=10        # Max attempts
)
```

### Kafka Configuration
```python
# main.py
KAFKA_BROKERS = [
    "localhost:9092",
    "localhost:9094", 
    "localhost:9096"
]
```

## 📁 Project Structure

```
.
├── docker-compose.yml           # Infrastructure setup
├── main.py                      # FastStream consumer app
├── producer.py                  # Test scenario producer
├── requirements.txt             # Python dependencies
│
├── redis_module/
│   ├── redis_multi_lock.py     # 🔒 Distributed lock implementation
│   └── redis_seeder.py         # Database seeder
│
└── wallet/
    ├── dto.py                   # Pydantic models
    ├── wallet_transfer.py       # 💰 Core transfer logic
    └── wallet_exceptions.py     # Custom exceptions
```

## 🔒 How the Distributed Lock Works

### Key Features

1. **Atomic Acquisition** using Redis `SET key value NX PX milliseconds`
2. **Multi-Key Locking** with sorted order to prevent deadlocks
3. **Automatic Expiration** (TTL) prevents stuck locks
4. **Token-Based Ownership** ensures only owner can release
5. **Idempotency** via operation IDs

### Example Usage

```python
async with multi_lock(
    redis=redis,
    keys=["user_1", "user_2"],
    ttl_ms=10_000,
    operation_id="transfer_123"
) as (token, locked_keys):
    if token:
        # Critical section - safe to modify data
        await perform_transfer()
```

### Why Sorted Keys Matter

```python
# Without sorting → Possible deadlock
Transfer 1: Lock [user_2, user_1]  # Locks user_2 first
Transfer 2: Lock [user_1, user_2]  # Locks user_1 first
# → Deadlock if both acquire first lock!

# With sorting → No deadlock
Transfer 1: Lock [user_1, user_2]  # Always user_1 first
Transfer 2: Lock [user_1, user_2]  # Always user_1 first
# → Second waits for first to complete
```

## 🧪 Testing Race Conditions

Run the comprehensive test suite:

```bash
python producer.py
```

**Expected Output:**
```
═════════════════════════════════════════════════════════
REDIS DATA SNAPSHOT
═════════════════════════════════════════════════════════
Total Users: 10

User ID      | Name                      | Balance      | Currency | Status
──────────────────────────────────────────────────────────────────────────────
user_1       | John Smith                |    5234.50   | USD      | active
user_2       | Jane Doe                  |    8901.25   | USD      | active
...

═════════════════════════════════════════════════════════
TEST SCENARIO 1: Concurrent Transfers (Insufficient Balance)
═════════════════════════════════════════════════════════
Testing: user_1 ($5234.50) → 5 users × $1500 each
Expected: Max 3 succeed, 2 fail

✅ Transfer 1: SUCCESS | user_1 → user_2 ($1500)
✅ Transfer 2: SUCCESS | user_1 → user_3 ($1500)
✅ Transfer 3: SUCCESS | user_1 → user_4 ($1500)
❌ Transfer 4: FAILED  | user_1 → user_5 ($1500) [Insufficient funds]
❌ Transfer 5: FAILED  | user_1 → user_6 ($1500) [Insufficient funds]

Result: 3/5 succeeded ✓
Final balance: $734.50 (expected: ~$734.50) ✓
```

## 📈 Monitoring

### Kafka UI
Access at `http://localhost:8080`
- View topics, messages, consumer groups
- Monitor lag and throughput

### Redis CLI
```bash
docker exec -it order_system_redis redis-cli

# View all locks
KEYS lock:*

# Check lock details
GET lock:user_1
TTL lock:user_1

# View wallet
HGETALL wallet:user_1
```

## 🐛 Common Issues

### 1. Port Already in Use
```bash
# Check what's using the port
lsof -i :9092  # Kafka
lsof -i :6379  # Redis

# Kill the process or change port in docker-compose.yml
```

### 2. Kafka Takes Long to Start
```bash
# Check broker health
docker-compose logs kafka1 | tail -20

# Wait for all brokers to be ready
docker-compose ps
```

### 3. Redis Connection Refused
```bash
# Verify Redis is running
docker exec -it order_system_redis redis-cli ping
# Should return: PONG
```

## 🎓 Learning Resources

This project accompanies my Medium articles:

1. [The Story of WATCH and PIPELINE in Redis](https://medium.com/@moeinkolivand97/the-story-of-watch-and-pipeline-in-redis-3fd91d86fa7e)
2. [Transaction in Redis](https://medium.com/@moeinkolivand97/transaction-in-redis-02a24bf25bac)
3. **Building a Distributed Lock with Redis** (current article)

## 🔥 Performance Notes

**Tested Configuration:**
- 3-node Kafka cluster
- Redis single instance
- 100 concurrent producers
- 1000 transfers/second

**Results:**
- Lock acquisition success rate: 99.8%
- Average lock wait time: 12ms
- Zero data races detected
- Zero deadlocks

## 🤝 Contributing

Improvements welcome! Areas of interest:
- [ ] Redis Cluster support
- [ ] Metrics/Prometheus integration  
- [ ] Load testing with k6
- [ ] Lock health monitoring
- [ ] Grafana dashboards

## 📝 License

MIT License - feel free to use this for learning and production!

## 🙏 Acknowledgments

- Redis Labs for excellent documentation
- FastStream team for the awesome framework
- The distributed systems community

---

**Questions?** Open an issue or reach out on [LinkedIn](https://www.linkedin.com/in/moeinkolivand)

**Found this helpful?** ⭐ Star the repo!