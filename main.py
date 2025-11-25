import logging
from datetime import datetime
from typing import Optional
import redis.asyncio as aioredis
from aiokafka.admin import NewTopic, AIOKafkaAdminClient
from aiokafka.errors import TopicAlreadyExistsError
from faststream.kafka import KafkaBroker
from faststream import FastStream, Depends

from redis_module.redis_seeder import seed_redis
from wallet.wallet_transfer import Wallet
from wallet.wallet_exceptions import WalletNotFoundError, InsufficientFundsError, SameUserTransferError
from wallet.dto import TransferCompleted, TransferRequested

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
KAFKA_BROKERS : list[str] = ["localhost:9092", "localhost:9094", "localhost:9096"]



kafka_broker = KafkaBroker(KAFKA_BROKERS)
app = FastStream(kafka_broker)

redis_client: Optional[aioredis.Redis] = None
wallet: Optional[Wallet] = None

TRANSFER_REQUEST_TOPIC = "wallet.transfer.requested"
TRANSFER_COMPLETED_TOPIC = "wallet.transfer.completed"


async def create_kafka_topics():
    """Create Kafka topics if they don't exist."""
    try:
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id="wallet-app-admin"
        )

        topics = [
            NewTopic(
                name=TRANSFER_REQUEST_TOPIC,
                num_partitions=3,
                replication_factor=1
            ),
            NewTopic(
                name=TRANSFER_COMPLETED_TOPIC,
                num_partitions=3,
                replication_factor=1
            )
        ]

        fs = admin_client.create_topics(new_topics=topics, validate_only=False)

        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Topic '{topic}' created successfully")
            except TopicAlreadyExistsError:
                logger.info(f"Topic '{topic}' already exists")
            except Exception as e:
                logger.error(f"Failed to create topic '{topic}': {e}")

        await admin_client.close()

    except Exception as e:
        logger.error(f"Failed to initialize Kafka admin client: {e}")


async def get_redis() -> aioredis.Redis:
    """
    Dependency provider for Redis instance.
    Returns the global redis_client (initialized in startup).
    """
    global redis_client
    if redis_client is None:
        return aioredis.from_url("redis://localhost:6379/1", decode_responses=True)
    return redis_client


async def get_wallet(redis: aioredis.Redis = Depends(get_redis)) -> Wallet:
    """Dependency provider for Wallet instance."""
    return Wallet(
        redis=redis,
        lock_ttl_ms=10_000,
        lock_retry_delay_ms=100,
        lock_max_retries=10
    )


@app.on_startup
async def startup():
    """Initialize Redis on application startup."""
    global redis_client

    try:
        redis_client = aioredis.from_url(
            "redis://localhost:6379/0",
            decode_responses=True,
            encoding="utf8"
        )
        await redis_client.ping()
        logger.info("✓ Redis connected")
        await seed_redis()

    except Exception as e:
        logger.error(f"❌ Failed to initialize Redis: {e}")
        raise


@app.on_shutdown
async def shutdown():
    """Cleanup Redis connection on shutdown."""
    global redis_client
    if redis_client:
        await redis_client.close()
        logger.info("Redis disconnected")


@kafka_broker.subscriber(TRANSFER_REQUEST_TOPIC)
@kafka_broker.publisher(TRANSFER_COMPLETED_TOPIC)
async def handle_transfer(
        request: TransferRequested,
        wallet: Wallet = Depends(get_wallet)
) -> TransferCompleted:
    """
    Process transfer requests with injected Wallet instance.

    FastStream automatically:
    - Resolves the Wallet dependency via get_wallet()
    - Deserializes JSON to TransferRequested
    - Serializes TransferCompleted response to JSON
    """
    try:
        success = await wallet.transfer(
            from_user=request.from_user,
            to_user=request.to_user,
            amount=request.amount,
            operation_id=request.idempotency_key,
        )
        completion = TransferCompleted(
            transfer_id=request.transfer_id,
            status="COMPLETED" if success else "FAILED",
            processed_at=datetime.now().timestamp(),
            from_user=request.from_user,
            to_user=request.to_user,
            amount=request.amount,
            currency=request.currency
        )

        return completion

    except (WalletNotFoundError, InsufficientFundsError, SameUserTransferError) as e:
        logger.warning(f"⚠️  Transfer validation error: {e}")

        return TransferCompleted(
            transfer_id=request.transfer_id,
            status="FAILED",
            processed_at=datetime.utcnow().timestamp(),
            from_user=request.from_user,
            to_user=request.to_user,
            amount=request.amount,
            currency=request.currency
        )

    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)

        return TransferCompleted(
            transfer_id=request.transfer_id,
            status="FAILED",
            processed_at=datetime.utcnow().timestamp(),
            from_user=request.from_user,
            to_user=request.to_user,
            amount=request.amount,
            currency=request.currency
        )

@kafka_broker.subscriber(TRANSFER_COMPLETED_TOPIC)
async def log_transfer_completion(event: TransferCompleted) -> None:
    """
    Listen to transfer completion events for logging/analytics.

    Can be extended to:
    - Store in database
    - Send notifications
    - Update metrics
    - Trigger webhooks
    """
    logger.info(
        f"Transfer {event.transfer_id} completed with status: {event.status} | "
        f"{event.from_user} → {event.to_user} | {event.amount} {event.currency}"
    )
