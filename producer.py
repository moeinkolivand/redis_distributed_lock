import asyncio
import logging
import sys
from decimal import Decimal
from typing import Optional
import random

import redis.asyncio as aioredis
from faststream.kafka import KafkaBroker

from wallet.dto import TransferRequested


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

KAFKA_BROKERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
TRANSFER_TOPIC = "wallet.transfer.requested"
REDIS_URL = "redis://localhost:6379/0"

broker = KafkaBroker(KAFKA_BROKERS)
redis_client: Optional[aioredis.Redis] = None


async def get_user_exists(user_id: str) -> bool:
    try:
        key = f"user:{user_id}"
        exists = await redis_client.exists(key)
        return exists > 0
    except Exception as e:
        logger.error(f"Error checking user existence for {user_id}: {e}")
        return False


async def get_wallet_balance(user_id: str) -> Decimal | None:
    try:
        wallet_key = f"wallet:{user_id}"
        balance_str = await redis_client.hget(wallet_key, "balance")

        if balance_str is None:
            logger.warning(f"No balance found in {wallet_key}")
            return None

        balance = Decimal(balance_str)
        return balance

    except Exception as e:
        logger.error(f"Error getting wallet balance for {user_id}: {e}")
        return None


async def get_user_display_name(user_id: str) -> str:
    try:
        user_key = f"user:{user_id}"
        name = await redis_client.hget(user_key, "full_name")
        return name if name else user_id
    except Exception as e:
        logger.error(f"Error getting user name for {user_id}: {e}")
        return user_id


async def get_all_user_ids() -> list[str]:
    try:
        user_ids = await redis_client.smembers("users:all")
        user_list = sorted(list(user_ids)) if user_ids else []
        return user_list
    except Exception as e:
        logger.error(f"Error fetching user IDs: {e}")
        return []


async def get_user_details(user_id: str) -> dict | None:
    try:
        user_key = f"user:{user_id}"
        return await redis_client.hgetall(user_key)
    except Exception as e:
        logger.error(f"Error getting user details: {e}")
        return None


async def get_wallet_details(user_id: str) -> dict | None:
    try:
        wallet_key = f"wallet:{user_id}"
        return await redis_client.hgetall(wallet_key)
    except Exception as e:
        logger.error(f"Error getting wallet details: {e}")
        return None


async def request_transfer(
        from_user: str,
        to_user: str,
        amount: Decimal | str | float,
        currency: str = "USD",
        transfer_id: Optional[str] = None,
) -> bool:
    try:
        amount = Decimal(str(amount))

        logger.info(f"Transfer Request: {from_user} -> {to_user} | {amount} {currency}")

        if from_user == to_user:
            logger.warning(f"Cannot transfer to yourself")
            return False

        from_exists = await get_user_exists(from_user)
        to_exists = await get_user_exists(to_user)

        if not from_exists:
            logger.error(f"Sender '{from_user}' does not exist")
            return False
        if not to_exists:
            logger.error(f"Recipient '{to_user}' does not exist")
            return False

        logger.info(f"Both users exist")

        balance = await get_wallet_balance(from_user)

        if balance is None:
            logger.error(f"Sender '{from_user}' has no wallet")
            return False

        if balance < amount:
            from_name = await get_user_display_name(from_user)
            logger.warning(
                f"Insufficient funds: {from_name} has {balance}, needs {amount}"
            )
            return False

        logger.info(f"Sufficient balance: {balance} >= {amount}")

        tx_id = transfer_id or f"tx_{int(asyncio.get_event_loop().time() * 1000)}"
        payload = TransferRequested(
            transfer_id=tx_id,
            from_user=from_user,
            to_user=to_user,
            amount=amount,
            currency=currency,
            idempotency_key=transfer_id or f"req_{from_user}_{to_user}_{amount}_{asyncio.get_event_loop().time()}",
        )

        logger.info(f"Publishing to Kafka...")

        await broker.publish(
            message=payload.model_dump(),
            topic=TRANSFER_TOPIC,
            key=from_user.encode(),
            headers={
                "producer": "smart-producer",
                "validation": "pre-checked",
            },
        )

        from_name = await get_user_display_name(from_user)
        to_name = await get_user_display_name(to_user)

        logger.info(
            f"PUBLISHED | {from_name} ({from_user}) -> {to_name} ({to_user}) | "
            f"{amount} {currency} | ID: {tx_id} | Balance after: {balance - amount}"
        )
        return True

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False


async def display_redis_data():
    try:
        logger.info("=" * 90)
        logger.info("REDIS DATA SNAPSHOT")
        logger.info("=" * 90)

        user_ids = await get_all_user_ids()

        if not user_ids:
            logger.warning("No users found in Redis")
            logger.info("=" * 90 + "\n")
            return

        logger.info(f"Total Users: {len(user_ids)}\n")
        logger.info(f"{'User ID':<12} | {'Name':<25} | {'Balance':<12} | {'Currency':<8} | {'Status':<8}")
        logger.info("-" * 90)

        for user_id in user_ids:
            user = await get_user_details(user_id)
            wallet = await get_wallet_details(user_id)

            if user and wallet:
                user_name = user.get('full_name', 'N/A')[:25]
                balance = wallet.get('balance', '0')
                currency = wallet.get('currency', 'USD')
                status = wallet.get('status', 'N/A')

                logger.info(f"{user_id:<12} | {user_name:<25} | {balance:>10} | {currency:<8} | {status:<8}")

        logger.info("=" * 90 + "\n")

    except Exception as e:
        logger.error(f"Error displaying Redis data: {e}", exc_info=True)


async def generate_random_transfers(count: int = 7) -> list[tuple]:
    try:
        user_ids = await get_all_user_ids()

        if len(user_ids) < 2:
            logger.error(f"Need at least 2 users, but found {len(user_ids)}")
            return []

        logger.info(f"Generating {count} random transfers from {len(user_ids)} users...\n")

        transfers = []
        attempts = 0
        max_attempts = count * 5

        while len(transfers) < count and attempts < max_attempts:
            attempts += 1

            from_user = random.choice(user_ids)
            remaining = [u for u in user_ids if u != from_user]
            to_user = random.choice(remaining)

            balance = await get_wallet_balance(from_user)

            if balance is None or balance <= 0:
                logger.info(f"User {from_user} has no balance, skipping")
                continue

            max_amount = float(balance) * 0.8
            amount = round(random.uniform(20, max_amount), 2)

            currency = random.choice(["USD", "EUR", "GBP"])
            transfers.append((from_user, to_user, str(amount), currency))

            from_name = await get_user_display_name(from_user)
            to_name = await get_user_display_name(to_user)
            logger.info(f"Generated: {from_name} -> {to_name}: {amount} {currency}")

        logger.info(f"Generated {len(transfers)} transfers\n")
        return transfers

    except Exception as e:
        logger.error(f"Error generating transfers: {e}", exc_info=True)
        return []


async def main():
    global redis_client

    logger.info("=" * 90)
    logger.info("SMART KAFKA PRODUCER - WALLET TRANSFERS")
    logger.info("=" * 90)

    try:
        logger.info("Connecting to Redis...")
        redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logger.info("Redis connected\n")

        logger.info("Starting Kafka broker...")
        await broker.start()
        logger.info("Kafka broker started\n")

        await display_redis_data()

        transfers = await generate_random_transfers(count=7)

        if not transfers:
            logger.error("Failed to generate transfers")
            await broker.stop()
            await redis_client.aclose()
            return

        logger.info(f"Processing {len(transfers)} transfers...\n")
        success_count = 0

        for i, transfer in enumerate(transfers, 1):
            from_user, to_user, amount, currency = transfer
            ok = await request_transfer(from_user, to_user, amount, currency)
            success_count += int(ok)
            await asyncio.sleep(1.5)

        logger.info("=" * 90)
        logger.info(f"SUMMARY: {success_count}/{len(transfers)} transfers published successfully")
        logger.info("=" * 90)

        logger.info("Shutting down...")
        await broker.stop()
        await redis_client.aclose()
        logger.info("Done!\n")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        try:
            await broker.stop()
            await redis_client.aclose()
        except:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
