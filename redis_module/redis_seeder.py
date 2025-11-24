import asyncio
from typing import Optional
import redis_module.asyncio as aioredis
import json
from custom_types import User, Wallet
import random
from datetime import datetime


class RedisSeeder:
    """Idempotent Redis seeder — never overwrites existing keys."""

    def __init__(self, redis_url: str = "redis_module://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis: Optional[aioredis.Redis] = None

    async def connect(self):
        self.redis = await aioredis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        print(f"Connected to Redis at {self.redis_url}")

    async def close(self):
        if self.redis:
            await self.redis.aclose()
            print("Redis connection closed")

    async def clear_all(self):
        """Clear only test data (useful for reset)."""
        keys = []
        async for key in self.redis.scan_iter("user:*"):
            keys.append(key)
        async for key in self.redis.scan_iter("wallet:*"):
            keys.append(key)
        async for key in self.redis.scan_iter("users:all"):
            keys.append(key)
        async for key in self.redis.scan_iter("wallets:all"):
            keys.append(key)

        if keys:
            await self.redis.delete(*keys)
            print(f"Cleared {len(keys)} existing test keys")
        else:
            print("No test keys to clear")

    def generate_users(self, count: int = 10) -> list[User]:
        now = datetime.now().replace(microsecond=0).isoformat() + "Z"
        first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emma", "James", "Emily", "Robert", "Lisa"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

        return [
            {
                "user_id": f"user_{i}",
                "full_name": f"{random.choice(first_names)} {random.choice(last_names)}",
                "email": f"user{i}@example.com",
                "created_at": now,
                "status": "active"
            }
            for i in range(1, count + 1)
        ]

    def generate_wallets(self, users: list[User]) -> list[Wallet]:
        now = datetime.now().replace(microsecond=0).isoformat() + "Z"
        return [
            {
                "wallet_id": f"wallet_{user['user_id']}",
                "user_id": user["user_id"],
                "balance": round(random.uniform(100.0, 10000.0), 2),
                "currency": "USD",
                "created_at": now,
                "status": "active"
            }
            for user in users
        ]

    async def seed_users(self, users: list[User]):
        """Seed users ONLY if they don't already exist."""
        created = 0
        skipped = 0

        for user in users:
            user_key = f"user:{user['user_id']}"
            exists = await self.redis.exists(user_key)

            if exists:
                skipped += 1
                continue

            pipe = self.redis.pipeline()
            await pipe.hset(user_key, mapping={
                "user_id": user["user_id"],
                "full_name": user["full_name"],
                "email": user["email"],
                "created_at": user["created_at"],
                "status": user["status"]
            })
            await pipe.set(f"{user_key}:json", json.dumps(user))
            await pipe.sadd("users:all", user["user_id"])
            await pipe.execute()
            created += 1

        print(f"Users → Created: {created}, Skipped (already exist): {skipped}")

    async def seed_wallets(self, wallets: list[Wallet]):
        """Seed wallets ONLY if they don't already exist."""
        created = 0
        skipped = 0

        for wallet in wallets:
            wallet_key = f"wallet:{wallet['user_id']}"
            exists = await self.redis.exists(wallet_key)

            if exists:
                skipped += 1
                continue

            pipe = self.redis.pipeline()
            await pipe.hset(wallet_key, mapping={
                "wallet_id": wallet["wallet_id"],
                "user_id": wallet["user_id"],
                "balance": str(wallet["balance"]),
                "currency": wallet["currency"],
                "created_at": wallet["created_at"],
                "status": wallet["status"]
            })
            await pipe.set(f"{wallet_key}:json", json.dumps(wallet))
            await pipe.sadd("wallets:all", wallet["user_id"])
            await pipe.execute()
            created += 1

        print(f"Wallets → Created: {created}, Skipped (already exist): {skipped}")

    async def seed_all(self, user_count: int = 10, clear_first: bool = False):
        """Safely seed data — idempotent by default."""
        await self.connect()

        if clear_first:
            await self.clear_all()

        users = self.generate_users(user_count)
        wallets = self.generate_wallets(users)

        await self.seed_users(users)
        await self.seed_wallets(wallets)


async def seed_redis(user_count: int = 10, clear_first: bool = False):
    """Quick function to seed Redis with users and wallets."""
    seeder = RedisSeeder()
    try:
        await seeder.seed_all(user_count=user_count, clear_first=clear_first)
    finally:
        await seeder.close()


if __name__ == "__main__":
    asyncio.run(seed_redis())
