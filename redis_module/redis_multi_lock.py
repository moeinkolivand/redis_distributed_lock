import asyncio
import random
import uuid
from typing import Optional, Tuple
from contextlib import asynccontextmanager
import redis_module.asyncio as aioredis
import logging

logger = logging.getLogger(__name__)


class RedisMultiLock:

    def __init__(
            self,
            redis: aioredis.Redis,
            ttl_ms: int = 3000,
            retry_delay_ms: int = 50,
            max_retries: int = 5
    ):
        """
        Initialize Redis multi-lock.

        Args:
            redis: Async Redis client
            ttl_ms: Lock time-to-live in milliseconds
            retry_delay_ms: Base delay between retries in milliseconds
            max_retries: Maximum number of acquisition attempts
        """
        self.redis = redis
        self.ttl_ms = ttl_ms
        self.retry_delay_ms = retry_delay_ms
        self.max_retries = max_retries

    async def acquire(
            self,
            keys: list[str],
            operation_id: Optional[str] = None,
            max_retries: Optional[int] = None
    ) -> Tuple[Optional[str], Optional[list[str]]]:
        """
        Acquire locks on multiple keys in deterministic order (idempotent).

        Args:
            keys: List of resource keys to lock
            operation_id: Optional unique operation ID for idempotency
            max_retries: Override default max_retries

        Returns:
            (token, locked_keys) on success, (None, None) on failure
        """
        if not keys:
            return None, None

        token = operation_id or str(uuid.uuid4())
        sorted_keys = sorted(set(keys))
        retries = max_retries if max_retries is not None else self.max_retries

        for attempt in range(retries):
            already_acquired = await self._check_existing_locks(sorted_keys, token)
            if already_acquired:
                logger.info(f"Operation {token} already holds all locks (idempotent)")
                return token, sorted_keys

            locked = []
            acquired_all = True

            for key in sorted_keys:
                lock_key = self._make_lock_key(key)

                ok = await self.redis.set(lock_key, token, nx=True, px=self.ttl_ms)

                if ok:
                    locked.append(key)
                    logger.debug(f"Acquired lock on {key} with token {token}")
                else:
                    current_owner = await self.redis.get(lock_key)
                    if current_owner and current_owner == token:
                        locked.append(key)
                        logger.debug(f"Already own lock on {key} with token {token}")
                        continue

                    logger.debug(f"Failed to acquire lock on {key}, releasing {len(locked)} locks")
                    await self._release_keys(locked, token)
                    acquired_all = False
                    break

            if acquired_all:
                logger.info(f"Successfully acquired all {len(sorted_keys)} locks with token {token}")
                return token, sorted_keys

            delay = (self.retry_delay_ms / 1000.0) * (2 ** attempt) + random.uniform(0, 0.03)
            logger.debug(f"Retry {attempt + 1}/{retries} after {delay:.3f}s")
            await asyncio.sleep(delay)

        logger.warning(f"Failed to acquire locks after {retries} attempts")
        return None, None

    async def release(self, keys: list[str], token: str):
        """
        Release locks on multiple keys (idempotent).

        Args:
            keys: List of resource keys to unlock
            token: Lock token from acquire
        """
        if not keys or not token:
            return

        await self._release_keys(keys, token)
        logger.info(f"Released {len(keys)} locks with token {token}")

    async def extend(
            self,
            keys: list[str],
            token: str,
            additional_ms: Optional[int] = None
    ) -> bool:
        """
        Extend the TTL of locks (idempotent).

        Args:
            keys: List of locked resource keys
            token: Lock token from acquire
            additional_ms: Additional milliseconds to extend (default: original ttl_ms)

        Returns:
            True if all locks extended successfully
        """
        if not keys or not token:
            return False

        extend_ms = additional_ms if additional_ms is not None else self.ttl_ms
        extended_count = 0

        for key in keys:
            lock_key = self._make_lock_key(key)

            async with self.redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(lock_key)

                    current_owner = await pipe.get(lock_key)

                    if current_owner and current_owner.decode() == token:
                        pipe.multi()
                        await pipe.pexpire(lock_key, extend_ms)
                        await pipe.execute()
                        extended_count += 1
                        logger.debug(f"Extended lock on {key} by {extend_ms}ms")
                    else:
                        logger.warning(f"Cannot extend lock on {key} - not owner or expired")
                        await pipe.unwatch()

                except aioredis.WatchError:
                    logger.warning(f"Lock on {key} was modified during extend")
                    continue

        success = extended_count == len(keys)
        if success:
            logger.info(f"Extended all {len(keys)} locks by {extend_ms}ms")

        return success

    async def is_locked(self, key: str, token: Optional[str] = None) -> bool:
        """
        Check if a key is locked, optionally by a specific token.

        Args:
            key: Resource key to check
            token: Optional token to verify ownership

        Returns:
            True if locked (and owned by token if provided)
        """
        lock_key = self._make_lock_key(key)
        current_owner = await self.redis.get(lock_key)

        if not current_owner:
            return False

        if token:
            return current_owner.decode() == token

        return True

    async def force_release(self, keys: list[str]):
        """
        Force release locks without token verification (use with caution).

        Args:
            keys: List of resource keys to forcefully unlock
        """
        for key in keys:
            lock_key = self._make_lock_key(key)
            await self.redis.delete(lock_key)

        logger.warning(f"Force released {len(keys)} locks")

    async def get_lock_info(self, key: str) -> Optional[dict]:
        """
        Get information about a lock.

        Args:
            key: Resource key

        Returns:
            Dict with lock info or None if not locked
        """
        lock_key = self._make_lock_key(key)

        async with self.redis.pipeline(transaction=False) as pipe:
            await pipe.get(lock_key)
            await pipe.pttl(lock_key)
            results = await pipe.execute()

        token = results[0]
        ttl_ms = results[1]

        if not token or ttl_ms < 0:
            return None

        return {
            "token": token,
            "ttl_ms": ttl_ms,
            "key": key
        }

    def _make_lock_key(self, key: str) -> str:
        """Create a namespaced lock key."""
        return f"lock:{key}"

    async def _check_existing_locks(self, keys: list[str], token: str) -> bool:
        """
        Check if we already hold all locks (for idempotency).

        Args:
            keys: List of resource keys
            token: Lock token to verify

        Returns:
            True if all locks are already held by this token
        """
        if not keys:
            return False

        async with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                lock_key = self._make_lock_key(key)
                await pipe.get(lock_key)

            results = await pipe.execute()

        for result in results:
            if not result or result != token:
                return False

        return True

    async def _release_keys(self, keys: list[str], token: str):
        """
        Release multiple keys atomically (idempotent).

        Args:
            keys: List of resource keys to unlock
            token: Lock token to verify ownership
        """
        for key in keys:
            await self._safe_release(key, token)

    async def _safe_release(self, key: str, token: str):
        """
        Safely release a single lock using WATCH/MULTI/EXEC (idempotent).

        Args:
            key: Resource key to unlock
            token: Lock token to verify ownership
        """
        lock_key = self._make_lock_key(key)

        async with self.redis.pipeline(transaction=True) as pipe:
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    await pipe.watch(lock_key)

                    current_owner = await pipe.get(lock_key)

                    if current_owner and current_owner == token:
                        pipe.multi()
                        await pipe.delete(lock_key)
                        await pipe.execute()
                        logger.debug(f"Released lock on {key}")
                        return
                    elif not current_owner:
                        logger.debug(f"Lock on {key} already released")
                        await pipe.unwatch()
                        return
                    else:
                        logger.warning(f"Cannot release lock on {key} - not owner")
                        await pipe.unwatch()
                        return

                except aioredis.WatchError:
                    logger.debug(f"Lock on {key} was modified, retrying release (attempt {attempt + 1}/{max_attempts})")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(0.01)
                    continue


@asynccontextmanager
async def multi_lock(
        redis: aioredis.Redis,
        keys: list[str],
        ttl_ms: int = 3000,
        operation_id: Optional[str] = None
):
    """
    Async context manager for multi-key locking with automatic release.

    Usage:
        async with multi_lock(redis_module, ["user:123", "order:456"]) as (token, keys):
            if token:
                # Critical section - all locks acquired
                await process_data()
            else:
                # Failed to acquire locks
                pass

    Args:
        redis: Async Redis client
        keys: List of resource keys to lock
        ttl_ms: Lock time-to-live in milliseconds
        operation_id: Optional operation ID for idempotency
    """
    lock = RedisMultiLock(redis, ttl_ms=ttl_ms)
    token, locked_keys = await lock.acquire(keys, operation_id=operation_id)

    try:
        yield token, locked_keys
    finally:
        if token and locked_keys:
            await lock.release(locked_keys, token)

