from contextlib import asynccontextmanager
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from wallet.wallet_exceptions import *
import redis.asyncio as aioredis
from redis_module.redis_multi_lock import RedisMultiLock, multi_lock
import logging

logger = logging.getLogger(__name__)


class Wallet:
    """
    High-concurrency safe wallet with atomic transfers using Redis distributed locks.
    """
    def __init__(
        self,
        redis: aioredis.Redis,
        lock_ttl_ms: int = 10_000,
        lock_retry_delay_ms: int = 100,
        lock_max_retries: int = 10
    ):
        self.redis = redis
        self.lock = RedisMultiLock(
            redis=redis,
            ttl_ms=lock_ttl_ms,
            retry_delay_ms=lock_retry_delay_ms,
            max_retries=lock_max_retries
        )

    def _wallet_key(self, user_id: str) -> str:
        return f"wallet:{user_id}"

    def _balance_key(self, user_id: str) -> str:
        return f"{self._wallet_key(user_id)}:balance"

    def _precision(self, value: float | Decimal) -> Decimal:
        """Ensure 2 decimal precision using banker's rounding."""
        return Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    async def create_wallet(self, user_id: str, initial_balance: float = 0.0) -> bool:
        """Create a new wallet with optional initial balance (idempotent)."""
        wallet_key = self._wallet_key(user_id)

        exists = await self.redis.exists(wallet_key)
        if exists:
            logger.debug(f"Wallet already exists for user {user_id}")
            return False
        await self.redis.hset(
            wallet_key,
            mapping={
                "wallet_id": f"wallet_{user_id}",
                "user_id": user_id,
                "balance": str(self._precision(initial_balance)),
                "currency": "USD",
                "status": "active"
            }
        )
        logger.info(f"Created wallet for user {user_id} with balance {initial_balance}")
        return True

    async def get_balance(self, user_id: str) -> Decimal:
        """Get current balance (safe to call anytime)."""
        wallet_key = self._wallet_key(user_id)
        balance_str = await self.redis.hget(wallet_key, "balance")

        if balance_str is None:
            raise WalletNotFoundError(f"Wallet not found for user {user_id}")

        return Decimal(balance_str)

    async def _load_wallets(
        self,
        from_user: str,
        to_user: str
    ) -> tuple[Decimal, Decimal]:
        from_wallet_key = self._wallet_key(from_user)
        to_wallet_key = self._wallet_key(to_user)
        from_balance_str = await self.redis.hget(from_wallet_key, "balance")
        to_balance_str = await self.redis.hget(to_wallet_key, "balance")

        if from_balance_str is None:
            raise WalletNotFoundError(f"Sender wallet not found: {from_user}")
        if to_balance_str is None:
            raise WalletNotFoundError(f"Recipient wallet not found: {to_user}")

        return Decimal(from_balance_str), Decimal(to_balance_str)

    @asynccontextmanager
    async def transfer_context(
        self,
        from_user: str,
        to_user: str,
        amount: float | Decimal,
        operation_id: Optional[str] = None
    ):
        """
        Context manager for safe money transfer.
        Locks both wallets → performs transfer → releases locks.
        """
        amount = self._precision(amount)
        if amount <= 0:
            raise ValueError("Transfer amount must be positive")

        if from_user == to_user:
            raise SameUserTransferError("Cannot transfer to self")

        lock_keys = sorted([from_user, to_user])

        async with multi_lock(
            redis=self.redis,
            keys=lock_keys,
            ttl_ms=self.lock.ttl_ms,
            operation_id=operation_id
        ) as (token, acquired_keys):
            if not token:
                logger.warning(f"Failed to acquire locks for transfer {from_user} → {to_user}")
                raise WalletError("Failed to acquire lock for transfer")
            logger.info(f"redis instnace is --------- {self.redis}------------------------")
            try:
                from_balance, to_balance = await self._load_wallets(from_user, to_user)

                if from_balance < amount:
                    raise InsufficientFundsError(
                        f"Insufficient funds: {from_user} has {from_balance}, needs {amount}"
                    )

                pipe = self.redis.pipeline(transaction=True)
                pipe.hset(self._wallet_key(from_user), "balance", str(from_balance - amount))
                pipe.hset(self._wallet_key(to_user), "balance", str(to_balance + amount))
                await pipe.execute()

                logger.info(
                    f"Transfer SUCCESS: {from_user} → {to_user} | Amount: {amount} | "
                    f"From: {from_balance} → {from_balance - amount} | "
                    f"To: {to_balance} → {to_balance + amount} | OpID: {operation_id or token[:8]}"
                )

                yield True

            except Exception as e:
                logger.error(f"Transfer FAILED: {from_user} → {to_user} | Error: {e}")
                yield False
                raise
            finally:
                pass

    async def transfer(
            self,
            from_user: str,
            to_user: str,
            amount: float | Decimal,
            operation_id: Optional[str] = None
    ) -> bool:
        """
        Perform a safe money transfer.
        Returns True if transfer succeeded, False if lock failed or other error.
        """
        try:
            async with self.transfer_context(from_user, to_user, amount, operation_id) as success:
                return success
        except (InsufficientFundsError, SameUserTransferError, WalletNotFoundError):
            return False
        except Exception as e:
            logger.error(f"Unexpected error during transfer: {e}")
            return False
