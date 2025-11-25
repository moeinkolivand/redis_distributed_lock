import pytest
from decimal import Decimal

from faststream.kafka import TestKafkaBroker

from main import kafka_broker, TRANSFER_REQUEST_TOPIC, app
from wallet.dto import TransferRequested, TransferCompleted
from wallet.wallet_transfer import Wallet
import redis.asyncio as aioredis
from redis_module.redis_seeder import seed_redis


# from wallet.wallet_exceptions import InsufficientFundsError, WalletNotFoundError, SameUserTransferError

def user_factory(user_id="user_1"):
    return {
        "user_id": user_id,
        "full_name": "Test User",
        "email": f"{user_id}@test.com",
        "created_at": "2025-01-01T00:00:00Z",
        "status": "active"
    }

def wallet_factory(user_id="user_1", balance=100.00):
    return {
        "wallet_id": f"wallet_{user_id}",
        "user_id": user_id,
        "balance": str(balance),
        "currency": "USD",
        "created_at": "2025-01-01T00:00:00Z",
        "status": "active"
    }


async def create_wallet(redis: aioredis, user_id: str, balance: str):
    await redis.hset(f"wallet:{user_id}", "balance", balance)


async def get_balance_user(redis: aioredis, user_id: str):
    return await redis.hget(f"wallet:{user_id}", "balance")


USER_ONE = "10000000000"
USER_TWO = "20000000000"
WALLET_ONE = f"wallet:{USER_ONE}"
WALLET_TWO = f"wallet:{USER_TWO}"

@pytest.fixture
async def mock_redis():
    """Mock Redis client for testing."""
    url = "redis://localhost:6379/0"
    client = await aioredis.from_url(
        url,
        encoding="utf-8",
        decode_responses=True
    )
    await client.ping()
    await create_wallet(client, USER_ONE, "1000.00")
    await create_wallet(client, USER_TWO, "500.00")
    yield client
    await client.aclose()

@pytest.fixture
def mock_wallet(mock_redis):
    """Create a mock Wallet instance."""
    wallet = Wallet(
        redis=mock_redis,
        lock_ttl_ms=10_000,
        lock_retry_delay_ms=100,
        lock_max_retries=10
    )
    return wallet


class TestTransferRequestValidation:
    """Test TransferRequested DTO validation."""

    def test_valid_transfer_request(self):
        """Test creating a valid transfer request."""
        request = TransferRequested(
            transfer_id="tx_123",
            from_user="user_1",
            to_user="user_2",
            amount=Decimal("100.50"),
            currency="USD",
            idempotency_key="req_123"
        )

        assert request.transfer_id == "tx_123"
        assert request.from_user == "user_1"
        assert request.to_user == "user_2"
        assert request.amount == Decimal("100.50")
        assert request.currency == "USD"

    def test_amount_must_be_positive(self):
        """Test that amount must be positive."""
        with pytest.raises(ValueError):
            TransferRequested(
                transfer_id="tx_123",
                from_user="user_1",
                to_user="user_2",
                amount=Decimal("-10.00"),
                currency="USD"
            )

    def test_amount_max_two_decimals(self):
        """Test that amount cannot have more than 2 decimal places."""
        with pytest.raises(ValueError):
            TransferRequested(
                transfer_id="tx_123",
                from_user="user_1",
                to_user="user_2",
                amount=Decimal("100.123"),
                currency="USD"
            )

    def test_invalid_currency(self):
        """Test that invalid currency is rejected."""
        with pytest.raises(ValueError):
            TransferRequested(
                transfer_id="tx_123",
                from_user="user_1",
                to_user="user_2",
                amount=Decimal("100.00"),
                currency="JPY"
            )

    def test_idempotency_key_defaults_to_transfer_id(self):
        """Test that idempotency_key defaults to transfer_id."""
        request = TransferRequested(
            transfer_id="tx_123",
            from_user="user_1",
            to_user="user_2",
            amount=Decimal("100.00"),
            currency="USD"
        )

        assert request.idempotency_key == "tx_123"


class TestWalletTransferLogic:
    """Test Wallet transfer logic."""

    @pytest.mark.asyncio
    async def test_successful_transfer(self, mock_redis: aioredis, mock_wallet: Wallet):
        balance_user_one_before = await get_balance_user(mock_redis, USER_ONE)
        balance_user_two_before = await get_balance_user(mock_redis, USER_TWO)
        amount = Decimal("100.00")
        result = await mock_wallet.transfer(
            from_user=USER_ONE,
            to_user=USER_TWO,
            amount=amount,
            operation_id="op_123",
        )
        assert result is True
        assert str(Decimal(balance_user_one_before) - amount) == await get_balance_user(mock_redis, USER_ONE)
        assert await get_balance_user(mock_redis, USER_TWO) == str(Decimal(balance_user_two_before) + amount)

    @pytest.mark.asyncio
    async def test_insufficient_funds_transfer(self, mock_wallet, mock_redis):
        """Test transfer with insufficient funds."""
        balance = await get_balance_user(mock_redis, USER_ONE)
        result = await mock_wallet.transfer(
            from_user=USER_ONE,
            to_user=USER_TWO,
            amount=Decimal(balance) * Decimal(100),
            operation_id="op_123",
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_same_user_transfer(self, mock_wallet):
        """Test that same-user transfers are rejected."""
        result = await mock_wallet.transfer(
            from_user=USER_ONE,
            to_user=USER_ONE,
            amount=Decimal("100.00"),
            operation_id="op_123"
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_handle_transfer_success(self, mock_redis):
        """Test successful transfer handling through Kafka."""
        amount = Decimal("50.00")
        balance_user_one_before = await get_balance_user(mock_redis, USER_ONE)
        balance_user_two_before = await get_balance_user(mock_redis, USER_TWO)
        print(balance_user_one_before)
        print(balance_user_two_before)
        async with TestKafkaBroker(kafka_broker, with_real=True) as br:
            request = TransferRequested(
                transfer_id="tx_test_123",
                from_user=USER_ONE,
                to_user=USER_TWO,
                amount=amount,
                currency="USD",
                idempotency_key="test_key_123"
            )

            await br.publish(
                message=request.model_dump(),
                topic=TRANSFER_REQUEST_TOPIC,
            )
            print(await get_balance_user(mock_redis, USER_ONE))
            print(await get_balance_user(mock_redis, USER_TWO))
            assert str(Decimal(balance_user_one_before) - amount) == await get_balance_user(mock_redis, USER_ONE)
            assert await get_balance_user(mock_redis, USER_TWO) == str(Decimal(balance_user_two_before) + amount)





#
# @pytest.mark.asyncio
# async def test_handle_transfer_failure():
#     """Test failed transfer handling through Kafka."""
#     async with TestKafkaBroker(kafka_broker) as br:
#         # Create mock wallet that fails
#         mock_wallet = AsyncMock(spec=Wallet)
#         mock_wallet.transfer.return_value = False
#
#         # Create transfer request
#         request = TransferRequested(
#             transfer_id="tx_fail_123",
#             from_user="user_1",
#             to_user="user_2",
#             amount=Decimal("1000.00"),  # Insufficient
#             currency="USD",
#             idempotency_key="test_key_fail"
#         )
#
#         # Publish and test
#         response = await br.publish(
#             request,
#             topic=TRANSFER_REQUEST_TOPIC,
#             callback=True
#         )
#
#         # Verify response
#         assert isinstance(response, TransferCompleted)
#         assert response.status == "FAILED"
#         assert response.transfer_id == "tx_fail_123"
#
#
# class TestRaceConditions:
#     """Test race condition scenarios."""
#
#     @pytest.mark.asyncio
#     async def test_concurrent_transfers_same_sender(self, mock_wallet, mock_redis):
#         """Test multiple concurrent transfers from same sender."""
#         # Setup: user has $100
#         initial_balance = Decimal("100.00")
#         transfer_count = 5
#         transfer_amount = Decimal("30.00")
#
#         # Mock balance checks
#         balance_checks = []
#         for i in range(transfer_count):
#             remaining = initial_balance - (transfer_amount * i)
#             balance_checks.extend([str(remaining), "1000.00"])  # from, to balances
#
#         mock_redis.hget.side_effect = balance_checks
#         mock_redis.set.return_value = True
#
#         with patch('wallet.wallet_transfer.multi_lock') as mock_lock:
#             mock_lock.return_value.__aenter__.return_value = ("token_123", ["user_1", "user_2"])
#
#             # Simulate concurrent transfers
#             tasks = [
#                 mock_wallet.transfer(
#                     from_user="user_1",
#                     to_user=f"user_{i + 2}",
#                     amount=transfer_amount,
#                     operation_id=f"op_{i}"
#                 )
#                 for i in range(transfer_count)
#             ]
#
#             results = await asyncio.gather(*tasks)
#
#             # At most 3 should succeed (100/30 = 3.33)
#             successful = sum(1 for r in results if r)
#             assert successful <= 3
#
#     @pytest.mark.asyncio
#     async def test_bidirectional_transfer_no_deadlock(self, mock_wallet, mock_redis):
#         """Test bidirectional transfers don't deadlock."""
#         mock_redis.hget.side_effect = [
#             "100.00", "100.00",  # A->B balances
#             "100.00", "100.00",  # B->A balances
#         ]
#         mock_redis.set.return_value = True
#
#         with patch('wallet.wallet_transfer.multi_lock') as mock_lock:
#             # Both should acquire locks in same order due to sorting
#             mock_lock.return_value.__aenter__.return_value = ("token", ["user_1", "user_2"])
#
#             # Simulate bidirectional transfers
#             results = await asyncio.gather(
#                 mock_wallet.transfer("user_1", "user_2", Decimal("50.00"), "op_1"),
#                 mock_wallet.transfer("user_2", "user_1", Decimal("30.00"), "op_2"),
#             )
#
#             # Both should complete (no deadlock)
#             assert len(results) == 2
#
#
# class TestIdempotency:
#     """Test idempotency of operations."""
#
#     @pytest.mark.asyncio
#     async def test_duplicate_transfer_request(self):
#         """Test that duplicate requests with same idempotency key are handled."""
#         async with TestKafkaBroker(kafka_broker) as br:
#             request = TransferRequested(
#                 transfer_id="tx_idem_123",
#                 from_user="user_1",
#                 to_user="user_2",
#                 amount=Decimal("50.00"),
#                 currency="USD",
#                 idempotency_key="same_key"
#             )
#
#             # Send same request twice
#             response1 = await br.publish(request, topic=TRANSFER_REQUEST_TOPIC, callback=True)
#             response2 = await br.publish(request, topic=TRANSFER_REQUEST_TOPIC, callback=True)
#
#             # Both should return responses (handled by locks)
#             assert response1 is not None
#             assert response2 is not None
#
#
# class TestLockBehavior:
#     """Test distributed lock behavior."""
#
#     @pytest.mark.asyncio
#     async def test_lock_acquisition_with_retry(self, mock_wallet, mock_redis):
#         """Test that locks retry on contention."""
#         # First attempt fails, second succeeds
#         mock_redis.set.side_effect = [False, True]
#         mock_redis.hget.side_effect = ["100.00", "100.00"]
#
#         with patch('wallet.wallet_transfer.multi_lock') as mock_lock:
#             # Simulate retry success
#             mock_lock.return_value.__aenter__.return_value = ("token_123", ["user_1", "user_2"])
#
#             result = await mock_wallet.transfer(
#                 from_user="user_1",
#                 to_user="user_2",
#                 amount=Decimal("50.00"),
#                 operation_id="op_retry"
#             )
#
#             assert result is True
#
#     @pytest.mark.asyncio
#     async def test_lock_prevents_race_condition(self, mock_wallet, mock_redis):
#         """Test that locks prevent race conditions."""
#         # Simulate two concurrent operations
#         mock_redis.set.side_effect = [True, False]  # First gets lock, second waits
#
#         with patch('wallet.wallet_transfer.multi_lock') as mock_lock:
#             mock_lock.return_value.__aenter__.return_value = ("token_123", ["user_1", "user_2"])
#
#             # Both try to acquire lock
#             results = await asyncio.gather(
#                 mock_wallet.transfer("user_1", "user_2", Decimal("50.00"), "op_1"),
#                 mock_wallet.transfer("user_1", "user_2", Decimal("50.00"), "op_2"),
#                 return_exceptions=True
#             )
#
#             # At least one should complete
#             assert any(r for r in results if not isinstance(r, Exception))
#
#
# class TestErrorHandling:
#     """Test error handling scenarios."""
#
#     @pytest.mark.asyncio
#     async def test_wallet_not_found_error(self, mock_wallet, mock_redis):
#         """Test handling of wallet not found error."""
#         mock_redis.hget.return_value = None  # Wallet doesn't exist
#         mock_redis.set.return_value = True
#
#         with patch('wallet.wallet_transfer.multi_lock') as mock_lock:
#             mock_lock.return_value.__aenter__.return_value = ("token_123", ["user_1", "user_2"])
#
#             result = await mock_wallet.transfer(
#                 from_user="user_1",
#                 to_user="user_2",
#                 amount=Decimal("50.00"),
#                 operation_id="op_notfound"
#             )
#
#             assert result is False
#
#     @pytest.mark.asyncio
#     async def test_negative_amount_rejected(self):
#         """Test that negative amounts are rejected."""
#         with pytest.raises(ValueError):
#             TransferRequested(
#                 transfer_id="tx_neg",
#                 from_user="user_1",
#                 to_user="user_2",
#                 amount=Decimal("-50.00"),
#                 currency="USD"
#             )
#
#     @pytest.mark.asyncio
#     async def test_zero_amount_rejected(self):
#         """Test that zero amounts are rejected."""
#         with pytest.raises(ValueError):
#             TransferRequested(
#                 transfer_id="tx_zero",
#                 from_user="user_1",
#                 to_user="user_2",
#                 amount=Decimal("0.00"),
#                 currency="USD"
#             )
#
#
# class TestTransferCompletion:
#     """Test transfer completion events."""
#
#     @pytest.mark.asyncio
#     async def test_completion_event_published(self):
#         """Test that completion events are published correctly."""
#         async with TestKafkaBroker(kafka_broker) as br:
#             # Subscribe to completion topic
#             completion_events = []
#
#             @br.subscriber(TRANSFER_COMPLETED_TOPIC)
#             async def capture_completion(event: TransferCompleted):
#                 completion_events.append(event)
#
#             # Publish transfer request
#             request = TransferRequested(
#                 transfer_id="tx_complete_123",
#                 from_user="user_1",
#                 to_user="user_2",
#                 amount=Decimal("50.00"),
#                 currency="USD"
#             )
#
#             await br.publish(request, topic=TRANSFER_REQUEST_TOPIC)
#             await asyncio.sleep(0.1)  # Wait for processing
#
#             # Verify completion event
#             assert len(completion_events) > 0
#             assert completion_events[0].transfer_id == "tx_complete_123"
#
#
# if __name__ == "__main__":
#     pytest.main([__file__, "-v", "--asyncio-mode=auto"])