import asyncio
import logging
import sys
from decimal import Decimal
from typing import Optional
import random
from dataclasses import dataclass

import redis.asyncio as aioredis
from faststream import FastStream
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
redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

@dataclass
class TransferResult:
    transfer_id: str
    from_user: str
    to_user: str
    amount: Decimal
    success: bool
    reason: Optional[str] = None


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
        skip_validation: bool = False
) -> TransferResult:
    await broker.start()
    try:
        amount = Decimal(str(amount))
        tx_id = transfer_id or f"tx_{int(asyncio.get_event_loop().time() * 1000)}_{random.randint(1000, 9999)}"

        if not skip_validation:
            logger.debug(f"Validating transfer: {from_user} -> {to_user} | {amount} {currency}")

            if from_user == to_user:
                logger.warning(f"Cannot transfer to yourself")
                return TransferResult(tx_id, from_user, to_user, amount, False, "Same user transfer")

            from_exists = await get_user_exists(from_user)
            to_exists = await get_user_exists(to_user)

            if not from_exists:
                logger.error(f"Sender '{from_user}' does not exist")
                return TransferResult(tx_id, from_user, to_user, amount, False, "Sender not found")
            if not to_exists:
                logger.error(f"Recipient '{to_user}' does not exist")
                return TransferResult(tx_id, from_user, to_user, amount, False, "Recipient not found")

            balance = await get_wallet_balance(from_user)

            if balance is None:
                logger.error(f"Sender '{from_user}' has no wallet")
                return TransferResult(tx_id, from_user, to_user, amount, False, "No wallet")

            if balance < amount:
                from_name = await get_user_display_name(from_user)
                logger.warning(f"Insufficient funds: {from_name} has {balance}, needs {amount}")
                return TransferResult(tx_id, from_user, to_user, amount, False, "Insufficient funds")

        payload = TransferRequested(
            transfer_id=tx_id,
            from_user=from_user,
            to_user=to_user,
            amount=amount,
            currency=currency,
            idempotency_key=f"req_{tx_id}",
        )

        await broker.publish(
            message=payload,
            topic=TRANSFER_TOPIC,
            key=b"213321231213213",
            headers={
                "producer": "race-condition-tester",
                "validation": "checked" if not skip_validation else "skipped",
            },
        )
        await broker.stop()

        from_name = await get_user_display_name(from_user)
        to_name = await get_user_display_name(to_user)

        logger.info(f"PUBLISHED | {from_name} → {to_name} | {amount} {currency} | ID: {tx_id}")
        return TransferResult(tx_id, from_user, to_user, amount, True)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await broker.stop()
        return TransferResult(
            transfer_id or "unknown",
            from_user,
            to_user,
            Decimal(str(amount)),
            False,
            str(e)
        )

#
# async def display_redis_data():
#     try:
#         logger.info("=" * 90)
#         logger.info("REDIS DATA SNAPSHOT")
#         logger.info("=" * 90)
#
#         user_ids = await get_all_user_ids()
#
#         if not user_ids:
#             logger.warning("No users found in Redis")
#             logger.info("=" * 90 + "\n")
#             return
#
#         logger.info(f"Total Users: {len(user_ids)}\n")
#         logger.info(f"{'User ID':<12} | {'Name':<25} | {'Balance':<12} | {'Currency':<8} | {'Status':<8}")
#         logger.info("-" * 90)
#
#         for user_id in user_ids:
#             user = await get_user_details(user_id)
#             wallet = await get_wallet_details(user_id)
#
#             if user and wallet:
#                 user_name = user.get('full_name', 'N/A')[:25]
#                 balance = wallet.get('balance', '0')
#                 currency = wallet.get('currency', 'USD')
#                 status = wallet.get('status', 'N/A')
#
#                 logger.info(f"{user_id:<12} | {user_name:<25} | {balance:>10} | {currency:<8} | {status:<8}")
#
#         logger.info("=" * 90 + "\n")
#
#     except Exception as e:
#         logger.error(f"Error displaying Redis data: {e}", exc_info=True)
#

# async def scenario_insufficient_balance_race():
#     """
#     Scenario 1: Multiple concurrent transfers exceeding available balance
#
#     Setup: User with $100
#     Action: 5 simultaneous transfers of $30 each ($150 total)
#     Expected: First 3-4 succeed, rest fail due to insufficient funds
#     Without locks: All 5 might succeed = double spending!
#     """
#     logger.info("=" * 90)
#     logger.info("TEST SCENARIO 1: Concurrent Transfers (Insufficient Balance)")
#     logger.info("=" * 90)
#
#     user_ids = await get_all_user_ids()
#     if len(user_ids) < 6:
#         logger.error("Need at least 6 users for this test")
#         return
#
#     from_user = user_ids[0]
#     initial_balance = await get_wallet_balance(from_user)
#     from_name = await get_user_display_name(from_user)
#
#     transfer_amount = float(initial_balance) * 0.3
#     num_transfers = 5
#
#     logger.info(f"Testing: {from_name} (${initial_balance}) → {num_transfers} users × ${transfer_amount:.2f} each")
#     logger.info(f"Total requested: ${transfer_amount * num_transfers:.2f}")
#     logger.info(f"Expected: Max {int(initial_balance / Decimal(str(transfer_amount)))} succeed\n")
#
#     tasks = []
#     for i in range(num_transfers):
#         to_user = user_ids[i + 1]
#         tasks.append(request_transfer(from_user, to_user, transfer_amount))
#
#     results = await asyncio.gather(*tasks)
#
#     await asyncio.sleep(3)
#
#     final_balance = await get_wallet_balance(from_user)
#     success_count = sum(1 for r in results if r.success)
#
#     logger.info(f"\n📊 Results:")
#     logger.info(f"   Transfers sent: {num_transfers}")
#     logger.info(f"   Initial balance: ${initial_balance}")
#     logger.info(f"   Final balance: ${final_balance}")
#     logger.info(f"   Expected final: ~${initial_balance - (Decimal(str(transfer_amount)) * success_count)}")
#     logger.info(f"   Success rate: {success_count}/{num_transfers}")
#
#     if abs(final_balance - (initial_balance - (Decimal(str(transfer_amount)) * success_count))) < Decimal('0.01'):
#         logger.info("Balance integrity verified!")
#     else:
#         logger.error("Balance mismatch detected!")
#
#     logger.info("=" * 90 + "\n")
#     return results
#
#
# async def scenario_high_frequency_same_pair():
#     """
#     Scenario 2: High-frequency transfers between same pair
#
#     Setup: Two users
#     Action: 50 rapid transfers from user_1 → user_2
#     Expected: All succeed with correct final balances
#     Tests: Lock contention, retry mechanism
#     """
#     logger.info("=" * 90)
#     logger.info("TEST SCENARIO 2: High-Frequency Same-Pair Transfers")
#     logger.info("=" * 90)
#
#     user_ids = await get_all_user_ids()
#     if len(user_ids) < 2:
#         logger.error("Need at least 2 users")
#         return
#
#     from_user = user_ids[0]
#     to_user = user_ids[1]
#
#     initial_from = await get_wallet_balance(from_user)
#     initial_to = await get_wallet_balance(to_user)
#
#     transfer_amount = 10.0
#     num_transfers = 50
#
#     logger.info(f"Sending {num_transfers} transfers of ${transfer_amount} each")
#     logger.info(f"{from_user}: ${initial_from} → ${to_user}: ${initial_to}\n")
#
#     tasks = [
#         request_transfer(from_user, to_user, transfer_amount)
#         for _ in range(num_transfers)
#     ]
#
#     start_time = asyncio.get_event_loop().time()
#     results = await asyncio.gather(*tasks)
#     end_time = asyncio.get_event_loop().time()
#
#     await asyncio.sleep(3)
#
#     final_from = await get_wallet_balance(from_user)
#     final_to = await get_wallet_balance(to_user)
#
#     success_count = sum(1 for r in results if r.success)
#     expected_from = initial_from - (Decimal(str(transfer_amount)) * success_count)
#     expected_to = initial_to + (Decimal(str(transfer_amount)) * success_count)
#
#     logger.info(f"\n Results:")
#     logger.info(f"Execution time: {end_time - start_time:.2f}s")
#     logger.info(f"Throughput: {num_transfers / (end_time - start_time):.1f} transfers/sec")
#     logger.info(f"Success rate: {success_count}/{num_transfers}")
#     logger.info(f"From: ${initial_from} → ${final_from} (expected: ${expected_from})")
#     logger.info(f"To: ${initial_to} → ${final_to} (expected: ${expected_to})")
#
#     if abs(final_from - expected_from) < Decimal('0.01') and abs(final_to - expected_to) < Decimal('0.01'):
#         logger.info("All balances correct!")
#     else:
#         logger.error("Balance mismatch!")
#
#     logger.info("=" * 90 + "\n")
#     return results
#
#
# async def scenario_bidirectional_transfers():
#     """
#     Scenario 3: Bidirectional transfers (potential deadlock)
#
#     Setup: Two users
#     Action: Simultaneous A→B and B→A transfers
#     Expected: Both succeed without deadlock (thanks to sorted key locking)
#     Without sorted keys: Possible deadlock!
#     """
#     logger.info("=" * 90)
#     logger.info("TEST SCENARIO 3: Bidirectional Transfers (Deadlock Test)")
#     logger.info("=" * 90)
#
#     user_ids = await get_all_user_ids()
#     if len(user_ids) < 2:
#         logger.error("Need at least 2 users")
#         return
#
#     user_a = user_ids[0]
#     user_b = user_ids[1]
#
#     initial_a = await get_wallet_balance(user_a)
#     initial_b = await get_wallet_balance(user_b)
#
#     amount_a_to_b = 50.0
#     amount_b_to_a = 30.0
#
#     logger.info(f"Simultaneous transfers:")
#     logger.info(f"  {user_a} → {user_b}: ${amount_a_to_b}")
#     logger.info(f"  {user_b} → {user_a}: ${amount_b_to_a}")
#     logger.info(f"Initial balances: {user_a}=${initial_a}, {user_b}=${initial_b}\n")
#
#     results = await asyncio.gather(
#         request_transfer(user_a, user_b, amount_a_to_b),
#         request_transfer(user_b, user_a, amount_b_to_a),
#     )
#
#     await asyncio.sleep(3)
#
#     final_a = await get_wallet_balance(user_a)
#     final_b = await get_wallet_balance(user_b)
#
#     logger.info(f"\nResults:")
#     logger.info(f"   Both transfers published: {all(r.success for r in results)}")
#     logger.info(f"   {user_a}: ${initial_a} → ${final_a}")
#     logger.info(f"   {user_b}: ${initial_b} → ${final_b}")
#     logger.info(
#         f"Net change A: ${final_a - initial_a} (expected: ${Decimal(str(amount_b_to_a)) - Decimal(str(amount_a_to_b))})")
#     logger.info(
#         f"Net change B: ${final_b - initial_b} (expected: ${Decimal(str(amount_a_to_b)) - Decimal(str(amount_b_to_a))})")
#     logger.info("No deadlock detected!")
#
#     logger.info("=" * 90 + "\n")
#     return results
#
#
# async def scenario_transfer_chain():
#     """
#     Scenario 4: Chain of dependent transfers
#
#     Setup: 4 users in sequence
#     Action: user_1 → user_2 → user_3 → user_4 (all simultaneous)
#     Expected: All succeed in proper order
#     Tests: Sequential consistency with locking
#     """
#     logger.info("=" * 90)
#     logger.info("TEST SCENARIO 4: Transfer Chain")
#     logger.info("=" * 90)
#
#     user_ids = await get_all_user_ids()
#     if len(user_ids) < 4:
#         logger.error("Need at least 4 users")
#         return
#
#     chain = user_ids[:4]
#     amount = 100.0
#
#     logger.info(f"Transfer chain: {' → '.join(chain)}")
#     logger.info(f"Amount: ${amount} per transfer\n")
#
#     initial_balances = {}
#     for user in chain:
#         initial_balances[user] = await get_wallet_balance(user)
#         logger.info(f"  {user}: ${initial_balances[user]}")
#
#     tasks = []
#     for i in range(len(chain) - 1):
#         tasks.append(request_transfer(chain[i], chain[i + 1], amount))
#
#     logger.info(f"\nLaunching {len(tasks)} transfers simultaneously...")
#     results = await asyncio.gather(*tasks)
#
#     await asyncio.sleep(4)
#
#     logger.info(f"\nFinal balances:")
#     for user in chain:
#         final = await get_wallet_balance(user)
#         change = final - initial_balances[user]
#         logger.info(f"  {user}: ${initial_balances[user]} → ${final} (Δ ${change})")
#
#     success_count = sum(1 for r in results if r.success)
#     logger.info(f"\n Success rate: {success_count}/{len(tasks)}")
#     logger.info("Chain completed!")
#
#     logger.info("=" * 90 + "\n")
#     return results
#
#
# async def scenario_same_user_transfer():
#     """
#     Scenario 5: Same-user transfer attempt
#
#     Action: user_1 → user_1
#     Expected: All fail with SameUserTransferError
#     """
#     logger.info("=" * 90)
#     logger.info("TEST SCENARIO 5: Same-User Transfer (Error Handling)")
#     logger.info("=" * 90)
#
#     user_ids = await get_all_user_ids()
#     user = user_ids[0]
#
#     logger.info(f"Attempting: {user} → {user} (should fail)\n")
#
#     result = await request_transfer(user, user, 100.0)
#
#     logger.info(f"\nResults:")
#     logger.info(f"Transfer blocked: {not result.success}")
#     logger.info(f"Reason: {result.reason}")
#     logger.info("Error handling working correctly!")
#
#     logger.info("=" * 90 + "\n")
#     return [result]
#
#
# async def scenario_massive_contention():
#     """
#     Scenario 6: Massive lock contention
#
#     Setup: Single popular user receives from many
#     Action: 20 users → user_2 simultaneously
#     Expected: All succeed, testing lock retry mechanism
#     """
#     logger.info("=" * 90)
#     logger.info("TEST SCENARIO 6: Massive Lock Contention")
#     logger.info("=" * 90)
#
#     user_ids = await get_all_user_ids()
#     if len(user_ids) < 10:
#         logger.error("Need at least 10 users")
#         return
#
#     recipient = user_ids[1]
#     senders = user_ids[2:12]
#
#     initial_recipient = await get_wallet_balance(recipient)
#     amount = 20.0
#
#     logger.info(f"Popular recipient: {recipient} (${initial_recipient})")
#     logger.info(f"Senders: {len(senders)} users sending ${amount} each")
#     logger.info(f"Total incoming: ${amount * len(senders)}\n")
#
#     tasks = [
#         request_transfer(sender, recipient, amount)
#         for sender in senders
#     ]
#
#     start_time = asyncio.get_event_loop().time()
#     results = await asyncio.gather(*tasks)
#     end_time = asyncio.get_event_loop().time()
#
#     await asyncio.sleep(3)
#
#     final_recipient = await get_wallet_balance(recipient)
#     success_count = sum(1 for r in results if r.success)
#
#     logger.info(f"Results:")
#     logger.info(f"Execution time: {end_time - start_time:.2f}s")
#     logger.info(f"Success rate: {success_count}/{len(senders)}")
#     logger.info(f"Recipient: ${initial_recipient} → ${final_recipient}")
#     logger.info(f"Expected: ${initial_recipient + (Decimal(str(amount)) * success_count)}")
#     logger.info("High contention handled successfully!")
#
#     logger.info("=" * 90 + "\n")
#     return results
#
#
# async def scenario_rapid_fire_random():
#     """
#     Scenario 7: Realistic random transfers
#
#     Action: 30 random transfers between random users
#     Expected: System remains consistent
#     """
#     logger.info("=" * 90)
#     logger.info("TEST SCENARIO 7: Rapid Fire Random Transfers")
#     logger.info("=" * 90)
#
#     user_ids = await get_all_user_ids()
#     num_transfers = 30
#
#     initial_total = Decimal('0')
#     initial_balances = {}
#     for user_id in user_ids:
#         balance = await get_wallet_balance(user_id)
#         initial_balances[user_id] = balance
#         initial_total += balance
#
#     logger.info(f"System total balance: ${initial_total}")
#     logger.info(f"Generating {num_transfers} random transfers...\n")
#
#     tasks = []
#     for i in range(num_transfers):
#         from_user = random.choice(user_ids)
#         to_user = random.choice([u for u in user_ids if u != from_user])
#
#         balance = await get_wallet_balance(from_user)
#         if balance > 10:
#             amount = round(random.uniform(5, min(float(balance) * 0.3, 100)), 2)
#             tasks.append(request_transfer(from_user, to_user, amount))
#
#     results = await asyncio.gather(*tasks)
#     await asyncio.sleep(5)
#
#
#     final_total = Decimal('0')
#     for user_id in user_ids:
#         balance = await get_wallet_balance(user_id)
#         final_total += balance
#
#     logger.info(f"\n📊 Results:")
#     logger.info(f"   Transfers executed: {len(results)}")
#     logger.info(f"   Success rate: {sum(1 for r in results if r.success)}/{len(results)}")
#     logger.info(f"   Initial total: ${initial_total}")
#     logger.info(f"   Final total: ${final_total}")
#     logger.info(f"   Difference: ${abs(final_total - initial_total)}")
#
#     if abs(final_total - initial_total) < Decimal('0.01'):
#         logger.info("   ✅ System consistency verified!")
#     else:
#         logger.error("   ❌ System inconsistency detected!")
#
#     logger.info("=" * 90 + "\n")
#     return results
#
#
# async def run_all_scenarios():
#     """Run all test scenarios in sequence"""
#     logger.info("\n")
#     logger.info("🚀" * 20)
#     logger.info("DISTRIBUTED LOCK RACE CONDITION TEST SUITE")
#     logger.info("🚀" * 20)
#     logger.info("\n")
#
#     await display_redis_data()
#
#     scenarios = [
#         ("Insufficient Balance Race", scenario_insufficient_balance_race),
#         ("High-Frequency Same Pair", scenario_high_frequency_same_pair),
#         ("Bidirectional Transfers", scenario_bidirectional_transfers),
#         ("Transfer Chain", scenario_transfer_chain),
#         ("Same-User Transfer", scenario_same_user_transfer),
#         ("Massive Contention", scenario_massive_contention),
#         ("Rapid Fire Random", scenario_rapid_fire_random),
#     ]
#
#     results = {}
#     for name, scenario_func in scenarios:
#         try:
#             logger.info(f"\n▶️  Running: {name}")
#             result = await scenario_func()
#             results[name] = result
#             await asyncio.sleep(2)  # Cool down between scenarios
#         except Exception as e:
#             logger.error(f"❌ Scenario '{name}' failed: {e}", exc_info=True)
#             results[name] = None
#
#     # Summary
#     logger.info("\n")
#     logger.info("=" * 90)
#     logger.info("FINAL SUMMARY")
#     logger.info("=" * 90)
#
#     for name, result in results.items():
#         if result:
#             status = "✅ PASSED"
#         else:
#             status = "❌ FAILED"
#         logger.info(f"{status} | {name}")
#
#     logger.info("=" * 90)
#
#     return results
#
#
# async def main():
#     global redis_client
#
#     logger.info("=" * 90)
#     logger.info("SMART KAFKA PRODUCER - WALLET RACE CONDITION TESTS")
#     logger.info("=" * 90)
#
#     try:
#         logger.info("Connecting to Redis...")
#         redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
#         await redis_client.ping()
#         logger.info("  Redis connected\n")
#
#         logger.info("Starting Kafka broker...")
#         await broker.start()
#         logger.info("  Kafka broker started\n")
#
#         # Run all test scenarios
#         await run_all_scenarios()
#
#         logger.info("\n  All tests completed!")
#         logger.info("Shutting down...")
#         await broker.stop()
#         await redis_client.aclose()
#         logger.info("Done!\n")
#
#     except Exception as e:
#         logger.error(f"Fatal error: {e}", exc_info=True)
#         try:
#             await broker.stop()
#             await redis_client.aclose()
#         except:
#             pass
#
#
if __name__ == "__main__":
    try:
        asyncio.run(request_transfer("user_6", "user_1", 100, "USD", "12341231231231", True))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
