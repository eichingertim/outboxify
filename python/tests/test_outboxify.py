# Copyright 2026 Outboxify Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from datetime import datetime, timezone, timedelta
import pytest

from outboxify import (
    OutboxStatus,
    OutboxPayload,
    DefaultOutboxRecord,
    PipelineConfig,
    DialectType,
    DialectRegistry,
    InMemoryOutboxRepository,
    MockBrokerPublisher,
    OutboxDispatcher,
    OutboxPollerEngine,
    OutboxReaperJob,
    OutboxHook,
    OutboxPublisher,
    SqlAlchemySessionHookManager,
    OutboxifyAppManager,
)


def test_dialects_sql_generation():
    registry = DialectRegistry()
    config = PipelineConfig(name="orders", table_name="ORDERS", batch_size=50, max_retries=3)

    # 1. Oracle
    oracle = registry.get_dialect(DialectType.ORACLE)
    ora_query = oracle.build_select_batch_for_update_query("ORDERS", config.columns, 50, 3)
    assert "FOR UPDATE SKIP LOCKED" in ora_query
    assert "FETCH FIRST 50 ROWS ONLY" in ora_query
    assert "SYSTIMESTAMP" in oracle.build_reap_stale_query("ORDERS", config.columns, 60, 3)

    # 2. PostgreSQL
    pg = registry.get_dialect(DialectType.POSTGRESQL)
    pg_query = pg.build_select_batch_for_update_query("ORDERS", config.columns, 50, 3)
    assert "FOR UPDATE SKIP LOCKED" in pg_query
    assert "LIMIT 50" in pg_query
    assert "WHERE status IN ('NEW', 'FAILED')" in pg.get_sparse_index_ddl("ORDERS", "idx_pg_sparse", config.columns)

    # 3. MySQL
    mysql = registry.get_dialect(DialectType.MYSQL)
    mysql_query = mysql.build_select_batch_for_update_query("ORDERS", config.columns, 50, 3)
    assert "FOR UPDATE SKIP LOCKED" in mysql_query
    assert "NOW(6)" in mysql.build_reap_stale_query("ORDERS", config.columns, 60, 3)

    # 4. SQL Server
    mssql = registry.get_dialect(DialectType.MSSQL)
    mssql_query = mssql.build_select_batch_for_update_query("ORDERS", config.columns, 50, 3)
    assert "WITH (UPDLOCK, READPAST, ROWLOCK)" in mssql_query
    assert "TOP (50)" in mssql_query


@pytest.mark.asyncio
async def test_fast_path_dispatch():
    repo = InMemoryOutboxRepository()
    broker = MockBrokerPublisher()
    dispatcher = OutboxDispatcher(repo, broker)
    config = PipelineConfig(name="orders", table_name="ORDERS")

    hook = OutboxHook(dispatcher, lambda name: config if name == "orders" else None)
    publisher = OutboxPublisher(repo, hook, dispatcher, lambda name: config if name == "orders" else None)

    class MockTransactionContext:
        def __init__(self):
            self._commit_callbacks = []

        def on_commit(self, cb):
            self._commit_callbacks.append(cb)

        def commit(self):
            for cb in self._commit_callbacks:
                cb()

    tx = MockTransactionContext()
    payload = OutboxPayload.of("orders.v1", {"orderId": "ord-py-1", "amount": 49.99}, partition_key="cust-10")

    result = await publisher.publish("orders", payload, tx)
    assert result.record_id

    # Before commit: stored as NEW in database, 0 broker messages
    db_record = await repo.find_by_id(config, result.record_id)
    assert db_record.status == OutboxStatus.NEW
    assert len(broker.published_records) == 0

    # Commit transaction
    tx.commit()
    await asyncio.sleep(0.05)

    # After commit: broker received message, record status is SENT
    assert len(broker.published_records) == 1
    assert broker.published_records[0].topic == "orders.v1"
    assert broker.published_records[0].partition_key == "cust-10"

    db_record = await repo.find_by_id(config, result.record_id)
    assert db_record.status == OutboxStatus.SENT
    assert db_record.processed_at is not None


@pytest.mark.asyncio
async def test_slow_path_poller():
    repo = InMemoryOutboxRepository()
    broker = MockBrokerPublisher()
    dispatcher = OutboxDispatcher(repo, broker)
    config = PipelineConfig(name="payments", table_name="PAYMENTS", batch_size=10)

    # Seed 2 pending records
    await repo.insert_record(config, DefaultOutboxRecord(outbox_id="rec-1", topic="pay.v1", payload="msg1"))
    await repo.insert_record(config, DefaultOutboxRecord(outbox_id="rec-2", topic="pay.v1", payload="msg2"))

    poller = OutboxPollerEngine(config, repo, dispatcher)
    count = await poller.poll_once()

    assert count == 2
    assert len(broker.published_records) == 2

    r1 = await repo.find_by_id(config, "rec-1")
    r2 = await repo.find_by_id(config, "rec-2")
    assert r1.status == OutboxStatus.SENT
    assert r2.status == OutboxStatus.SENT


@pytest.mark.asyncio
async def test_reaper_watchdog():
    repo = InMemoryOutboxRepository()
    config = PipelineConfig(name="orders", processing_timeout_seconds=0)

    # Seed orphaned PROCESSING record
    old_time = datetime.now(timezone.utc) - timedelta(seconds=10)
    stale = DefaultOutboxRecord(
        outbox_id="stale-1",
        topic="orders.v1",
        payload="stale",
        status=OutboxStatus.PROCESSING,
        updated_at=old_time
    )
    await repo.insert_record(config, stale)
    repo.records["stale-1"].status = OutboxStatus.PROCESSING
    repo.records["stale-1"].updated_at = old_time

    reaper = OutboxReaperJob(config, repo)
    reaped = await reaper.reap_once()

    assert reaped == 1
    recovered = await repo.find_by_id(config, "stale-1")
    assert recovered.status == OutboxStatus.FAILED
    assert recovered.retry_count == 1
    assert recovered.last_error == "PROCESSING_TIMEOUT_EXCEEDED"


@pytest.mark.asyncio
async def test_sqlalchemy_session_hook_rollback_safety():
    repo = InMemoryOutboxRepository()
    broker = MockBrokerPublisher()
    dispatcher = OutboxDispatcher(repo, broker)
    config = PipelineConfig(name="orders", table_name="ORDERS")
    hook = OutboxHook(dispatcher, lambda name: config if name == "orders" else None)

    manager = SqlAlchemySessionHookManager(hook)
    payload = OutboxPayload.of("orders.v1", "order-data")

    # Stage record in active session
    manager.stage_record("orders", "ord-rb-1", payload)

    # Trigger Rollback
    manager.on_after_rollback(None)
    await asyncio.sleep(0.05)

    # Verify 0 messages sent to broker
    assert len(broker.published_records) == 0


@pytest.mark.asyncio
async def test_fastapi_app_manager_lifecycle():
    repo = InMemoryOutboxRepository()
    broker = MockBrokerPublisher()
    pipelines = {
        "orders": PipelineConfig(name="orders", poll_interval_seconds=0.05),
        "payments": PipelineConfig(name="payments", poll_interval_seconds=0.05),
    }

    app_manager = OutboxifyAppManager(pipelines, repo, broker)
    await app_manager.start()

    assert len(app_manager.pollers) == 2
    assert app_manager.pollers[0]._running is True

    # Publish via app manager
    result = await app_manager.publisher.publish("orders", OutboxPayload.of("orders.v1", "{\"ok\":true}"))
    assert result.record_id

    await asyncio.sleep(0.1)
    assert len(broker.published_records) == 1

    await app_manager.stop()
    assert app_manager.pollers[0]._running is False
