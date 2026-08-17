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

import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List

# Ensure python root is importable when run standalone
root_dir = Path(__file__).resolve().parents[3] / "python"
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
import uvicorn

from outboxify import (
    PipelineConfig,
    InMemoryOutboxRepository,
    BrokerPublisher,
    OutboxRecordMixin,
    OutboxResult,
    OutboxPayload,
    DefaultOutboxRecord,
    OutboxifyAppManager,
    SqlAlchemySessionHookManager,
    create_outboxify_lifespan,
)

from .database import engine, Base, get_db
from .models import Order, OrderOutboxRecord, OrderCreate, OrderRead

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("order-service")

# Create tables in SQLite on startup
Base.metadata.create_all(bind=engine)

# 1. Configure Outboxify Pipeline
orders_pipeline = PipelineConfig(
    name="orders",
    table_name="orders_outbox",
    dialect="SQLITE",
    batch_size=50,
    poll_interval_ms=1000,
    processing_timeout_seconds=30,
    max_retries=3,
    immediate_send={"enabled": True},
)


# 2. In-Memory Broker Publisher Logger for Zero-Setup Local Demo
class DemoBrokerPublisher(BrokerPublisher):
    def __init__(self):
        self.delivered_messages = []

    async def publish(self, pipeline: str, record: DefaultOutboxRecord) -> OutboxResult:
        logger.info(
            "🔥 [KAFKA BROKER] Delivered message to topic '%s' (Key: '%s', ID: '%s'): %s",
            record.get_topic(),
            record.get_partition_key(),
            record.get_outbox_id(),
            record.get_payload(),
        )
        self.delivered_messages.append({
            "id": record.get_outbox_id(),
            "topic": record.get_topic(),
            "partition_key": record.get_partition_key(),
            "payload": json.loads(record.get_payload()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return OutboxResult.success(record.get_outbox_id(), record.get_topic(), 0, 0)

    async def publish_batch(self, pipeline: str, records: List[DefaultOutboxRecord]) -> List[OutboxResult]:
        results = []
        for r in records:
            results.append(await self.publish(pipeline, r))
        return results

    async def close(self) -> None:
        self.delivered_messages.clear()


repository = InMemoryOutboxRepository()
broker_publisher = DemoBrokerPublisher()

# 3. Create Outboxify App Lifecycle Manager
app_manager = OutboxifyAppManager(
    pipelines={"orders": orders_pipeline},
    repository=repository,
    broker_publisher=broker_publisher,
)

hook_manager = SqlAlchemySessionHookManager(app_manager.hook)
lifespan = create_outboxify_lifespan(app_manager)

# 4. Initialize FastAPI Application
app = FastAPI(
    title="Outboxify Python Order Service",
    description="Microservice demonstrating Dual-Write Transactional Outbox Pattern with Outboxify",
    version="0.1.0",
    lifespan=lifespan,
)


@app.post("/api/orders", response_model=OrderRead, status_code=status.HTTP_201_CREATED)
async def create_order(request: OrderCreate, db: Session = Depends(get_db)):
    """Creates an order and dispatches an outbox event atomically upon transaction commit."""
    order_id = f"ord-{uuid.uuid4().hex[:8]}"
    outbox_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    try:
        # 1. Write domain order to SQLite
        order = Order(
            id=order_id,
            customer_id=request.customer_id,
            item=request.item,
            amount=request.amount,
            status="CONFIRMED",
            created_at=now,
        )
        db.add(order)

        # 2. Stage outbox record in SQLite and Outboxify
        event_payload = json.dumps({
            "orderId": order_id,
            "customerId": request.customer_id,
            "item": request.item,
            "amount": request.amount,
            "timestamp": now.isoformat(),
        })

        outbox_payload = OutboxPayload(
            topic="orders.events",
            partition_key=request.customer_id,
            payload=event_payload,
        )

        outbox_record = DefaultOutboxRecord(
            outbox_pipeline="orders",
            outbox_id=outbox_id,
            topic="orders.events",
            partition_key=request.customer_id,
            payload=event_payload,
            status="NEW",
            created_at=now,
            updated_at=now,
        )
        await repository.insert_record(orders_pipeline, outbox_record)

        # Stage post-commit fast-path hook
        hook_manager.stage_record("orders", outbox_id, outbox_payload)

        # 3. Commit transaction
        db.commit()
        db.refresh(order)

        # Fast-Path post-commit dispatch
        hook_manager.on_after_commit(db)

        return order

    except Exception as e:
        db.rollback()
        hook_manager.on_after_rollback(db)
        logger.error("Transaction rolled back: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/orders/simulate-failure")
async def create_order_with_failure(request: OrderCreate, db: Session = Depends(get_db)):
    """Simulates an unexpected error during order placement to prove zero-orphan rollback safety."""
    order_id = f"ord-fail-{uuid.uuid4().hex[:8]}"
    outbox_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    try:
        order = Order(
            id=order_id,
            customer_id=request.customer_id,
            item=request.item,
            amount=request.amount,
            status="PENDING",
            created_at=now,
        )
        db.add(order)

        event_payload = json.dumps({"orderId": order_id, "customerId": request.customer_id})
        outbox_payload = OutboxPayload(topic="orders.events", partition_key=request.customer_id, payload=event_payload)
        hook_manager.stage_record("orders", outbox_id, outbox_payload)

        # Intentionally raise an error before commit
        raise RuntimeError(f"Simulated Third-Party Payment Provider Outage for order {order_id}!")

    except Exception as e:
        db.rollback()
        hook_manager.on_after_rollback(db)
        logger.warning("Simulated transaction rollback triggered successfully.")
        return {
            "status": "ROLLED_BACK",
            "error": str(e),
            "message": "Transaction rolled back. Outboxify guarantees NO orphaned message was published to Kafka!",
        }


@app.get("/api/orders", response_model=List[OrderRead])
def list_orders(db: Session = Depends(get_db)):
    """Lists all confirmed orders in the domain database."""
    return db.query(Order).order_by(Order.created_at.desc()).all()


@app.get("/api/outbox")
def list_outbox_records():
    """Lists live outbox rows in the repository."""
    return [
        {
            "id": r.get_outbox_id(),
            "pipeline": r.get_outbox_pipeline(),
            "topic": r.get_topic(),
            "partition_key": r.get_partition_key(),
            "payload": r.get_payload(),
            "status": r.get_status(),
            "retry_count": r.get_retry_count(),
            "last_error": r.get_last_error(),
            "created_at": r.get_created_at().isoformat() if r.get_created_at() else None,
            "processed_at": r.get_processed_at().isoformat() if r.get_processed_at() else None,
        }
        for r in repository.records.values()
    ]


@app.get("/api/broker/messages")
def list_broker_messages():
    """Lists events delivered to the event broker."""
    return broker_publisher.delivered_messages


if __name__ == "__main__":
    print("""
================================================================
   🚀 Outboxify Python FastAPI Order Service Started!
   
   Endpoints:
     POST http://localhost:8000/api/orders             - Create order + outbox event
     POST http://localhost:8000/api/orders/simulate-failure - Test rollback safety
     GET  http://localhost:8000/api/orders             - List orders
     GET  http://localhost:8000/api/outbox             - List outbox table rows
     GET  http://localhost:8000/api/broker/messages    - List published broker messages
     
   API Docs: http://localhost:8000/docs
================================================================
    """)
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
