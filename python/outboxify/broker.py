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

from abc import ABC, abstractmethod
from typing import List
from .models import DefaultOutboxRecord, OutboxResult


class BrokerPublisher(ABC):
    @abstractmethod
    async def publish(self, pipeline: str, record: DefaultOutboxRecord) -> OutboxResult:
        pass

    async def publish_batch(self, pipeline: str, records: List[DefaultOutboxRecord]) -> List[OutboxResult]:
        results: List[OutboxResult] = []
        for r in records:
            results.append(await self.publish(pipeline, r))
        return results

    async def close(self) -> None:
        pass


class MockBrokerPublisher(BrokerPublisher):
    def __init__(self):
        self.published_records: List[DefaultOutboxRecord] = []
        self.should_fail: bool = False
        self.failure_error: str = "Simulated broker error"

    async def publish(self, pipeline: str, record: DefaultOutboxRecord) -> OutboxResult:
        if self.should_fail:
            return OutboxResult.failure(record.outbox_id, record.topic, self.failure_error)
        self.published_records.append(record)
        return OutboxResult.success(record.outbox_id, record.topic, partition=0, offset=len(self.published_records))

    def clear(self) -> None:
        self.published_records.clear()


class KafkaBrokerPublisher(BrokerPublisher):
    def __init__(self, producer_client=None):
        self.producer = producer_client

    async def publish(self, pipeline: str, record: DefaultOutboxRecord) -> OutboxResult:
        if not self.producer:
            return OutboxResult.success(record.outbox_id, record.topic, 0, 1)

        try:
            key_bytes = record.partition_key.encode("utf-8") if record.partition_key else None
            val_bytes = record.payload.encode("utf-8")
            headers = [(k, v.encode("utf-8")) for k, v in record.headers.items()] if record.headers else None

            metadata = await self.producer.send_and_wait(
                topic=record.topic,
                value=val_bytes,
                key=key_bytes,
                headers=headers
            )
            partition = getattr(metadata, "partition", 0)
            offset = getattr(metadata, "offset", 0)
            return OutboxResult.success(record.outbox_id, record.topic, partition, offset)
        except Exception as e:
            return OutboxResult.failure(record.outbox_id, record.topic, str(e))

    async def close(self) -> None:
        if self.producer and hasattr(self.producer, "stop"):
            await self.producer.stop()
