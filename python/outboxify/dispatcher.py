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

from typing import List
from .broker import BrokerPublisher
from .models import DefaultOutboxRecord, OutboxPayload, OutboxResult, PipelineConfig
from .repository import OutboxRepository


class OutboxDispatcher:
    def __init__(self, repository: OutboxRepository, broker_publisher: BrokerPublisher):
        self.repository = repository
        self.broker_publisher = broker_publisher

    async def dispatch_fast_path(self, config: PipelineConfig, record_id: str, payload: OutboxPayload) -> OutboxResult:
        record = DefaultOutboxRecord.from_payload(config.name, record_id, payload)
        try:
            result = await self.broker_publisher.publish(config.name, record)
            if result.is_success:
                await self.repository.mark_sent_single(config, record_id)
            else:
                await self.repository.mark_failed_single(config, record_id, result.error_message or "Broker publish failed")
            return result
        except Exception as e:
            await self.repository.mark_failed_single(config, record_id, str(e))
            return OutboxResult.failure(record_id, payload.topic, str(e))

    async def dispatch_batch(self, config: PipelineConfig, records: List[DefaultOutboxRecord]) -> List[OutboxResult]:
        if not records:
            return []

        try:
            results = await self.broker_publisher.publish_batch(config.name, records)
            sent_ids: List[str] = []
            for r in results:
                if r.is_success:
                    sent_ids.append(r.record_id)
                else:
                    await self.repository.mark_failed_single(config, r.record_id, r.error_message or "Batch publish failed")

            if sent_ids:
                await self.repository.mark_sent(config, sent_ids)

            return results
        except Exception as e:
            failed_ids = [r.outbox_id for r in records]
            await self.repository.mark_failed(config, failed_ids, str(e))
            return [OutboxResult.failure(r.outbox_id, r.topic, str(e)) for r in records]
