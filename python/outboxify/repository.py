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
from datetime import datetime, timezone
from typing import Dict, List, Optional
import uuid
from .models import DefaultOutboxRecord, OutboxPayload, OutboxStatus, PipelineConfig


class OutboxRepository(ABC):
    @abstractmethod
    async def fetch_batch_for_update(self, config: PipelineConfig, batch_size: int) -> List[DefaultOutboxRecord]:
        pass

    @abstractmethod
    async def mark_sent(self, config: PipelineConfig, record_ids: List[str]) -> int:
        pass

    @abstractmethod
    async def mark_sent_single(self, config: PipelineConfig, record_id: str) -> bool:
        pass

    @abstractmethod
    async def mark_failed(self, config: PipelineConfig, record_ids: List[str], error_message: str) -> int:
        pass

    @abstractmethod
    async def mark_failed_single(self, config: PipelineConfig, record_id: str, error_message: str) -> bool:
        pass

    @abstractmethod
    async def reap_stale_records(self, config: PipelineConfig, timeout_seconds: int, max_retries: int) -> int:
        pass

    @abstractmethod
    async def insert_record(self, config: PipelineConfig, record: DefaultOutboxRecord, session=None) -> str:
        pass

    @abstractmethod
    async def find_by_id(self, config: PipelineConfig, record_id: str) -> Optional[DefaultOutboxRecord]:
        pass


class InMemoryOutboxRepository(OutboxRepository):
    def __init__(self):
        self.records: Dict[str, DefaultOutboxRecord] = {}

    async def fetch_batch_for_update(self, config: PipelineConfig, batch_size: int) -> List[DefaultOutboxRecord]:
        matched: List[DefaultOutboxRecord] = []
        for r in self.records.values():
            if r.status in (OutboxStatus.NEW, OutboxStatus.FAILED) and r.retry_count < config.max_retries:
                matched.append(r)
                if len(matched) >= batch_size:
                    break

        now = datetime.now(timezone.utc)
        for r in matched:
            r.status = OutboxStatus.PROCESSING
            r.updated_at = now

        return matched

    async def mark_sent(self, config: PipelineConfig, record_ids: List[str]) -> int:
        count = 0
        now = datetime.now(timezone.utc)
        for rid in record_ids:
            if rid in self.records:
                r = self.records[rid]
                r.status = OutboxStatus.SENT
                r.processed_at = now
                r.updated_at = now
                count += 1
        return count

    async def mark_sent_single(self, config: PipelineConfig, record_id: str) -> bool:
        return (await self.mark_sent(config, [record_id])) > 0

    async def mark_failed(self, config: PipelineConfig, record_ids: List[str], error_message: str) -> int:
        count = 0
        now = datetime.now(timezone.utc)
        for rid in record_ids:
            if rid in self.records:
                r = self.records[rid]
                r.status = OutboxStatus.FAILED
                r.retry_count += 1
                r.last_error = error_message
                r.updated_at = now
                count += 1
        return count

    async def mark_failed_single(self, config: PipelineConfig, record_id: str, error_message: str) -> bool:
        return (await self.mark_failed(config, [record_id], error_message)) > 0

    async def reap_stale_records(self, config: PipelineConfig, timeout_seconds: int, max_retries: int) -> int:
        now = datetime.now(timezone.utc)
        reaped = 0
        for r in self.records.values():
            if r.status == OutboxStatus.PROCESSING:
                age_seconds = (now - r.updated_at).total_seconds()
                if age_seconds >= timeout_seconds:
                    r.status = OutboxStatus.FAILED
                    r.retry_count += 1
                    r.last_error = "PROCESSING_TIMEOUT_EXCEEDED"
                    r.updated_at = now
                    reaped += 1
        return reaped

    async def insert_record(self, config: PipelineConfig, record: DefaultOutboxRecord, session=None) -> str:
        record_id = record.outbox_id or str(uuid.uuid4())
        record.outbox_id = record_id
        record.outbox_pipeline = config.name
        self.records[record_id] = record
        return record_id

    async def find_by_id(self, config: PipelineConfig, record_id: str) -> Optional[DefaultOutboxRecord]:
        return self.records.get(record_id)
