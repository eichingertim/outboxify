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

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
from typing import Any, Dict, Optional
import uuid


class OutboxStatus(str, Enum):
    NEW = "NEW"
    PROCESSING = "PROCESSING"
    SENT = "SENT"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


@dataclass
class OutboxPayload:
    topic: str
    payload: str
    partition_key: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def of(cls, topic: str, payload: Any, partition_key: Optional[str] = None, headers: Optional[Dict[str, str]] = None) -> "OutboxPayload":
        payload_str = payload if isinstance(payload, str) else json.dumps(payload)
        return cls(topic=topic, payload=payload_str, partition_key=partition_key, headers=headers or {})


@dataclass
class OutboxResult:
    record_id: str
    status: OutboxStatus
    topic: str
    partition: int = -1
    offset: int = -1
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: Optional[str] = None

    @property
    def is_success(self) -> bool:
        return self.status == OutboxStatus.SENT

    @classmethod
    def success(cls, record_id: str, topic: str, partition: int = 0, offset: int = 0) -> "OutboxResult":
        return cls(record_id=record_id, status=OutboxStatus.SENT, topic=topic, partition=partition, offset=offset)

    @classmethod
    def failure(cls, record_id: str, topic: str, error_message: str) -> "OutboxResult":
        return cls(record_id=record_id, status=OutboxStatus.FAILED, topic=topic, error_message=error_message)


@dataclass
class DefaultOutboxRecord:
    outbox_pipeline: str = "default"
    outbox_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic: str = ""
    partition_key: Optional[str] = None
    payload: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    status: OutboxStatus = OutboxStatus.NEW
    retry_count: int = 0
    last_error: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    processed_at: Optional[datetime] = None

    @classmethod
    def from_payload(cls, pipeline: str, record_id: str, payload: OutboxPayload) -> "DefaultOutboxRecord":
        return cls(
            outbox_pipeline=pipeline,
            outbox_id=record_id,
            topic=payload.topic,
            partition_key=payload.partition_key,
            payload=payload.payload,
            headers=payload.headers,
            status=OutboxStatus.NEW
        )


@dataclass
class ColumnMapping:
    id: str = "id"
    topic: str = "topic"
    partition_key: str = "partition_key"
    payload: str = "payload"
    headers: str = "headers"
    status: str = "status"
    retry_count: str = "retry_count"
    last_error: str = "last_error"
    created_at: str = "created_at"
    updated_at: str = "updated_at"
    processed_at: str = "processed_at"


@dataclass
class PipelineConfig:
    name: str = "default"
    enabled: bool = True
    table_name: str = "OUTBOX_RECORD"
    dialect: str = "AUTO_DETECT"
    batch_size: int = 100
    poll_interval_seconds: float = 1.0
    processing_timeout_seconds: int = 300
    reaper_interval_seconds: float = 10.0
    max_retries: int = 5
    immediate_send_enabled: bool = True
    columns: ColumnMapping = field(default_factory=ColumnMapping)
    broker_type: str = "KAFKA"
