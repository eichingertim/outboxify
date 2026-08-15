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
from typing import Callable, Optional
import uuid
from .dispatcher import OutboxDispatcher
from .models import DefaultOutboxRecord, OutboxPayload, OutboxResult, PipelineConfig
from .repository import OutboxRepository


class OutboxHook:
    def __init__(self, dispatcher: OutboxDispatcher, config_resolver: Callable[[str], Optional[PipelineConfig]]):
        self.dispatcher = dispatcher
        self.config_resolver = config_resolver

    def register_for_commit(self, pipeline: str, record_id: str, payload: OutboxPayload, tx_context=None) -> None:
        config = self.config_resolver(pipeline)
        if not config or not config.immediate_send_enabled:
            return

        if tx_context and hasattr(tx_context, "on_commit"):
            tx_context.on_commit(lambda: asyncio.create_task(self.dispatcher.dispatch_fast_path(config, record_id, payload)))
        else:
            asyncio.create_task(self.dispatcher.dispatch_fast_path(config, record_id, payload))


class OutboxPublisher:
    def __init__(
        self,
        repository: OutboxRepository,
        hook: OutboxHook,
        dispatcher: OutboxDispatcher,
        config_resolver: Callable[[str], Optional[PipelineConfig]]
    ):
        self.repository = repository
        self.hook = hook
        self.dispatcher = dispatcher
        self.config_resolver = config_resolver

    async def publish(self, pipeline: str, payload: OutboxPayload, tx_context=None) -> OutboxResult:
        config = self.config_resolver(pipeline)
        if not config:
            raise ValueError(f"Pipeline '{pipeline}' is not configured")

        record_id = str(uuid.uuid4())
        record = DefaultOutboxRecord.from_payload(pipeline, record_id, payload)

        await self.repository.insert_record(config, record, tx_context)
        self.hook.register_for_commit(pipeline, record_id, payload, tx_context)

        return OutboxResult.success(record_id, payload.topic)
