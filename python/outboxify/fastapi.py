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

from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, List
from .broker import BrokerPublisher
from .dispatcher import OutboxDispatcher
from .models import PipelineConfig
from .poller import OutboxPollerEngine
from .publisher import OutboxHook, OutboxPublisher
from .reaper import OutboxReaperJob
from .repository import OutboxRepository


class OutboxifyAppManager:
    def __init__(
        self,
        pipelines: Dict[str, PipelineConfig],
        repository: OutboxRepository,
        broker_publisher: BrokerPublisher
    ):
        self.pipelines = pipelines
        self.repository = repository
        self.broker_publisher = broker_publisher
        self.dispatcher = OutboxDispatcher(self.repository, self.broker_publisher)
        self.hook = OutboxHook(self.dispatcher, lambda name: self.pipelines.get(name))
        self.publisher = OutboxPublisher(self.repository, self.hook, self.dispatcher, lambda name: self.pipelines.get(name))

        self.pollers: List[OutboxPollerEngine] = []
        self.reapers: List[OutboxReaperJob] = []

        for cfg in self.pipelines.values():
            if cfg.enabled:
                self.pollers.append(OutboxPollerEngine(cfg, self.repository, self.dispatcher))
                self.reapers.append(OutboxReaperJob(cfg, self.repository))

    async def start(self) -> None:
        for p in self.pollers:
            await p.start()
        for r in self.reapers:
            await r.start()

    async def stop(self) -> None:
        for p in self.pollers:
            await p.stop()
        for r in self.reapers:
            await r.stop()
        await self.broker_publisher.close()


def create_outboxify_lifespan(manager: OutboxifyAppManager):
    @asynccontextmanager
    async def lifespan(app) -> AsyncIterator[None]:
        await manager.start()
        try:
            yield
        finally:
            await manager.stop()

    return lifespan
