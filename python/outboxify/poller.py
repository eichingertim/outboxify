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
import logging
from typing import Optional
from .dispatcher import OutboxDispatcher
from .models import PipelineConfig
from .repository import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxPollerEngine:
    def __init__(self, config: PipelineConfig, repository: OutboxRepository, dispatcher: OutboxDispatcher):
        self.config = config
        self.repository = repository
        self.dispatcher = dispatcher
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._task = None

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.config.poll_interval_seconds)
                if not self._running:
                    break
                await self.poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Poller cycle exception for pipeline %s: %s", self.config.name, e)

    async def poll_once(self) -> int:
        if not self.config.enabled:
            return 0

        records = await self.repository.fetch_batch_for_update(self.config, self.config.batch_size)
        if not records:
            return 0

        await self.dispatcher.dispatch_batch(self.config, records)
        return len(records)
