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

from typing import Any, Dict, List, Optional
from .models import OutboxPayload
from .publisher import OutboxHook


class OutboxRecordMixin:
    """
    SQLAlchemy declarative mixin for entities functioning as embedded outbox records (e.g. Orders).
    """
    outbox_pipeline: str = "default"
    outbox_id: Optional[str] = None
    outbox_topic: Optional[str] = None
    outbox_partition_key: Optional[str] = None
    outbox_payload: Optional[str] = None
    outbox_headers: Optional[Dict[str, str]] = None
    outbox_status: str = "NEW"

    def get_outbox_pipeline(self) -> str:
        return getattr(self, "outbox_pipeline", "default")

    def get_outbox_id(self) -> str:
        return getattr(self, "outbox_id", None) or getattr(self, "id", None)

    def get_outbox_topic(self) -> str:
        return getattr(self, "outbox_topic", None) or getattr(self, "topic", None)

    def get_outbox_partition_key(self) -> Optional[str]:
        return getattr(self, "outbox_partition_key", None) or getattr(self, "partition_key", None)

    def get_outbox_payload(self) -> str:
        return getattr(self, "outbox_payload", None) or getattr(self, "payload", "")

    def get_outbox_headers(self) -> Dict[str, str]:
        return getattr(self, "outbox_headers", None) or {}


class SqlAlchemySessionHookManager:
    """
    Hooks into SQLAlchemy Session lifecycle to trigger fast-path dispatch upon successful after_commit.
    """
    def __init__(self, hook: OutboxHook):
        self.hook = hook
        self._staged_records: List[tuple] = []

    def stage_record(self, pipeline: str, record_id: str, payload: OutboxPayload) -> None:
        self._staged_records.append((pipeline, record_id, payload))

    def on_after_commit(self, session: Any) -> None:
        to_dispatch = list(self._staged_records)
        self._staged_records.clear()
        for pipeline, record_id, payload in to_dispatch:
            self.hook.register_for_commit(pipeline, record_id, payload)

    def on_after_rollback(self, session: Any) -> None:
        # Purge staged records on rollback - zero phantom sends
        self._staged_records.clear()
