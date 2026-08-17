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

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field
from sqlalchemy import Column, String, Float, Integer, Text, DateTime
from .database import Base


class Order(Base):
    __tablename__ = "orders"

    id = Column(String(64), primary_key=True, index=True)
    customer_id = Column(String(64), nullable=False)
    item = Column(String(255), nullable=False)
    amount = Column(Float, nullable=False)
    status = Column(String(32), default="CONFIRMED")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class OrderOutboxRecord(Base):
    __tablename__ = "orders_outbox"

    id = Column(String(64), primary_key=True, index=True)
    topic = Column(String(255), nullable=False)
    partition_key = Column(String(255), nullable=True)
    payload = Column(Text, nullable=False)
    headers = Column(Text, nullable=True)
    status = Column(String(32), default="NEW", nullable=False)
    retry_count = Column(Integer, default=0)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    processed_at = Column(DateTime, nullable=True)


class OrderCreate(BaseModel):
    customer_id: str = Field(..., example="cust-301")
    item: str = Field(..., example="Wireless Noise-Cancelling Earbuds")
    amount: float = Field(..., example=199.99)


class OrderRead(BaseModel):
    id: str
    customer_id: str
    item: str
    amount: float
    status: str
    created_at: datetime

    class Config:
        from_attributes = True
