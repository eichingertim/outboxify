/*
 * Copyright 2026 Outboxify Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const crypto = require('crypto');
const { InMemoryOutboxRepository, OutboxPayload } = require('@outboxify/core');

class OrderDatabase {
  constructor(pipelineConfig, outboxPublisher) {
    this.pipelineConfig = pipelineConfig;
    this.outboxPublisher = outboxPublisher;
    /** @type {Map<string, Object>} */
    this.orders = new Map();
  }

  /**
   * Transactional Order Creation + Fast-Path Outbox Staging
   */
  async createOrder({ customerId, item, amount }) {
    const orderId = `ord-${crypto.randomUUID().substring(0, 8)}`;
    const now = new Date().toISOString();

    // 1. Stage domain record
    const order = {
      id: orderId,
      customerId,
      item,
      amount: Number(amount),
      status: 'CONFIRMED',
      createdAt: now
    };

    // 2. Stage Outbox event
    const eventPayload = JSON.stringify({
      orderId,
      customerId,
      item,
      amount: Number(amount),
      timestamp: now
    });

    const payload = OutboxPayload.of('orders.events', eventPayload, customerId);
    const result = await this.outboxPublisher.publish('orders', payload);

    // 3. Commit domain record
    this.orders.set(orderId, order);

    return {
      order,
      outboxRecordId: result.recordId
    };
  }

  /**
   * Simulated transaction failure to demonstrate rollback safety
   */
  async createOrderWithFailure({ customerId, item, amount }) {
    const orderId = `ord-fail-${crypto.randomUUID().substring(0, 8)}`;
    const now = new Date().toISOString();

    // Simulating transactional staging
    const pendingOrder = {
      id: orderId,
      customerId,
      item,
      amount: Number(amount),
      status: 'PENDING',
      createdAt: now
    };

    // An unexpected failure occurs before commit!
    throw new Error(`Simulated Payment Gateway Crash for order ${orderId} — rolling back transaction.`);
  }

  getAllOrders() {
    return Array.from(this.orders.values()).sort(
      (a, b) => new Date(b.createdAt) - new Date(a.createdAt)
    );
  }

  async getOutboxRecords() {
    const repository = this.outboxPublisher.dispatcher.repository;
    return Array.from(repository.records.values()).map(r => ({
      id: r.getOutboxId(),
      pipeline: r.getOutboxPipeline(),
      topic: r.getTopic(),
      partitionKey: r.getPartitionKey(),
      payload: r.getPayload(),
      status: r.getStatus(),
      retryCount: r.getRetryCount(),
      lastError: r.getLastError(),
      createdAt: r.getCreatedAt(),
      updatedAt: r.getUpdatedAt(),
      processedAt: r.getProcessedAt()
    }));
  }
}

module.exports = {
  OrderDatabase
};
