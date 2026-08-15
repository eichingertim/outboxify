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
const { OutboxRepository } = require('../spi/outbox-repository');
const { OutboxStatus } = require('../model/outbox-status');
const { DefaultOutboxRecord } = require('../model/outbox-record');

class InMemoryOutboxRepository extends OutboxRepository {
  constructor() {
    super();
    /** @type {Map<string, DefaultOutboxRecord>} */
    this.records = new Map();
  }

  async fetchBatchForUpdate(config, batchSize) {
    const matched = [];
    const maxRetries = config.maxRetries !== undefined ? config.maxRetries : 5;

    for (const record of this.records.values()) {
      if (
        (record.status === OutboxStatus.NEW || record.status === OutboxStatus.FAILED) &&
        record.retryCount < maxRetries
      ) {
        matched.push(record);
        if (matched.length >= batchSize) {
          break;
        }
      }
    }

    // Atomically mark claimed records as PROCESSING
    for (const r of matched) {
      r.status = OutboxStatus.PROCESSING;
      r.updatedAt = new Date();
    }

    return matched;
  }

  async markProcessing(config, recordIds) {
    let count = 0;
    for (const id of recordIds) {
      const record = this.records.get(id);
      if (record) {
        record.status = OutboxStatus.PROCESSING;
        record.updatedAt = new Date();
        count++;
      }
    }
    return count;
  }

  async markSent(config, recordIds) {
    let count = 0;
    for (const id of recordIds) {
      const record = this.records.get(id);
      if (record) {
        record.status = OutboxStatus.SENT;
        record.processedAt = new Date();
        record.updatedAt = new Date();
        count++;
      }
    }
    return count;
  }

  async markSentSingle(config, recordId) {
    return this.markSent(config, [recordId]);
  }

  async markFailed(config, recordIds, errorMessage) {
    let count = 0;
    for (const id of recordIds) {
      const record = this.records.get(id);
      if (record) {
        record.status = OutboxStatus.FAILED;
        record.retryCount += 1;
        record.lastError = errorMessage;
        record.updatedAt = new Date();
        count++;
      }
    }
    return count;
  }

  async markFailedSingle(config, recordId, errorMessage) {
    return this.markFailed(config, [recordId], errorMessage);
  }

  async reapStaleRecords(config, timeoutSeconds, maxRetries) {
    const now = Date.now();
    const thresholdMs = timeoutSeconds * 1000;
    let reaped = 0;

    for (const record of this.records.values()) {
      if (record.status === OutboxStatus.PROCESSING) {
        const age = now - record.updatedAt.getTime();
        if (age >= thresholdMs) {
          record.status = OutboxStatus.FAILED;
          record.retryCount += 1;
          record.lastError = 'PROCESSING_TIMEOUT_EXCEEDED';
          record.updatedAt = new Date();
          reaped++;
        }
      }
    }
    return reaped;
  }

  async insertRecord(config, record) {
    const id = record.getOutboxId() || crypto.randomUUID();
    const outboxRecord = new DefaultOutboxRecord({
      outboxPipeline: config.name,
      outboxId: id,
      topic: record.getTopic(),
      partitionKey: record.getPartitionKey(),
      payload: record.getPayload(),
      headers: record.getHeaders(),
      status: record.getStatus() || OutboxStatus.NEW,
      retryCount: record.getRetryCount() || 0,
      lastError: record.getLastError() || null,
      createdAt: record.getCreatedAt() || new Date(),
      updatedAt: record.getUpdatedAt() || new Date()
    });

    this.records.set(id, outboxRecord);
    return id;
  }

  async findById(config, recordId) {
    return this.records.get(recordId) || null;
  }

  clear() {
    this.records.clear();
  }
}

module.exports = {
  InMemoryOutboxRepository
};
