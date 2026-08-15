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
const { OutboxStatus } = require('./outbox-status');

class DefaultOutboxRecord {
  /**
   * @param {Object} options
   * @param {string} [options.outboxPipeline]
   * @param {string} [options.outboxId]
   * @param {string} options.topic
   * @param {string} [options.partitionKey]
   * @param {string} options.payload
   * @param {Record<string, string>} [options.headers]
   * @param {string} [options.status]
   * @param {number} [options.retryCount]
   * @param {string} [options.lastError]
   * @param {Date} [options.createdAt]
   * @param {Date} [options.updatedAt]
   * @param {Date} [options.processedAt]
   */
  constructor(options) {
    this.outboxPipeline = options.outboxPipeline || 'default';
    this.outboxId = options.outboxId || crypto.randomUUID();
    this.topic = options.topic;
    this.partitionKey = options.partitionKey || null;
    this.payload = options.payload;
    this.headers = options.headers || {};
    this.status = options.status || OutboxStatus.NEW;
    this.retryCount = options.retryCount || 0;
    this.lastError = options.lastError || null;
    this.createdAt = options.createdAt || new Date();
    this.updatedAt = options.updatedAt || new Date();
    this.processedAt = options.processedAt || null;
  }

  getOutboxPipeline() { return this.outboxPipeline; }
  getOutboxId() { return this.outboxId; }
  getTopic() { return this.topic; }
  getPartitionKey() { return this.partitionKey; }
  getPayload() { return this.payload; }
  getHeaders() { return this.headers; }
  getStatus() { return this.status; }
  getRetryCount() { return this.retryCount; }
  getLastError() { return this.lastError; }
  getCreatedAt() { return this.createdAt; }
  getUpdatedAt() { return this.updatedAt; }
  getProcessedAt() { return this.processedAt; }

  static fromPayload(pipeline, id, payload) {
    return new DefaultOutboxRecord({
      outboxPipeline: pipeline,
      outboxId: id,
      topic: payload.topic,
      partitionKey: payload.partitionKey,
      payload: payload.payload,
      headers: payload.headers,
      status: OutboxStatus.NEW
    });
  }
}

module.exports = {
  DefaultOutboxRecord
};
