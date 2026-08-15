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

class ColumnMapping {
  constructor(custom = {}) {
    this.id = custom.id || 'id';
    this.topic = custom.topic || 'topic';
    this.partitionKey = custom.partitionKey || 'partition_key';
    this.payload = custom.payload || 'payload';
    this.headers = custom.headers || 'headers';
    this.status = custom.status || 'status';
    this.retryCount = custom.retryCount || 'retry_count';
    this.lastError = custom.lastError || 'last_error';
    this.createdAt = custom.createdAt || 'created_at';
    this.updatedAt = custom.updatedAt || 'updated_at';
    this.processedAt = custom.processedAt || 'processed_at';
  }
}

class PipelineConfig {
  constructor(options = {}) {
    this.name = options.name || 'default';
    this.enabled = options.enabled !== undefined ? Boolean(options.enabled) : true;
    this.tableName = options.tableName || 'OUTBOX_RECORD';
    this.dialect = options.dialect || 'AUTO_DETECT';
    this.batchSize = options.batchSize !== undefined ? Number(options.batchSize) : 100;
    this.pollIntervalMs = options.pollIntervalMs !== undefined ? Number(options.pollIntervalMs) : 1000;
    this.processingTimeoutSeconds = options.processingTimeoutSeconds !== undefined ? Number(options.processingTimeoutSeconds) : 300;
    this.reaperIntervalMs = options.reaperIntervalMs !== undefined ? Number(options.reaperIntervalMs) : 10000;
    this.maxRetries = options.maxRetries !== undefined ? Number(options.maxRetries) : 5;
    this.pollerThreads = options.pollerThreads !== undefined ? Number(options.pollerThreads) : 1;
    this.immediateSendEnabled = options.immediateSend?.enabled !== undefined ? Boolean(options.immediateSend.enabled) : true;
    this.columns = new ColumnMapping(options.columns);
    this.broker = options.broker || { type: 'KAFKA', producer: {} };
  }
}

module.exports = {
  ColumnMapping,
  PipelineConfig
};
