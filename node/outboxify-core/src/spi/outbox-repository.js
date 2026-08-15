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

/**
 * Storage SPI for managing outbox record lifecycle states in Node.js.
 */
class OutboxRepository {
  async fetchBatchForUpdate(config, batchSize) { throw new Error('Not implemented'); }
  async markProcessing(config, recordIds) { throw new Error('Not implemented'); }
  async markSent(config, recordIds) { throw new Error('Not implemented'); }
  async markSentSingle(config, recordId) { throw new Error('Not implemented'); }
  async markFailed(config, recordIds, errorMessage) { throw new Error('Not implemented'); }
  async markFailedSingle(config, recordId, errorMessage) { throw new Error('Not implemented'); }
  async reapStaleRecords(config, timeoutSeconds, maxRetries) { throw new Error('Not implemented'); }
  async insertRecord(config, record) { throw new Error('Not implemented'); }
  async findById(config, recordId) { throw new Error('Not implemented'); }
}

module.exports = {
  OutboxRepository
};
