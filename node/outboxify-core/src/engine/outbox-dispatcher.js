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

const { DefaultOutboxRecord } = require('../model/outbox-record');
const { OutboxResult } = require('../model/outbox-result');

class OutboxDispatcher {
  /**
   * @param {import('../spi/outbox-repository').OutboxRepository} repository
   * @param {import('../spi/broker-publisher').BrokerPublisher} brokerPublisher
   */
  constructor(repository, brokerPublisher) {
    if (!repository) throw new Error('repository must not be null');
    if (!brokerPublisher) throw new Error('brokerPublisher must not be null');

    this.repository = repository;
    this.brokerPublisher = brokerPublisher;
  }

  /**
   * Fast-Path single record dispatch triggered by post-commit hook.
   */
  async dispatchFastPath(config, recordId, payload) {
    const record = DefaultOutboxRecord.fromPayload(config.name, recordId, payload);
    try {
      const result = await this.brokerPublisher.publish(config.name, record);
      if (result.isSuccess()) {
        await this.repository.markSentSingle(config, recordId);
      } else {
        await this.repository.markFailedSingle(config, recordId, result.errorMessage || 'Broker publish failed');
      }
      return result;
    } catch (err) {
      await this.repository.markFailedSingle(config, recordId, err.message);
      return OutboxResult.failure(recordId, payload.topic, err.message, err);
    }
  }

  /**
   * Slow-Path batch dispatch triggered by poller engine.
   */
  async dispatchBatch(config, records) {
    if (!records || records.length === 0) {
      return [];
    }

    try {
      const results = await this.brokerPublisher.publishBatch(config.name, records);
      const sentIds = [];
      const failedMap = new Map();

      for (const res of results) {
        if (res.isSuccess()) {
          sentIds.push(res.recordId);
        } else {
          failedMap.set(res.recordId, res.errorMessage || 'Broker batch publish failed');
        }
      }

      if (sentIds.length > 0) {
        await this.repository.markSent(config, sentIds);
      }

      for (const [id, errorMsg] of failedMap.entries()) {
        await this.repository.markFailedSingle(config, id, errorMsg);
      }

      return results;
    } catch (err) {
      const failedIds = records.map(r => r.getOutboxId());
      await this.repository.markFailed(config, failedIds, err.message);
      return records.map(r => OutboxResult.failure(r.getOutboxId(), r.getTopic(), err.message, err));
    }
  }
}

module.exports = {
  OutboxDispatcher
};
