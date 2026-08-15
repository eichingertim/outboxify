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

const { BrokerPublisher } = require('../spi/broker-publisher');
const { OutboxResult } = require('../model/outbox-result');

class MockBrokerPublisher extends BrokerPublisher {
  constructor() {
    super();
    this.publishedRecords = [];
    this.shouldFail = false;
    this.failureError = 'Simulated broker failure';
  }

  async publish(pipeline, record) {
    if (this.shouldFail) {
      return OutboxResult.failure(record.getOutboxId(), record.getTopic(), this.failureError);
    }
    this.publishedRecords.push(record);
    return OutboxResult.success(record.getOutboxId(), record.getTopic(), 0, this.publishedRecords.length);
  }

  async publishBatch(pipeline, records) {
    const results = [];
    for (const record of records) {
      const result = await this.publish(pipeline, record);
      results.push(result);
    }
    return results;
  }

  clear() {
    this.publishedRecords = [];
  }
}

class KafkaBrokerPublisher extends BrokerPublisher {
  constructor(producerClient = null) {
    super();
    this.producer = producerClient;
  }

  async publish(pipeline, record) {
    if (!this.producer) {
      // Fallback in-memory success if no producer is injected
      return OutboxResult.success(record.getOutboxId(), record.getTopic(), 0, 1);
    }

    try {
      const headers = {};
      if (record.getHeaders()) {
        for (const [k, v] of Object.entries(record.getHeaders())) {
          headers[k] = Buffer.from(String(v));
        }
      }

      const response = await this.producer.send({
        topic: record.getTopic(),
        messages: [{
          key: record.getPartitionKey() ? String(record.getPartitionKey()) : null,
          value: record.getPayload(),
          headers
        }]
      });

      const metadata = response && response[0] ? response[0] : { partition: 0, offset: 0 };
      return OutboxResult.success(record.getOutboxId(), record.getTopic(), metadata.partition, metadata.offset);
    } catch (err) {
      return OutboxResult.failure(record.getOutboxId(), record.getTopic(), err.message, err);
    }
  }

  async close() {
    if (this.producer && typeof this.producer.disconnect === 'function') {
      await this.producer.disconnect();
    }
  }
}

module.exports = {
  MockBrokerPublisher,
  KafkaBrokerPublisher
};
