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

const {
  PipelineConfig,
  InMemoryOutboxRepository,
  OutboxDispatcher,
  OutboxPollerEngine,
  OutboxReaperJob,
  OutboxHook,
  OutboxPublisher,
  OutboxResult,
  BrokerPublisher
} = require('@outboxify/core');
const { OrderDatabase } = require('./db');
const { createApp } = require('./app');

// 1. Configure Outboxify Pipeline
const pipelineConfig = new PipelineConfig({
  name: 'orders',
  tableName: 'ORDERS_OUTBOX',
  dialect: 'SQLITE',
  batchSize: 50,
  pollIntervalMs: 1000,
  processingTimeoutSeconds: 30,
  maxRetries: 3,
  immediateSend: { enabled: true }
});

// 2. In-Memory Broker Publisher Logger for Zero-Setup Local Demo
class DemoBrokerPublisher extends BrokerPublisher {
  constructor() {
    super();
    this.deliveredMessages = [];
  }

  async publish(pipeline, record) {
    console.log(
      `🔥 [KAFKA BROKER] Delivered message to topic '${record.getTopic()}' (Key: '${record.getPartitionKey()}', ID: '${record.getOutboxId()}'): ${record.getPayload()}`
    );
    this.deliveredMessages.push({
      id: record.getOutboxId(),
      topic: record.getTopic(),
      key: record.getPartitionKey(),
      payload: JSON.parse(record.getPayload()),
      timestamp: new Date().toISOString()
    });
    return OutboxResult.success(record.getOutboxId(), record.getTopic(), 0, 0);
  }

  async publishBatch(pipeline, records) {
    return Promise.all(records.map(r => this.publish(pipeline, r)));
  }

  getDeliveredMessages() {
    return this.deliveredMessages;
  }
}

// 3. Initialize Outboxify Core Engines
const repository = new InMemoryOutboxRepository();
const brokerPublisher = new DemoBrokerPublisher();
const dispatcher = new OutboxDispatcher(repository, brokerPublisher);
const configResolver = name => (name === 'orders' ? pipelineConfig : null);
const hook = new OutboxHook(dispatcher, configResolver);
const outboxPublisher = new OutboxPublisher(repository, hook, dispatcher, configResolver);

// 4. Start Background Poller & Watchdog Reaper
const poller = new OutboxPollerEngine(pipelineConfig, repository, dispatcher);
const reaper = new OutboxReaperJob(pipelineConfig, repository);

poller.start();
reaper.start();

// 5. Initialize Domain Database & HTTP Application
const orderDb = new OrderDatabase(pipelineConfig, outboxPublisher);
const app = createApp({ orderDb, brokerPublisher });

const PORT = process.env.PORT || 3001;
const server = app.listen(PORT, () => {
  console.log(`
================================================================
   🚀 Outboxify Node.js Order Service Started!
   
   Endpoints:
     POST http://localhost:${PORT}/api/orders             - Create order + outbox event
     POST http://localhost:${PORT}/api/orders/simulate-failure - Test rollback safety
     GET  http://localhost:${PORT}/api/orders             - List orders
     GET  http://localhost:${PORT}/api/outbox             - List outbox table rows
     GET  http://localhost:${PORT}/api/broker/messages    - List published broker messages
================================================================
  `);
});

// Graceful Shutdown
function shutdown() {
  console.log('\nShutting down Outboxify Node.js service...');
  poller.stop();
  reaper.stop();
  server.close(() => {
    console.log('Outboxify service terminated gracefully.');
    process.exit(0);
  });
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
