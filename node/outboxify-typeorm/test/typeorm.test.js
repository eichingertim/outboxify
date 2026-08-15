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

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  PipelineConfig,
  InMemoryOutboxRepository,
  MockBrokerPublisher,
  OutboxDispatcher,
  OutboxHook
} = require('../../outboxify-core/src/index');

const { OutboxEntitySubscriber } = require('../src/index');

test('TypeORM subscriber hooks into transaction commit and executes fast-path dispatch', async () => {
  const repo = new InMemoryOutboxRepository();
  const broker = new MockBrokerPublisher();
  const dispatcher = new OutboxDispatcher(repo, broker);
  const config = new PipelineConfig({ name: 'orders', tableName: 'ORDERS' });
  const hook = new OutboxHook(dispatcher, (name) => (name === 'orders' ? config : null));

  const subscriber = new OutboxEntitySubscriber(hook);

  // Mock TypeORM QueryRunner
  let committed = false;
  const mockQueryRunner = {
    isTransactionActive: true,
    data: {},
    commitTransaction: async function() {
      committed = true;
    }
  };

  // Mock domain entity with embedded outbox interface
  const orderEntity = {
    getOutboxPipeline: () => 'orders',
    getOutboxId: () => 'ord-555',
    getTopic: () => 'orders.events',
    getPartitionKey: () => 'cust-123',
    getPayload: () => '{"orderId":"ord-555","total":150.0}',
    getHeaders: () => ({ eventType: 'OrderCreated' })
  };

  // 1. Entity inserted in active transaction
  subscriber.afterInsert({ entity: orderEntity, queryRunner: mockQueryRunner });

  // Broker has not received message before commit
  assert.equal(broker.publishedRecords.length, 0);

  // 2. Commit transaction
  await mockQueryRunner.commitTransaction();
  assert.ok(committed);

  // Await async dispatch
  await new Promise((resolve) => setTimeout(resolve, 50));

  // 3. Broker received dispatched record
  assert.equal(broker.publishedRecords.length, 1);
  assert.equal(broker.publishedRecords[0].getOutboxId(), 'ord-555');
  assert.equal(broker.publishedRecords[0].getTopic(), 'orders.events');
  assert.equal(broker.publishedRecords[0].getPartitionKey(), 'cust-123');
});
