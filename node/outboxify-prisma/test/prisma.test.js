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

const { createOutboxifyPrismaExtension } = require('../src/index');

test('Prisma client extension intercepts create query and triggers fast-path dispatch', async () => {
  const repo = new InMemoryOutboxRepository();
  const broker = new MockBrokerPublisher();
  const dispatcher = new OutboxDispatcher(repo, broker);
  const config = new PipelineConfig({ name: 'orders', tableName: 'ORDERS' });
  const hook = new OutboxHook(dispatcher, (name) => (name === 'orders' ? config : null));

  const extension = createOutboxifyPrismaExtension(hook, ['Order']);

  // Simulate Prisma query execution
  const mockArgs = { data: { id: 'ord-prisma-1', topic: 'orders.v1', payload: '{"amount":42}' } };
  const mockQuery = async (args) => {
    return {
      outboxPipeline: 'orders',
      id: 'ord-prisma-1',
      topic: 'orders.v1',
      customerId: 'cust-99',
      payload: '{"amount":42}'
    };
  };

  const result = await extension.query.$allModels.create({
    model: 'Order',
    operation: 'create',
    args: mockArgs,
    query: mockQuery
  });

  assert.equal(result.id, 'ord-prisma-1');

  // Await async dispatch
  await new Promise((resolve) => setTimeout(resolve, 50));

  assert.equal(broker.publishedRecords.length, 1);
  assert.equal(broker.publishedRecords[0].getOutboxId(), 'ord-prisma-1');
  assert.equal(broker.publishedRecords[0].getTopic(), 'orders.v1');
});
