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
  OutboxPayload
} = require('../../outboxify-core/src/index');

const { OutboxifyService, OutboxifyModule } = require('../src/index');

test('NestJS dynamic module and service manages lifecycles and provides publisher', async () => {
  const repo = new InMemoryOutboxRepository();
  const broker = new MockBrokerPublisher();

  const moduleDef = OutboxifyModule.forRoot({
    pipelines: {
      orders: new PipelineConfig({ name: 'orders', tableName: 'ORDERS', pollIntervalMs: 50 }),
      payments: new PipelineConfig({ name: 'payments', tableName: 'PAYMENTS', pollIntervalMs: 50 })
    },
    repository: repo,
    brokerPublisher: broker
  });

  assert.equal(moduleDef.module, OutboxifyModule);
  assert.equal(moduleDef.providers.length, 1);

  const service = moduleDef.providers[0].useFactory();
  assert.ok(service instanceof OutboxifyService);
  assert.equal(service.pollers.length, 2);
  assert.equal(service.reapers.length, 2);

  // Initialize lifecycle
  service.onModuleInit();
  assert.ok(service.pollers[0].running);

  // Publish record via service
  const result = await service.publish('orders', OutboxPayload.of('orders.v1', '{"item":"book"}'));
  assert.ok(result.recordId);

  // Await async dispatch
  await new Promise((resolve) => setTimeout(resolve, 60));

  assert.equal(broker.publishedRecords.length, 1);
  assert.equal(broker.publishedRecords[0].getTopic(), 'orders.v1');

  // Destroy lifecycle
  await service.onModuleDestroy();
  assert.equal(service.pollers[0].running, false);
});
