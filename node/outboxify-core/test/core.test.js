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
  OutboxStatus,
  OutboxPayload,
  DefaultOutboxRecord,
  PipelineConfig,
  DialectType,
  OracleDialect,
  PostgresDialect,
  MySqlDialect,
  SqlServerDialect,
  SqliteDialect,
  DialectRegistry,
  MockBrokerPublisher,
  InMemoryOutboxRepository,
  OutboxDispatcher,
  OutboxPollerEngine,
  OutboxReaperJob,
  OutboxHook,
  OutboxPublisher
} = require('../src/index');

test('Dialects generate correct database-specific locking and query syntax', () => {
  const registry = new DialectRegistry();
  const config = new PipelineConfig({
    name: 'orders',
    tableName: 'ORDERS',
    batchSize: 50,
    maxRetries: 3
  });

  // 1. Oracle
  const oracle = registry.getDialect(DialectType.ORACLE);
  const oracleQuery = oracle.buildSelectBatchForUpdateQuery('ORDERS', config.columns, 50, 3);
  assert.match(oracleQuery, /FOR UPDATE SKIP LOCKED/);
  assert.match(oracleQuery, /FETCH FIRST 50 ROWS ONLY/);
  assert.match(oracle.buildReapStaleQuery('ORDERS', config.columns, 60, 3), /SYSTIMESTAMP/);

  // 2. PostgreSQL
  const pg = registry.getDialect(DialectType.POSTGRESQL);
  const pgQuery = pg.buildSelectBatchForUpdateQuery('ORDERS', config.columns, 50, 3);
  assert.match(pgQuery, /FOR UPDATE SKIP LOCKED/);
  assert.match(pgQuery, /LIMIT 50/);
  assert.match(pg.getSparseIndexDdl('ORDERS', 'idx_sparse', config.columns), /WHERE status IN \('NEW', 'FAILED'\)/);

  // 3. MySQL
  const mysql = registry.getDialect(DialectType.MYSQL);
  const mysqlQuery = mysql.buildSelectBatchForUpdateQuery('ORDERS', config.columns, 50, 3);
  assert.match(mysqlQuery, /FOR UPDATE SKIP LOCKED/);
  assert.match(mysql.buildReapStaleQuery('ORDERS', config.columns, 60, 3), /NOW\(6\)/);

  // 4. SQL Server
  const mssql = registry.getDialect(DialectType.MSSQL);
  const mssqlQuery = mssql.buildSelectBatchForUpdateQuery('ORDERS', config.columns, 50, 3);
  assert.match(mssqlQuery, /WITH \(UPDLOCK, READPAST, ROWLOCK\)/);
  assert.match(mssqlQuery, /TOP \(50\)/);
});

test('Fast-Path post-commit dispatch publishes to broker and updates record to SENT', async () => {
  const repo = new InMemoryOutboxRepository();
  const broker = new MockBrokerPublisher();
  const dispatcher = new OutboxDispatcher(repo, broker);

  const config = new PipelineConfig({ name: 'orders', tableName: 'ORDERS' });
  const configResolver = (name) => (name === 'orders' ? config : null);
  const hook = new OutboxHook(dispatcher, configResolver);
  const publisher = new OutboxPublisher(repo, hook, dispatcher, configResolver);

  // Transaction simulation with onCommit callback
  let commitCallback = null;
  const mockTx = {
    onCommit: (cb) => { commitCallback = cb; }
  };

  const payload = OutboxPayload.of('orders.v1', { orderId: 'ord-100', amount: 99.99 }, 'cust-1');
  const result = await publisher.publish('orders', payload, mockTx);
  assert.ok(result.recordId);

  // Before commit: record is NEW in database, 0 broker messages
  let dbRecord = await repo.findById(config, result.recordId);
  assert.equal(dbRecord.status, OutboxStatus.NEW);
  assert.equal(broker.publishedRecords.length, 0);

  // Trigger commit
  assert.ok(commitCallback);
  commitCallback();

  // Await async post-commit dispatch
  await new Promise((resolve) => setTimeout(resolve, 50));

  assert.equal(broker.publishedRecords.length, 1);
  assert.equal(broker.publishedRecords[0].getTopic(), 'orders.v1');
  assert.equal(broker.publishedRecords[0].getPartitionKey(), 'cust-1');

  dbRecord = await repo.findById(config, result.recordId);
  assert.equal(dbRecord.status, OutboxStatus.SENT);
  assert.ok(dbRecord.processedAt);
});

test('Slow-Path poller engine claims batch and dispatches successfully', async () => {
  const repo = new InMemoryOutboxRepository();
  const broker = new MockBrokerPublisher();
  const dispatcher = new OutboxDispatcher(repo, broker);
  const config = new PipelineConfig({ name: 'payments', tableName: 'PAYMENTS', batchSize: 10 });

  // Seed 3 pending records
  await repo.insertRecord(config, DefaultOutboxRecord.fromPayload('payments', 'rec-1', OutboxPayload.of('pay.v1', 'data-1')));
  await repo.insertRecord(config, DefaultOutboxRecord.fromPayload('payments', 'rec-2', OutboxPayload.of('pay.v1', 'data-2')));
  await repo.insertRecord(config, DefaultOutboxRecord.fromPayload('payments', 'rec-3', OutboxPayload.of('pay.v1', 'data-3')));

  const poller = new OutboxPollerEngine(config, repo, dispatcher);
  const processedCount = await poller.pollOnce();

  assert.equal(processedCount, 3);
  assert.equal(broker.publishedRecords.length, 3);

  const r1 = await repo.findById(config, 'rec-1');
  const r2 = await repo.findById(config, 'rec-2');
  const r3 = await repo.findById(config, 'rec-3');

  assert.equal(r1.status, OutboxStatus.SENT);
  assert.equal(r2.status, OutboxStatus.SENT);
  assert.equal(r3.status, OutboxStatus.SENT);
});

test('Reaper watchdog recovers orphaned PROCESSING records', async () => {
  const repo = new InMemoryOutboxRepository();
  const config = new PipelineConfig({ name: 'orders', processingTimeoutSeconds: 0 }); // instant timeout for test

  // Seed record in PROCESSING status
  const staleRecord = new DefaultOutboxRecord({
    outboxPipeline: 'orders',
    outboxId: 'stale-1',
    topic: 'orders.v1',
    payload: 'stale-payload',
    status: OutboxStatus.PROCESSING,
    updatedAt: new Date(Date.now() - 10000)
  });
  await repo.insertRecord(config, staleRecord);
  // Force status back to PROCESSING for test
  repo.records.get('stale-1').status = OutboxStatus.PROCESSING;
  repo.records.get('stale-1').updatedAt = new Date(Date.now() - 10000);

  const reaper = new OutboxReaperJob(config, repo);
  const reaped = await reaper.reapOnce();

  assert.equal(reaped, 1);
  const recovered = await repo.findById(config, 'stale-1');
  assert.equal(recovered.status, OutboxStatus.FAILED);
  assert.equal(recovered.retryCount, 1);
  assert.equal(recovered.lastError, 'PROCESSING_TIMEOUT_EXCEEDED');
});
