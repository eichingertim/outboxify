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

const { OutboxStatus, isEligibleForProcessing } = require('./model/outbox-status');
const { OutboxPayload } = require('./model/outbox-payload');
const { DefaultOutboxRecord } = require('./model/outbox-record');
const { OutboxResult } = require('./model/outbox-result');
const { PipelineConfig, ColumnMapping } = require('./model/pipeline-config');
const { DatabaseDialect, DialectType } = require('./spi/database-dialect');
const { OracleDialect } = require('./dialects/oracle-dialect');
const { PostgresDialect } = require('./dialects/postgres-dialect');
const { MySqlDialect } = require('./dialects/mysql-dialect');
const { SqlServerDialect } = require('./dialects/sqlserver-dialect');
const { SqliteDialect } = require('./dialects/sqlite-dialect');
const { DialectRegistry } = require('./dialects/dialect-registry');
const { BrokerPublisher } = require('./spi/broker-publisher');
const { MockBrokerPublisher, KafkaBrokerPublisher } = require('./broker/kafka-broker-publisher');
const { OutboxRepository } = require('./spi/outbox-repository');
const { InMemoryOutboxRepository } = require('./repository/in-memory-outbox-repository');
const { OutboxDispatcher } = require('./engine/outbox-dispatcher');
const { OutboxPollerEngine } = require('./engine/outbox-poller-engine');
const { OutboxReaperJob } = require('./engine/outbox-reaper-job');
const { OutboxHook } = require('./engine/outbox-hook');
const { OutboxPublisher } = require('./engine/outbox-publisher');

module.exports = {
  OutboxStatus,
  isEligibleForProcessing,
  OutboxPayload,
  DefaultOutboxRecord,
  OutboxResult,
  PipelineConfig,
  ColumnMapping,
  DatabaseDialect,
  DialectType,
  OracleDialect,
  PostgresDialect,
  MySqlDialect,
  SqlServerDialect,
  SqliteDialect,
  DialectRegistry,
  BrokerPublisher,
  MockBrokerPublisher,
  KafkaBrokerPublisher,
  OutboxRepository,
  InMemoryOutboxRepository,
  OutboxDispatcher,
  OutboxPollerEngine,
  OutboxReaperJob,
  OutboxHook,
  OutboxPublisher
};
