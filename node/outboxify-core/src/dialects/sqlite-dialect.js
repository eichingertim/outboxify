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

const { DatabaseDialect, DialectType } = require('../spi/database-dialect');

class SqliteDialect extends DatabaseDialect {
  getDialectType() {
    return DialectType.SQLITE;
  }

  getLockingClause() {
    return '';
  }

  getTimestampFunction() {
    return "datetime('now')";
  }

  getStaleThresholdExpression(seconds) {
    return `datetime('now', '-${Number(seconds)} seconds')`;
  }

  buildSelectBatchForUpdateQuery(tableName, columns, batchSize, maxRetries) {
    return `SELECT ${columns.id}, ${columns.topic}, ${columns.partitionKey}, ${columns.payload}, ` +
      `${columns.headers}, ${columns.status}, ${columns.retryCount}, ${columns.lastError}, ` +
      `${columns.createdAt}, ${columns.updatedAt}, ${columns.processedAt} ` +
      `FROM ${tableName} ` +
      `WHERE ${columns.status} IN ('NEW', 'FAILED') ` +
      `AND ${columns.retryCount} < ${Number(maxRetries)} ` +
      `ORDER BY ${columns.createdAt} ASC ` +
      `LIMIT ${Number(batchSize)}`;
  }

  buildReapStaleQuery(tableName, columns, timeoutSeconds, maxRetries) {
    return `UPDATE ${tableName} ` +
      `SET ${columns.status} = 'FAILED', ` +
      `${columns.retryCount} = ${columns.retryCount} + 1, ` +
      `${columns.lastError} = 'PROCESSING_TIMEOUT_EXCEEDED', ` +
      `${columns.updatedAt} = datetime('now') ` +
      `WHERE ${columns.status} = 'PROCESSING' ` +
      `AND ${columns.updatedAt} < datetime('now', '-${Number(timeoutSeconds)} seconds')`;
  }

  getSparseIndexDdl(tableName, indexName, columns) {
    return `CREATE INDEX ${indexName} ON ${tableName} (${columns.createdAt}) WHERE ${columns.status} IN ('NEW', 'FAILED')`;
  }

  supportsSkipLocked() {
    return false;
  }
}

module.exports = {
  SqliteDialect
};
