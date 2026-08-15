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

const DialectType = Object.freeze({
  AUTO_DETECT: 'AUTO_DETECT',
  ORACLE: 'ORACLE',
  POSTGRESQL: 'POSTGRESQL',
  MYSQL: 'MYSQL',
  MSSQL: 'MSSQL',
  SQLITE: 'SQLITE'
});

/**
 * Base database dialect contract for Node.js.
 */
class DatabaseDialect {
  getDialectType() { throw new Error('Not implemented'); }
  getLockingClause() { return 'FOR UPDATE SKIP LOCKED'; }
  getTimestampFunction() { return 'CURRENT_TIMESTAMP'; }
  getStaleThresholdExpression(seconds) { throw new Error('Not implemented'); }
  buildSelectBatchForUpdateQuery(tableName, columns, batchSize, maxRetries) { throw new Error('Not implemented'); }
  buildReapStaleQuery(tableName, columns, timeoutSeconds, maxRetries) { throw new Error('Not implemented'); }
  getSparseIndexDdl(tableName, indexName, columns) { throw new Error('Not implemented'); }
  supportsSkipLocked() { return true; }
}

module.exports = {
  DialectType,
  DatabaseDialect
};
