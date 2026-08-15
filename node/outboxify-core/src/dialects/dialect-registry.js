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

const { DialectType } = require('../spi/database-dialect');
const { OracleDialect } = require('./oracle-dialect');
const { PostgresDialect } = require('./postgres-dialect');
const { MySqlDialect } = require('./mysql-dialect');
const { SqlServerDialect } = require('./sqlserver-dialect');
const { SqliteDialect } = require('./sqlite-dialect');

class DialectRegistry {
  constructor() {
    this.dialects = new Map();
    this.register(new OracleDialect());
    this.register(new PostgresDialect());
    this.register(new MySqlDialect());
    this.register(new SqlServerDialect());
    this.register(new SqliteDialect());
  }

  register(dialect) {
    this.dialects.set(dialect.getDialectType(), dialect);
  }

  getDialect(type) {
    if (!type || type === DialectType.AUTO_DETECT) {
      return this.dialects.get(DialectType.POSTGRESQL);
    }
    const dialect = this.dialects.get(type.toUpperCase());
    if (!dialect) {
      throw new Error(`Unsupported dialect type: ${type}`);
    }
    return dialect;
  }
}

module.exports = {
  DialectRegistry
};
