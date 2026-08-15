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

const crypto = require('crypto');
const { DefaultOutboxRecord } = require('../model/outbox-record');
const { OutboxResult } = require('../model/outbox-result');
const { OutboxStatus } = require('../model/outbox-status');

class OutboxPublisher {
  /**
   * @param {import('../spi/outbox-repository').OutboxRepository} repository
   * @param {import('./outbox-hook').OutboxHook} hook
   * @param {import('./outbox-dispatcher').OutboxDispatcher} dispatcher
   * @param {(name: string) => import('../model/pipeline-config').PipelineConfig} configResolver
   */
  constructor(repository, hook, dispatcher, configResolver) {
    this.repository = repository;
    this.hook = hook;
    this.dispatcher = dispatcher;
    this.configResolver = configResolver;
  }

  /**
   * Staged transactional publish:
   * 1. Inserts record in database
   * 2. Registers post-commit hook for fast-path dispatch
   */
  async publish(pipeline, payload, transactionContext = null) {
    const config = this.configResolver(pipeline);
    if (!config) {
      throw new Error(`Pipeline '${pipeline}' is not configured`);
    }

    const recordId = crypto.randomUUID();
    const record = DefaultOutboxRecord.fromPayload(pipeline, recordId, payload);

    await this.repository.insertRecord(config, record, transactionContext);

    // Register post-commit fast-path trigger
    this.hook.registerForCommit(pipeline, recordId, payload, transactionContext);

    return OutboxResult.success(recordId, payload.topic);
  }
}

module.exports = {
  OutboxPublisher
};
