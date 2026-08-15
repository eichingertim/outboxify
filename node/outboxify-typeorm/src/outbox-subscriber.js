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

const { OutboxPayload } = require('../../outboxify-core/src/model/outbox-payload');

/**
 * TypeORM Entity Subscriber that intercepts entities implementing the Outbox contract
 * and attaches post-commit fast-path triggers to the active QueryRunner transaction.
 */
class OutboxEntitySubscriber {
  /**
   * @param {import('../../outboxify-core/src/engine/outbox-hook').OutboxHook} hook
   */
  constructor(hook) {
    this.hook = hook;
  }

  afterInsert(event) {
    this.handleOutboxEvent(event);
  }

  afterUpdate(event) {
    this.handleOutboxEvent(event);
  }

  handleOutboxEvent(event) {
    const entity = event.entity;
    if (!entity) return;

    // Check if entity conforms to outbox contract
    const isOutboxEntity = typeof entity.getOutboxId === 'function' || entity.outboxId !== undefined;
    if (!isOutboxEntity) return;

    const pipeline = typeof entity.getOutboxPipeline === 'function' ? entity.getOutboxPipeline() : (entity.outboxPipeline || 'default');
    const id = typeof entity.getOutboxId === 'function' ? entity.getOutboxId() : entity.outboxId;
    const topic = typeof entity.getTopic === 'function' ? entity.getTopic() : entity.topic;
    const partitionKey = typeof entity.getPartitionKey === 'function' ? entity.getPartitionKey() : entity.partitionKey;
    const payloadData = typeof entity.getPayload === 'function' ? entity.getPayload() : entity.payload;
    const headers = typeof entity.getHeaders === 'function' ? entity.getHeaders() : (entity.headers || {});

    if (!id || !topic || payloadData === undefined) return;

    const payload = OutboxPayload.of(topic, payloadData, partitionKey, headers);
    const queryRunner = event.queryRunner;

    if (queryRunner && queryRunner.isTransactionActive) {
      if (!queryRunner.data) {
        queryRunner.data = {};
      }
      if (!queryRunner.data._outboxify_commit_hooks) {
        queryRunner.data._outboxify_commit_hooks = [];
        
        // Wrap queryRunner.commitTransaction
        const originalCommit = queryRunner.commitTransaction.bind(queryRunner);
        queryRunner.commitTransaction = async () => {
          await originalCommit();
          const hooks = queryRunner.data._outboxify_commit_hooks || [];
          for (const cb of hooks) {
            try {
              cb();
            } catch (e) {
              // ignore callback error
            }
          }
        };
      }

      queryRunner.data._outboxify_commit_hooks.push(() => {
        this.hook.registerForCommit(pipeline, id, payload);
      });
    } else {
      this.hook.registerForCommit(pipeline, id, payload);
    }
  }
}

module.exports = {
  OutboxEntitySubscriber
};
