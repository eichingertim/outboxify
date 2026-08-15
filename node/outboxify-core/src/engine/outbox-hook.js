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

class OutboxHook {
  /**
   * @param {import('./outbox-dispatcher').OutboxDispatcher} dispatcher
   * @param {(name: string) => import('../model/pipeline-config').PipelineConfig} configResolver
   */
  constructor(dispatcher, configResolver) {
    this.dispatcher = dispatcher;
    this.configResolver = configResolver;
  }

  /**
   * Registers a post-commit callback on a transaction or dispatches immediately.
   * @param {string} pipeline
   * @param {string} recordId
   * @param {import('../model/outbox-payload').OutboxPayload} payload
   * @param {Object} [transactionContext] Optional transaction context with commit hooks
   */
  registerForCommit(pipeline, recordId, payload, transactionContext = null) {
    const config = this.configResolver(pipeline);
    if (!config || !config.immediateSendEnabled) {
      return;
    }

    if (transactionContext && typeof transactionContext.onCommit === 'function') {
      transactionContext.onCommit(() => {
        this.dispatcher.dispatchFastPath(config, recordId, payload).catch(() => {});
      });
    } else {
      // Direct dispatch
      setImmediate(() => {
        this.dispatcher.dispatchFastPath(config, recordId, payload).catch(() => {});
      });
    }
  }
}

module.exports = {
  OutboxHook
};
