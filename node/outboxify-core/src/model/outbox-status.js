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

/**
 * Lifecycle states of an outbox record.
 * @readonly
 * @enum {string}
 */
const OutboxStatus = Object.freeze({
  NEW: 'NEW',
  PROCESSING: 'PROCESSING',
  SENT: 'SENT',
  FAILED: 'FAILED',
  DEAD_LETTER: 'DEAD_LETTER'
});

/**
 * Checks if status is eligible for pickup by background poller.
 * @param {string} status
 * @returns {boolean}
 */
function isEligibleForProcessing(status) {
  return status === OutboxStatus.NEW || status === OutboxStatus.FAILED;
}

module.exports = {
  OutboxStatus,
  isEligibleForProcessing
};
