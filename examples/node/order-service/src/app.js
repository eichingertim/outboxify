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

const express = require('express');

function createApp({ orderDb, brokerPublisher }) {
  const app = express();
  app.use(express.json());

  // 1. Create Order (Success flow with transactional outbox)
  app.post('/api/orders', async (req, res) => {
    try {
      const { customerId, item, amount } = req.body;
      if (!customerId || !item || amount === undefined) {
        return res.status(400).json({ error: 'customerId, item, and amount are required' });
      }

      const result = await orderDb.createOrder({ customerId, item, amount });
      return res.status(201).json(result);
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  });

  // 2. Simulate Failure (Demonstrates rollback safety)
  app.post('/api/orders/simulate-failure', async (req, res) => {
    try {
      const { customerId, item, amount } = req.body;
      await orderDb.createOrderWithFailure({ customerId, item, amount });
      return res.json({ message: 'Unexpected success' });
    } catch (err) {
      return res.status(500).json({
        status: 'ROLLED_BACK',
        error: err.message,
        message: 'Transaction rolled back. Outboxify guarantees NO orphaned message was published to Kafka!'
      });
    }
  });

  // 3. List Confirmed Orders
  app.get('/api/orders', (req, res) => {
    return res.json(orderDb.getAllOrders());
  });

  // 4. List Outbox Table Rows
  app.get('/api/outbox', async (req, res) => {
    const records = await orderDb.getOutboxRecords();
    return res.json(records);
  });

  // 5. List Messages Delivered to Event Broker
  app.get('/api/broker/messages', (req, res) => {
    if (typeof brokerPublisher.getDeliveredMessages === 'function') {
      return res.json(brokerPublisher.getDeliveredMessages());
    }
    return res.json({ message: 'Live Kafka publisher active' });
  });

  return app;
}

module.exports = {
  createApp
};
