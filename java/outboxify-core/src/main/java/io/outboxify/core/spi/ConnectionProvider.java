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

package io.outboxify.core.spi;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * SPI for acquiring and releasing JDBC Connections.
 * Allows framework bridges (e.g. Spring TransactionSynchronizationManager / DataSourceUtils)
 * to participate seamlessly in surrounding application transactions.
 */
@FunctionalInterface
public interface ConnectionProvider {

    /**
     * Obtains an active JDBC Connection.
     *
     * @return active connection
     * @throws SQLException on error
     */
    Connection getConnection() throws SQLException;

    /**
     * Releases or closes the JDBC Connection when an operation concludes.
     *
     * @param connection the connection to release
     * @throws SQLException on error
     */
    default void releaseConnection(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
