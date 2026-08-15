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

package io.outboxify.spring.datasource;

import io.outboxify.core.spi.ConnectionProvider;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Spring-aware {@link ConnectionProvider} that integrates with Spring's {@link DataSourceUtils}
 * to bind database operations to active Spring {@code @Transactional} contexts.
 */
public class SpringConnectionProvider implements ConnectionProvider {

    private final DataSource dataSource;

    public SpringConnectionProvider(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return DataSourceUtils.getConnection(dataSource);
    }

    @Override
    public void releaseConnection(Connection connection) throws SQLException {
        DataSourceUtils.releaseConnection(connection, dataSource);
    }
}
