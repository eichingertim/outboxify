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

package io.outboxify.dialects;

import io.outboxify.core.spi.DatabaseDialect;
import io.outboxify.core.spi.DialectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.Map;

/**
 * Central registry and auto-detection resolver for {@link DatabaseDialect} implementations.
 */
public class DialectRegistry {

    private static final Logger log = LoggerFactory.getLogger(DialectRegistry.class);

    private final Map<DialectType, DatabaseDialect> dialects = new EnumMap<>(DialectType.class);

    public DialectRegistry() {
        register(new OracleDialect());
        register(new PostgresDialect());
        register(new MySqlDialect());
        register(new SqlServerDialect());
        register(new H2Dialect());
    }

    public void register(DatabaseDialect dialect) {
        dialects.put(dialect.getDialectType(), dialect);
    }

    public DatabaseDialect getDialect(DialectType type) {
        if (type == null || type == DialectType.AUTO_DETECT) {
            return dialects.get(DialectType.POSTGRESQL);
        }
        DatabaseDialect dialect = dialects.get(type);
        if (dialect == null) {
            throw new IllegalArgumentException("Unsupported dialect type: " + type);
        }
        return dialect;
    }

    /**
     * Resolves dialect for a given pipeline configuration and DataSource.
     *
     * @param configuredType explicitly configured dialect or AUTO_DETECT
     * @param dataSource database DataSource
     * @return resolved DatabaseDialect
     */
    public DatabaseDialect resolveDialect(DialectType configuredType, DataSource dataSource) {
        if (configuredType != null && configuredType != DialectType.AUTO_DETECT) {
            return getDialect(configuredType);
        }

        if (dataSource == null) {
            log.warn("DataSource is null, falling back to PostgreSQL dialect");
            return getDialect(DialectType.POSTGRESQL);
        }

        try (Connection connection = dataSource.getConnection()) {
            return autoDetectFromConnection(connection);
        } catch (SQLException e) {
            log.warn("Failed to inspect DatabaseMetaData for dialect auto-detection, defaulting to PostgreSQL: {}", e.getMessage());
            return getDialect(DialectType.POSTGRESQL);
        }
    }

    /**
     * Auto-detects dialect by inspecting JDBC connection metadata.
     *
     * @param connection active JDBC connection
     * @return detected DatabaseDialect
     * @throws SQLException on metadata read failure
     */
    public DatabaseDialect autoDetectFromConnection(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String productName = metaData.getDatabaseProductName().toLowerCase();

        if (productName.contains("oracle")) {
            log.info("Auto-detected Oracle database dialect ({})", productName);
            return getDialect(DialectType.ORACLE);
        } else if (productName.contains("postgres")) {
            log.info("Auto-detected PostgreSQL database dialect ({})", productName);
            return getDialect(DialectType.POSTGRESQL);
        } else if (productName.contains("mysql") || productName.contains("mariadb")) {
            log.info("Auto-detected MySQL/MariaDB database dialect ({})", productName);
            return getDialect(DialectType.MYSQL);
        } else if (productName.contains("microsoft") || productName.contains("sql server")) {
            log.info("Auto-detected Microsoft SQL Server database dialect ({})", productName);
            return getDialect(DialectType.MSSQL);
        } else if (productName.contains("h2")) {
            log.info("Auto-detected H2 database dialect ({})", productName);
            return getDialect(DialectType.H2);
        }

        log.warn("Unrecognized database product '{}', defaulting to PostgreSQL dialect", productName);
        return getDialect(DialectType.POSTGRESQL);
    }
}
