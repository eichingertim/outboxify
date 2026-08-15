# Copyright 2026 Outboxify Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict
from .models import ColumnMapping


class DialectType(str, Enum):
    AUTO_DETECT = "AUTO_DETECT"
    ORACLE = "ORACLE"
    POSTGRESQL = "POSTGRESQL"
    MYSQL = "MYSQL"
    MSSQL = "MSSQL"
    SQLITE = "SQLITE"


class DatabaseDialect(ABC):
    @abstractmethod
    def get_dialect_type(self) -> DialectType:
        pass

    def get_locking_clause(self) -> str:
        return "FOR UPDATE SKIP LOCKED"

    def get_timestamp_function(self) -> str:
        return "CURRENT_TIMESTAMP"

    @abstractmethod
    def build_select_batch_for_update_query(self, table_name: str, cols: ColumnMapping, batch_size: int, max_retries: int) -> str:
        pass

    @abstractmethod
    def build_reap_stale_query(self, table_name: str, cols: ColumnMapping, timeout_seconds: int, max_retries: int) -> str:
        pass

    @abstractmethod
    def get_sparse_index_ddl(self, table_name: str, index_name: str, cols: ColumnMapping) -> str:
        pass


class OracleDialect(DatabaseDialect):
    def get_dialect_type(self) -> DialectType:
        return DialectType.ORACLE

    def get_locking_clause(self) -> str:
        return "FOR UPDATE SKIP LOCKED"

    def get_timestamp_function(self) -> str:
        return "SYSTIMESTAMP"

    def build_select_batch_for_update_query(self, table_name: str, cols: ColumnMapping, batch_size: int, max_retries: int) -> str:
        return (
            f"SELECT {cols.id}, {cols.topic}, {cols.partition_key}, {cols.payload}, "
            f"{cols.headers}, {cols.status}, {cols.retry_count}, {cols.last_error}, "
            f"{cols.created_at}, {cols.updated_at}, {cols.processed_at} "
            f"FROM {table_name} "
            f"WHERE {cols.status} IN ('NEW', 'FAILED') "
            f"AND {cols.retry_count} < {max_retries} "
            f"ORDER BY {cols.created_at} ASC "
            f"FETCH FIRST {batch_size} ROWS ONLY "
            f"FOR UPDATE SKIP LOCKED"
        )

    def build_reap_stale_query(self, table_name: str, cols: ColumnMapping, timeout_seconds: int, max_retries: int) -> str:
        return (
            f"UPDATE {table_name} "
            f"SET {cols.status} = 'FAILED', "
            f"{cols.retry_count} = {cols.retry_count} + 1, "
            f"{cols.last_error} = 'PROCESSING_TIMEOUT_EXCEEDED', "
            f"{cols.updated_at} = SYSTIMESTAMP "
            f"WHERE {cols.status} = 'PROCESSING' "
            f"AND {cols.updated_at} < (SYSTIMESTAMP - NUMTODSINTERVAL({timeout_seconds}, 'SECOND'))"
        )

    def get_sparse_index_ddl(self, table_name: str, index_name: str, cols: ColumnMapping) -> str:
        return (
            f"CREATE INDEX {index_name} ON {table_name} ("
            f"CASE WHEN {cols.status} IN ('NEW', 'FAILED') THEN {cols.status} ELSE NULL END, "
            f"{cols.created_at})"
        )


class PostgresDialect(DatabaseDialect):
    def get_dialect_type(self) -> DialectType:
        return DialectType.POSTGRESQL

    def get_locking_clause(self) -> str:
        return "FOR UPDATE SKIP LOCKED"

    def get_timestamp_function(self) -> str:
        return "CURRENT_TIMESTAMP"

    def build_select_batch_for_update_query(self, table_name: str, cols: ColumnMapping, batch_size: int, max_retries: int) -> str:
        return (
            f"SELECT {cols.id}, {cols.topic}, {cols.partition_key}, {cols.payload}, "
            f"{cols.headers}, {cols.status}, {cols.retry_count}, {cols.last_error}, "
            f"{cols.created_at}, {cols.updated_at}, {cols.processed_at} "
            f"FROM {table_name} "
            f"WHERE {cols.status} IN ('NEW', 'FAILED') "
            f"AND {cols.retry_count} < {max_retries} "
            f"ORDER BY {cols.created_at} ASC "
            f"LIMIT {batch_size} "
            f"FOR UPDATE SKIP LOCKED"
        )

    def build_reap_stale_query(self, table_name: str, cols: ColumnMapping, timeout_seconds: int, max_retries: int) -> str:
        return (
            f"UPDATE {table_name} "
            f"SET {cols.status} = 'FAILED', "
            f"{cols.retry_count} = {cols.retry_count} + 1, "
            f"{cols.last_error} = 'PROCESSING_TIMEOUT_EXCEEDED', "
            f"{cols.updated_at} = CURRENT_TIMESTAMP "
            f"WHERE {cols.status} = 'PROCESSING' "
            f"AND {cols.updated_at} < (CURRENT_TIMESTAMP - ({timeout_seconds} * INTERVAL '1 SECOND'))"
        )

    def get_sparse_index_ddl(self, table_name: str, index_name: str, cols: ColumnMapping) -> str:
        return (
            f"CREATE INDEX {index_name} ON {table_name} ({cols.created_at}) "
            f"WHERE {cols.status} IN ('NEW', 'FAILED')"
        )


class MySqlDialect(DatabaseDialect):
    def get_dialect_type(self) -> DialectType:
        return DialectType.MYSQL

    def get_locking_clause(self) -> str:
        return "FOR UPDATE SKIP LOCKED"

    def get_timestamp_function(self) -> str:
        return "NOW(6)"

    def build_select_batch_for_update_query(self, table_name: str, cols: ColumnMapping, batch_size: int, max_retries: int) -> str:
        return (
            f"SELECT {cols.id}, {cols.topic}, {cols.partition_key}, {cols.payload}, "
            f"{cols.headers}, {cols.status}, {cols.retry_count}, {cols.last_error}, "
            f"{cols.created_at}, {cols.updated_at}, {cols.processed_at} "
            f"FROM {table_name} "
            f"WHERE {cols.status} IN ('NEW', 'FAILED') "
            f"AND {cols.retry_count} < {max_retries} "
            f"ORDER BY {cols.created_at} ASC "
            f"LIMIT {batch_size} "
            f"FOR UPDATE SKIP LOCKED"
        )

    def build_reap_stale_query(self, table_name: str, cols: ColumnMapping, timeout_seconds: int, max_retries: int) -> str:
        return (
            f"UPDATE {table_name} "
            f"SET {cols.status} = 'FAILED', "
            f"{cols.retry_count} = {cols.retry_count} + 1, "
            f"{cols.last_error} = 'PROCESSING_TIMEOUT_EXCEEDED', "
            f"{cols.updated_at} = NOW(6) "
            f"WHERE {cols.status} = 'PROCESSING' "
            f"AND {cols.updated_at} < DATE_SUB(NOW(6), INTERVAL {timeout_seconds} SECOND)"
        )

    def get_sparse_index_ddl(self, table_name: str, index_name: str, cols: ColumnMapping) -> str:
        return f"CREATE INDEX {index_name} ON {table_name} ({cols.status}, {cols.created_at})"


class SqlServerDialect(DatabaseDialect):
    def get_dialect_type(self) -> DialectType:
        return DialectType.MSSQL

    def get_locking_clause(self) -> str:
        return "WITH (UPDLOCK, READPAST, ROWLOCK)"

    def get_timestamp_function(self) -> str:
        return "SYSUTCDATETIME()"

    def build_select_batch_for_update_query(self, table_name: str, cols: ColumnMapping, batch_size: int, max_retries: int) -> str:
        return (
            f"SELECT TOP ({batch_size}) "
            f"{cols.id}, {cols.topic}, {cols.partition_key}, {cols.payload}, "
            f"{cols.headers}, {cols.status}, {cols.retry_count}, {cols.last_error}, "
            f"{cols.created_at}, {cols.updated_at}, {cols.processed_at} "
            f"FROM {table_name} WITH (UPDLOCK, READPAST, ROWLOCK) "
            f"WHERE {cols.status} IN ('NEW', 'FAILED') "
            f"AND {cols.retry_count} < {max_retries} "
            f"ORDER BY {cols.created_at} ASC"
        )

    def build_reap_stale_query(self, table_name: str, cols: ColumnMapping, timeout_seconds: int, max_retries: int) -> str:
        return (
            f"UPDATE {table_name} "
            f"SET {cols.status} = 'FAILED', "
            f"{cols.retry_count} = {cols.retry_count} + 1, "
            f"{cols.last_error} = 'PROCESSING_TIMEOUT_EXCEEDED', "
            f"{cols.updated_at} = SYSUTCDATETIME() "
            f"WHERE {cols.status} = 'PROCESSING' "
            f"AND {cols.updated_at} < DATEADD(second, -{timeout_seconds}, SYSUTCDATETIME())"
        )

    def get_sparse_index_ddl(self, table_name: str, index_name: str, cols: ColumnMapping) -> str:
        return (
            f"CREATE NONCLUSTERED INDEX {index_name} ON {table_name} ({cols.created_at}) "
            f"WHERE {cols.status} IN ('NEW', 'FAILED')"
        )


class SqliteDialect(DatabaseDialect):
    def get_dialect_type(self) -> DialectType:
        return DialectType.SQLITE

    def get_locking_clause(self) -> str:
        return ""

    def get_timestamp_function(self) -> str:
        return "datetime('now')"

    def build_select_batch_for_update_query(self, table_name: str, cols: ColumnMapping, batch_size: int, max_retries: int) -> str:
        return (
            f"SELECT {cols.id}, {cols.topic}, {cols.partition_key}, {cols.payload}, "
            f"{cols.headers}, {cols.status}, {cols.retry_count}, {cols.last_error}, "
            f"{cols.created_at}, {cols.updated_at}, {cols.processed_at} "
            f"FROM {table_name} "
            f"WHERE {cols.status} IN ('NEW', 'FAILED') "
            f"AND {cols.retry_count} < {max_retries} "
            f"ORDER BY {cols.created_at} ASC "
            f"LIMIT {batch_size}"
        )

    def build_reap_stale_query(self, table_name: str, cols: ColumnMapping, timeout_seconds: int, max_retries: int) -> str:
        return (
            f"UPDATE {table_name} "
            f"SET {cols.status} = 'FAILED', "
            f"{cols.retry_count} = {cols.retry_count} + 1, "
            f"{cols.last_error} = 'PROCESSING_TIMEOUT_EXCEEDED', "
            f"{cols.updated_at} = datetime('now') "
            f"WHERE {cols.status} = 'PROCESSING' "
            f"AND {cols.updated_at} < datetime('now', '-{timeout_seconds} seconds')"
        )

    def get_sparse_index_ddl(self, table_name: str, index_name: str, cols: ColumnMapping) -> str:
        return f"CREATE INDEX {index_name} ON {table_name} ({cols.created_at}) WHERE {cols.status} IN ('NEW', 'FAILED')"


class DialectRegistry:
    def __init__(self):
        self._dialects: Dict[DialectType, DatabaseDialect] = {
            DialectType.ORACLE: OracleDialect(),
            DialectType.POSTGRESQL: PostgresDialect(),
            DialectType.MYSQL: MySqlDialect(),
            DialectType.MSSQL: SqlServerDialect(),
            DialectType.SQLITE: SqliteDialect(),
        }

    def register(self, dialect: DatabaseDialect) -> None:
        self._dialects[dialect.get_dialect_type()] = dialect

    def get_dialect(self, dialect_type: DialectType) -> DatabaseDialect:
        if dialect_type == DialectType.AUTO_DETECT or not dialect_type:
            return self._dialects[DialectType.POSTGRESQL]
        
        dtype = DialectType(dialect_type.upper()) if isinstance(dialect_type, str) else dialect_type
        if dtype in self._dialects:
            return self._dialects[dtype]
        raise ValueError(f"Unsupported database dialect: {dialect_type}")
