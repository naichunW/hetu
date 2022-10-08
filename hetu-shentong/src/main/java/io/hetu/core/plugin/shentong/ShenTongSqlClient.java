/*
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
package io.hetu.core.plugin.shentong;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.hetu.core.plugin.shentong.optimization.ShenTongQueryGenerator;
import io.prestosql.plugin.jdbc.*;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.*;
import io.prestosql.spi.function.ExternalFunctionHub;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.type.*;

import javax.inject.Inject;
import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.hetu.core.plugin.shentong.TypeUtils.*;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.*;
import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.BASE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.DEFAULT;
import static io.prestosql.spi.StandardErrorCode.*;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Locale.ENGLISH;

public class ShenTongSqlClient
        extends BaseJdbcClient {
    private final Logger logger = Logger.get(ShenTongSqlClient.class);

    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";

    private final boolean allowModifyTable;
    private final BaseJdbcConfig config;

    private final JdbcPushDownModule pushDownModule;

    @Inject
    public ShenTongSqlClient(
            BaseJdbcConfig config,
            ShenTongSqlConfig shenTongSqlConfig,
            @StatsCollecting ConnectionFactory connectionFactory,
            ExternalFunctionHub externalFunctionHub) {
        super(config, "\"", connectionFactory, externalFunctionHub);
        this.allowModifyTable = shenTongSqlConfig.getAllowModifyTable();
        this.pushDownModule = config.getPushDownModule();
        this.config = config;

    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
        if (!this.allowModifyTable) {
            throw new PrestoException(PERMISSION_DENIED, "CREATE TABLE is disabled in this catalog");
        }

        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        } catch (SQLException e) {
            boolean exists = DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState());
            throw new PrestoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable) {
        if (!this.allowModifyTable) {
            throw new PrestoException(PERMISSION_DENIED, "CREATE TABLE is disabled in this catalog");
        }

        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported in ShenTong");
        }

        String sql = format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, schemaName, tableName),
                quoted(newTable.getTableName()));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column) {
        if (!this.allowModifyTable) {
            throw new PrestoException(PERMISSION_DENIED, "ADD COLUMNS is disabled in this catalog");
        }

        super.addColumn(session, handle, column);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName) {
        if (!this.allowModifyTable) {
            throw new PrestoException(PERMISSION_DENIED, "RENAME COLUMNS is disabled in this catalog");
        }

        super.renameColumn(identity, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column) {
        if (!this.allowModifyTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP COLUMNS is disabled in this catalog");
        }

        super.dropColumn(identity, handle, column);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
        if (!this.allowModifyTable) {
            throw new PrestoException(PERMISSION_DENIED, "CREATE TABLE is disabled in this catalog");
        }

        return super.beginCreateTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
        if (!this.allowModifyTable) {
            throw new PrestoException(PERMISSION_DENIED, "INSERT INTO TABLE is disabled in this catalog");
        }
        return super.beginInsertTable(session, tableMetadata);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException {
        //在神通mpp中KSTORE代表TABLE
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[]{"KSTORE", "VIEW"});
    }

    @Override
    public Optional<QueryGenerator<JdbcQueryGeneratorResult, JdbcConverterContext>> getQueryGenerator(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution) {
        JdbcPushDownModule gpPushDownModule = pushDownModule == DEFAULT ? BASE_PUSHDOWN : pushDownModule;
        JdbcPushDownParameter pushDownParameter = new JdbcPushDownParameter(getIdentifierQuote(), this.caseInsensitiveNameMatching, gpPushDownModule, functionResolution);
        return Optional.of(new ShenTongQueryGenerator(determinismEvaluator, rowExpressionService, functionManager, functionResolution, pushDownParameter, config));
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        Optional<ColumnMapping> columnMapping;

        String jdbcTypeName = "";
        if (typeHandle.getJdbcTypeName().isPresent()) {
            jdbcTypeName = typeHandle.getJdbcTypeName().get().toUpperCase(ENGLISH);
        }
        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
                if (jdbcTypeName.equals("UINT4")) {
                    columnMapping = Optional.of(bigintColumnMapping());
                } else {
                    columnMapping = Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                break;
            default:
                columnMapping = super.toPrestoType(session, connection, typeHandle);
        }
        if (!columnMapping.isPresent()) {
            logger.warn(
                    "openLooKeng does not support the shentong type " + typeHandle.getJdbcTypeName().orElse("")
                            + '(' + typeHandle.getColumnSize() + ", " + typeHandle.getDecimalDigits()
                            + ')');
        }
        return columnMapping;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle) {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                //神通map的DATA_TYPE，TYPE_NAME 非SQL标准，需要进程转换，参考oscar jdbc中的OscarResultSetMetaData
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, connection, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType(), nullable));
                    } else {
                        logger.warn("ShenTong JdbcColumnHandle skip unsupported column types: " + typeHandle);
                    }
                }
                if (columns.isEmpty()) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Hetu's query push down requires to get output columns of the given sql query.
     * The returned list of columns does not necessarily match with the underlying table schema.
     * It interprets all the selected values as a separate column.
     * For example `SELECT CAST(MAX(price) AS varchar) as max_price FORM orders GROUP BY customer`
     * query will return a map with a single {@link ColumnHandle} for a VARCHAR max_price column.
     *
     * @param session the connector session for the JdbcClient
     * @param sql     the sub-query to process
     * @param types   Hetu types of intermediate symbols
     * @return a Map of output symbols treated as columns along with their {@link ColumnHandle}s
     */
    @Override
    public Map<String, ColumnHandle> getColumns(ConnectorSession session, String sql, Map<String, Type> types) {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
             PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSetMetaData metadata = statement.getMetaData();
            ImmutableMap.Builder<String, ColumnHandle> builder = new ImmutableMap.Builder<>();

            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                String columnName = metadata.getColumnLabel(i);
                String typeName = metadata.getColumnTypeName(i);
                int precision = metadata.getPrecision(i);
                int dataType = metadata.getColumnType(i);
                int scale = metadata.getScale(i);
                // need to deal with different data type here

                boolean isNullAble = metadata.isNullable(i) != ResultSetMetaData.columnNoNulls;

                JdbcTypeHandle typeHandle = new JdbcTypeHandle(dataType, Optional.ofNullable(typeName), precision, scale, Optional.empty());
                Optional<ColumnMapping> columnMapping;
                try {
                    columnMapping = toPrestoType(session, connection, typeHandle);
                } catch (UnsupportedOperationException ex) {
                    // User configured to fail the query if the data type is not supported
                    return Collections.emptyMap();
                }
                // skip unsupported column types
                if (columnMapping.isPresent()) {
                    Type type = columnMapping.get().getType();
                    JdbcColumnHandle handle = new JdbcColumnHandle(columnName, typeHandle, type, isNullAble);
                    builder.put(columnName.toLowerCase(ENGLISH), handle);
                } else {
                    logger.warn("ShenTong ColumnHandle skip unsupported column types: " + typeHandle);
                    return Collections.emptyMap();
                }
            }

            return builder.build();
        } catch (SQLException | PrestoException e) {
            // No need to raise an error.
            // This method is used inside applySubQuery method to extract the column types from a sub-query.
            // Returning empty map will indicate that something wrong and let the Hetu to execute the query as usual.
            logger.warn("in shentong push down, shentong data source error msg[%s] rewrite sql[%s]", e.getMessage(), sql);
            return Collections.emptyMap();
        }
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction() {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed() {
        return true;
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException {
        if (table.getGeneratedSql().isPresent()) {
            // Hetu: If the query is pushed down, use it as the table
            return new ShenTongQueryBuilder(identifierQuote, true).buildSql(
                    this,
                    session,
                    connection,
                    null,
                    null,
                    table.getGeneratedSql().get().getSql(),
                    columns,
                    table.getConstraint(),
                    split.getAdditionalPredicate(),
                    tryApplyLimit(table.getLimit()));
        }
        return new ShenTongQueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                table.getCatalogName(),
                table.getSchemaName(),
                table.getTableName(),
                columns,
                table.getConstraint(),
                split.getAdditionalPredicate(),
                tryApplyLimit(table.getLimit()));
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName) {
        //神通查询语句中只能通过 schema.table查询，不能指定catalog.schema.table三层查询，所有catatlog永远置为null
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            schemaTableName,
                            null,
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return Optional.of(getOnlyElement(tableHandles));
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }


}
