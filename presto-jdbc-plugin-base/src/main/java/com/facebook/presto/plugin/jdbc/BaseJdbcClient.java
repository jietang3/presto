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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class BaseJdbcClient
        implements JdbcClient
{
    protected final String connectorId;
    protected final Driver driver;
    protected final String connectionUrl;
    protected final Properties connectionProperties;
    protected final String identifierQuote;

    public BaseJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, String identifierQuote)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.identifierQuote = checkNotNull(identifierQuote, "identifierQuote is null");

        checkNotNull(config, "config is null");
        try {
            Class<? extends Driver> driverClass = getClass().getClassLoader().loadClass(config.getDriverClass()).asSubclass(Driver.class);
            driver = driverClass.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not load driver class " + config.getDriverClass(), e);
        }

        connectionUrl = config.getConnectionUrl();

        connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
                ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                // skip the databases information_schema and sys schemas
                if (schemaName.equals("information_schema") || schemaName.equals("sys")) {
                    continue;
                }
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<String> getTableNames(String schema)
    {
        checkNotNull(schema, "schema is null");
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            if (metaData.storesUpperCaseIdentifiers()) {
                schema = schema.toUpperCase();
            }

            try (ResultSet resultSet = metaData.getTables(connection.getCatalog(), schema, null, null)) {
                ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
                while (resultSet.next()) {
                    tableNames.add(resultSet.getString(3).toLowerCase());
                }
                return tableNames.build();
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        checkNotNull(schemaTableName, "schemaTableName is null");
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metaData.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase();
                jdbcTableName = jdbcTableName.toUpperCase();
            }
            try (ResultSet resultSet = metaData.getTables(connection.getCatalog(), jdbcSchemaName, jdbcTableName, null)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(connectorId, schemaTableName, resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
                }
                if (tableHandles.isEmpty() || tableHandles.size() > 1) {
                    return null;
                }
                return Iterables.getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<ColumnMetadata> getColumns(JdbcTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getColumns(tableHandle.getCatalogName(), tableHandle.getSchemaName(), tableHandle.getTableName(), null)) {
                List<ColumnMetadata> columns = new ArrayList<>();
                int ordinalPosition = 0;
                while (resultSet.next()) {
                    ColumnType columnType = toColumnType(resultSet.getInt(5));
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString(4).toLowerCase();
                        columns.add(new ColumnMetadata(columnName, columnType, ordinalPosition, false));
                        ordinalPosition++;
                    }
                }
                if (columns.isEmpty()) {
                    return null;
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public PartitionResult getPartitions(JdbcTableHandle jdbcTableHandle, TupleDomain tupleDomain)
    {
        // currently we don't support partitions
        return new PartitionResult(ImmutableList.<Partition>of(new JdbcPartition(jdbcTableHandle, tupleDomain)), TupleDomain.all());
    }

    @Override
    public SplitSource getPartitionSplits(JdbcPartition jdbcPartition)
    {
        JdbcTableHandle jdbcTableHandle = jdbcPartition.getJdbcTableHandle();
        JdbcSplit jdbcSplit = new JdbcSplit(
                connectorId,
                jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                connectionUrl,
                Maps.fromProperties(connectionProperties),
                jdbcPartition.getTupleDomain());
        return new FixedSplitSource(connectorId, ImmutableList.of(jdbcSplit));
    }

    @Override
    public Connection getConnection(JdbcSplit split)
            throws SQLException
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : split.getConnectionProperties().entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return driver.connect(split.getConnectionUrl(), properties);
    }

    @Override
    public String buildSql(JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        return new QueryBuilder(identifierQuote).buildSql(
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain());
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public JdbcOutputTableHandle beginInsert(ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle, Collection<String> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection getConnection(JdbcOutputTableHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    protected String toTypeString(ColumnType columnType)
    {
    	throw new UnsupportedOperationException();
    }
    
    protected ColumnType toColumnType(int jdbcType)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return ColumnType.BOOLEAN;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return ColumnType.LONG;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return ColumnType.DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return ColumnType.STRING;
        }
        return null;
    }
}
