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


import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


public class MysqlJdbcClient
        extends BaseJdbcClient
{
    @Inject
    public MysqlJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "`");
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
                ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                // skip the internal schemas
                if (schemaName.equals("information_schema") || schemaName.equals("mysql")) {
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
            try (ResultSet resultSet = metaData.getTables(schema, null, null, null)) {
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
            try (ResultSet resultSet = metaData.getTables(jdbcSchemaName, null, jdbcTableName, null)) {
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
    public JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata)
    {
    	//throw new UnsupportedOperationException();
        // checkArgument(!isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<ColumnType> columnTypes = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
        
        // get the root directory for the database
        SchemaTableName table = tableMetadata.getTable();
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();
        
        checkState(getTableHandle(table) == null, String.format("%s.%s already exists!", schemaName, tableName));
        
        JdbcOutputTableHandle handle = new JdbcOutputTableHandle(
        		connectorId,
        		schemaName,
        		null,
        		tableName,
        		columnNames.build(),
        		columnTypes.build(),
        		tableMetadata.getOwner(),
        		"",
        		"",
        		new HashMap<String,String>());
        
        
        Connection conn = null;
    	Statement stmt = null;
    	
    	try {
			conn = this.getConnection(handle);
			checkNotNull(conn, "conn is null!");
			stmt = conn.createStatement();
			
			StringBuilder sqlBuilder = new StringBuilder();
			
			sqlBuilder.append(String.format("create table %s.%s (", handle.getCatalogName(), handle.getTableName()));
			
			List<String> columnNameList = handle.getColumnNames();
			List<ColumnType> columnTypeList = handle.getColumnTypes();
			for (int i = 0; i < columnNameList.size(); i++)
			{
				sqlBuilder.append(String.format("%s %s,", columnNameList.get(i), toTypeString(columnTypeList.get(i))));
			}
			
			sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
			sqlBuilder.append(")");
			
			stmt.executeUpdate(sqlBuilder.toString());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	finally
    	{
    		try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	return handle;
    }
    
//    @Override
//    public void commitCreateTable(JdbcOutputTableHandle handle, Collection<String> fragments)
//    {
//    	Connection conn = null;
//    	Statement stmt = null;
//    	
//    	try {
//			conn = this.getConnection(handle);
//			checkNotNull(conn, "conn is null!");
//			stmt = conn.createStatement();
//			
//			StringBuilder sqlBuilder = new StringBuilder();
//			
//			sqlBuilder.append(String.format("create table %s.%s (", handle.getCatalogName(), handle.getTableName()));
//			
//			List<String> columnNames = handle.getColumnNames();
//			List<ColumnType> columnTypes = handle.getColumnTypes();
//			for (int i = 0; i < columnNames.size(); i++)
//			{
//				sqlBuilder.append(String.format("%s %s,", columnNames.get(i), toTypeString(columnTypes.get(i))));
//			}
//			
//			sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
//			sqlBuilder.append(")");
//			
//			stmt.executeUpdate(sqlBuilder.toString());
//			
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	finally
//    	{
//    		try {
//				if (conn != null)
//					conn.close();
//			} catch (SQLException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    		
//    		try {
//				if (stmt != null)
//					stmt.close();
//			} catch (SQLException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    	}
//    }
    
    @Override
    public Connection getConnection(JdbcOutputTableHandle handle)
    {
    		try {
				return driver.connect(connectionUrl, connectionProperties);
			} catch (SQLException e) {
				
				e.printStackTrace();
				return null;
			}
    }
    
    @Override
    protected String toTypeString(ColumnType columnType)
    {
    	switch (columnType)
    	{
    	case LONG:
    		return "bigint";
    	case BOOLEAN:
    		return "boolean";
    	case DOUBLE:
    		return "double";
    	case STRING:
    		return "varchar(1024)";
    	default:
    		throw new IllegalArgumentException("columnType");
    	}
    }
    
    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("insert into %s.%s values(", handle.getCatalogName(), handle.getTableName()));
        
        for (int i = 0; i < handle.getColumnNames().size(); i++)
		{
			sqlBuilder.append("?,");
		}
        
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
		sqlBuilder.append(")");
		
		return sqlBuilder.toString();
    }
    
    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle, Collection<String> fragments)
    {
    }
    
    @Override
    public JdbcOutputTableHandle beginInsert(ConnectorTableMetadata tableMetadata)
    {
    	ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<ColumnType> columnTypes = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
        
        // get the root directory for the database
        SchemaTableName table = tableMetadata.getTable();
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();
        
        checkState(getTableHandle(table) != null, String.format("%s.%s does not exist!", schemaName, tableName));
        
        JdbcOutputTableHandle handle = new JdbcOutputTableHandle(
        		connectorId,
        		schemaName,
        		null,
        		tableName,
        		columnNames.build(),
        		columnTypes.build(),
        		tableMetadata.getOwner(),
        		"",
        		"",
        		new HashMap<String,String>());
        
        return handle;
    }
}
