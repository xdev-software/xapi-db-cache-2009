package xdev.db.cache2009.jdbc;

/*-
 * #%L
 * SqlEngine Database Adapter Cache 2009
 * %%
 * Copyright (C) 2003 - 2021 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Result;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCMetaData;
import xdev.util.ProgressMonitor;


public class Cache2009JDBCMetaData extends JDBCMetaData
{
	private static final long	serialVersionUID	= 3056371675160639173L;
	
	
	public Cache2009JDBCMetaData(Cache2009JDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	public TableInfo[] getTableInfos(ProgressMonitor monitor, EnumSet<TableType> types)
			throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<TableInfo> list = new ArrayList<>();
		
		JDBCConnection jdbcConnection = (JDBCConnection)this.dataSource.openConnection();
		
		try
		{
			Result result = jdbcConnection.query("call %Library.SQLCatalog_SQLTables()");
			
			while(result.next() && !monitor.isCanceled())
			{
				String name = result.getString("RELATION_NAME");
				String table_type = result.getString("TYPE");
				
				TableType type = null;
				if(table_type.equals("TABLE"))
				{
					type = TableType.TABLE;
				}
				else if(table_type.equals("VIEW"))
				{
					type = TableType.VIEW;
				}
				else if(table_type.equals("SYSTEM TABLE"))
				{
					type = TableType.OTHER;
				}
				
				if(type != null && types.contains(type))
				{
					list.add(new TableInfo(type,null,name));
				}
			}
			
			result.close();
		}
		finally
		{
			jdbcConnection.close();
		}
		
		monitor.done();
		
		TableInfo[] tables = list.toArray(new TableInfo[list.size()]);
		Arrays.sort(tables);
		return tables;
	}
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE CACHED TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" (");
		
		ColumnMetaData[] columns = table.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			
			ColumnMetaData column = columns[i];
			appendEscapedName(column.getName(),sb);
			sb.append(" ");
			appendColumnDefinition(column,sb,params);
		}
		
		for(Index index : table.getIndices())
		{
			sb.append(", ");
			appendIndexDefinition(index,sb);
		}
		
		sb.append(")");
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD COLUMN ");
		appendEscapedName(column.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params);
		if(columnBefore == null)
		{
			sb.append(" FIRST");
		}
		else
		{
			sb.append(" AFTER ");
			appendEscapedName(columnBefore.getName(),sb);
		}
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ALTER COLUMN ");
		appendEscapedName(existing.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params);
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	

	@SuppressWarnings("incomplete-switch")
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		DataType clientType = clientColumn.getType();
		DataType dbType = dbColumn.getType();
		
		if(clientType == dbType)
		{
			switch(clientType)
			{
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case REAL:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME:
				case TIMESTAMP:
				case BOOLEAN:
				case BINARY:
				case VARBINARY:
				case LONGVARCHAR:
				case LONGVARBINARY:
				case CLOB:
				case BLOB:
				{
					return true;
				}
				
				case NUMERIC:
				case DECIMAL:
				{
					return clientColumn.getLength() == dbColumn.getLength()
							&& clientColumn.getScale() == dbColumn.getScale();
				}
				
				case CHAR:
				case VARCHAR:
				{
					return clientColumn.getLength() == dbColumn.getLength();
				}
			}
		}
		
		Boolean match = getTypeMatch(clientColumn,dbColumn);
		if(match != null)
		{
			return match;
		}
		
		match = getTypeMatch(dbColumn,clientColumn);
		if(match != null)
		{
			return match;
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private Boolean getTypeMatch(ColumnMetaData thisColumn, ColumnMetaData thatColumn)
	{
		DataType thisType = thisColumn.getType();
		DataType thatType = thatColumn.getType();
		
		switch(thisType)
		{
			case CLOB:
			{
				return thatType == DataType.LONGVARCHAR;
			}
			
			case LONGVARCHAR:
			{
				return thatType == DataType.CLOB;
			}
			
			case BLOB:
			{
				return thatType == DataType.BINARY;
			}
			
			case BINARY:
			{
				return thatType == DataType.BLOB;
			}
			
			case BOOLEAN:
			{
				return thatType == DataType.TINYINT && thatColumn.getLength() == 1;
			}
			
			case TINYINT:
			{
				return thatType == DataType.BOOLEAN && thisColumn.getLength() == 1;
			}
		}
		
		return null;
	}
	

	@SuppressWarnings("incomplete-switch")
	private void appendColumnDefinition(ColumnMetaData column, StringBuilder sb, List params)
	{
		DataType type = column.getType();
		switch(type)
		{
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case REAL:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME:
			case TIMESTAMP:
			case BOOLEAN:
			case BINARY:
			case VARBINARY:
			case LONGVARCHAR:
			case LONGVARBINARY:
			{
				sb.append(type.name());
			}
			break;
			
			case NUMERIC:
			case DECIMAL:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(",");
				sb.append(column.getScale());
				sb.append(")");
			}
			break;
			
			case CHAR:
			case VARCHAR:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(")");
			}
			break;
			
			case CLOB:
			{
				sb.append("LONGVARCHAR");
			}
			break;
			
			case BLOB:
			{
				sb.append("BINARY");
			}
			break;
		}
		
		if(column.isAutoIncrement())
		{
			sb.append(" GENERATED BY DEFAULT AS IDENTITY");
		}
		else
		{
			Object defaultValue = column.getDefaultValue();
			if(!(defaultValue == null && !column.isNullable()))
			{
				sb.append(" DEFAULT ");
				if(defaultValue == null)
				{
					sb.append("NULL");
				}
				else
				{
					sb.append("?");
					params.add(defaultValue);
				}
			}
		}
		
		if(column.isNullable())
		{
			sb.append(" NULL");
		}
		else
		{
			sb.append(" NOT NULL");
		}
	}
	
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP COLUMN ");
		appendEscapedName(column.getName(),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendIndexDefinition(index,sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private void appendIndexDefinition(Index index, StringBuilder sb) throws DBException
	{
		sb.append("CONSTRAINT ");
		appendEscapedName(getValidIndexName(index),sb);
		sb.append(" ");
		
		switch(index.getType())
		{
			case PRIMARY_KEY:
			{
				sb.append("PRIMARY KEY");
			}
			break;
			
			case UNIQUE:
			{
				sb.append("UNIQUE");
			}
			break;
			
			default:
			{
				throw new DBException(this.dataSource,
						"Only primary keys and unique indices are supported.");
			}
		}
		
		sb.append(" (");
		String[] columns = index.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			appendEscapedName(columns[i],sb);
		}
		sb.append(")");
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		
		sb.append(" DROP CONSTRAINT ");
		appendEscapedName(getValidIndexName(index),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private String getValidIndexName(Index index)
	{
		String name = index.getName();
		if(name.equals("PRIMARY_KEY"))
		{
			name = "PK";
		}
		return name;
	}
}
