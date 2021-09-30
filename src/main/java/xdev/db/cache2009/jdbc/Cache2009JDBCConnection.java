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


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import xdev.db.DBException;
import xdev.db.jdbc.JDBCConnection;


public class Cache2009JDBCConnection extends JDBCConnection<Cache2009JDBCDataSource, Cache2009Dbms>
{
	public Cache2009JDBCConnection(Cache2009JDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	
	@Override
	public void createTable(String tableName, String primaryKey, Map<String, String> columnMap,
			boolean isAutoIncrement, Map<String, String> foreignKeys) throws Exception
	{
		
		if(!columnMap.containsKey(primaryKey))
		{
			columnMap.put(primaryKey,"INTEGER"); //$NON-NLS-1$
		}
		StringBuffer createStatement = null;
		
		if(isAutoIncrement)
		{
			createStatement = new StringBuffer("CREATE TABLE " + tableName + "(" //$NON-NLS-1$ //$NON-NLS-2$
					+ primaryKey + " " + columnMap.get(primaryKey) + " IDENTITY NOT NULL,"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		else
		{
			createStatement = new StringBuffer("CREATE TABLE " + tableName + "(" //$NON-NLS-1$ //$NON-NLS-2$
					+ primaryKey + " " + columnMap.get(primaryKey) + ","); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		int i = 1;
		for(String keySet : columnMap.keySet())
		{
			if(!keySet.equals(primaryKey))
			{
				if(i < columnMap.size())
				{
					createStatement.append(keySet + " " + columnMap.get(keySet) + ","); //$NON-NLS-1$ //$NON-NLS-2$
					
				}
				else
				{
					createStatement.append(keySet + " " + columnMap.get(keySet) + ")"); //$NON-NLS-1$ //$NON-NLS-2$
					
				}
				
			}
			i++;
		}
		
		if(log.isDebugEnabled())
		{
			log.debug("SQL Statement to create a table: " + createStatement.toString()); //$NON-NLS-1$
		}
		
		Connection connection = super.getConnection();
		Statement  statement  = connection.createStatement();
		try
		{
			statement.execute(createStatement.toString());
		}
		catch(SQLException e)
		{
			// Cache2009 does not support "IF NOT EXISTS" in create sql
			// statement
			if(!e.getMessage().contains("<Table 'SQLUser." + tableName + "' already exists>")) //$NON-NLS-1$ //$NON-NLS-2$
			{
				throw e;
			}
		}
		finally
		{
			statement.close();
			connection.close();
		}
	}
	
	
	@Override
	public Date getServerTime() throws DBException, ParseException
	{
		String selectTime = "SELECT GETDATE()"; //$NON-NLS-1$
		return super.getServerTime(selectTime);
	}
	
}
