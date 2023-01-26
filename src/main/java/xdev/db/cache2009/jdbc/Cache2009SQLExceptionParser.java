/*
 * SqlEngine Database Adapter Cache 2009 - XAPI SqlEngine Database Adapter for Cache 2009
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.cache2009.jdbc;

import java.sql.SQLException;

import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineTableNotFoundException;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineUniqueConstraintInsertViolationException;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineUniqueConstraintUpdateViolationException;



/**
 * The Class Cache2009SQLExceptionParser.
 * 
 * @author Thomas Muenz
 */
public class Cache2009SQLExceptionParser implements SQLExceptionParser
{
	
	// Docu somewhere says that the negative Cache error codes are positive in
	// Cache JDBC. 
	/** The Constant SQLCODE_TableOrViewNotFound. */
	public static final short	SQLCODE_TableOrViewNotFound					= 30;
	
	/** The Constant SQLCODE_UNIQUEorPRIMARYKEYfailedUponINSERT. */
	public static final short	SQLCODE_UNIQUEorPRIMARYKEYfailedUponINSERT	= 119;
	
	/** The Constant SQLCODE_UNIQUEorPRIMARYKEYfailedUponUPDATE. */
	public static final short	SQLCODE_UNIQUEorPRIMARYKEYfailedUponUPDATE	= 120;
	
	/** The Constant SQLCODE_IndexAlreadyDefinedForTable. */
	public static final short	SQLCODE_IndexAlreadyDefinedForTable			= 324;
	
	
	/**
	 * @param e
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser#parseSQLException(java.sql.SQLException)
	 */
	@Override
	public SQLEngineException parseSQLException(final SQLException e)
	{
		switch(e.getErrorCode())
		{
			case SQLCODE_UNIQUEorPRIMARYKEYfailedUponINSERT:
				return new SQLEngineUniqueConstraintInsertViolationException(e);
			case SQLCODE_UNIQUEorPRIMARYKEYfailedUponUPDATE:
				return new SQLEngineUniqueConstraintUpdateViolationException(e);
			case SQLCODE_TableOrViewNotFound:
				return new SQLEngineTableNotFoundException(e);
			case SQLCODE_IndexAlreadyDefinedForTable:
				return new SQLEngineException(e);
			default:
				return new SQLEngineException(e);
		}
	}
	
	
	/**
	 * Instantiates a new cache2009 sql exception parser.
	 */
	public Cache2009SQLExceptionParser()
	{
		super();
	}
	
}
