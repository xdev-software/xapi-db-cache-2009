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

import static com.xdev.jadoth.sqlengine.SQL.LANG.SELECT;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.NEW_LINE;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.apo;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.comma_;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.par;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.rap;

import java.sql.ResultSet;

import com.xdev.jadoth.sqlengine.SQL;
import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineRuntimeException;
import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;
import com.xdev.jadoth.sqlengine.interfaces.SqlExecutor;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTable;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger.Event;
import com.xdev.jadoth.sqlengine.util.ResultTable;



/**
 * The Class Cache2009Dbms.
 * 
 * @author Thomas Muenz
 */
public class Cache2009Dbms
		extends
		DbmsAdaptor.Implementation<Cache2009Dbms, Cache2009DMLAssembler, Cache2009DDLMapper, Cache2009RetrospectionAccessor, Cache2009Syntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	// see [SQL Reference] > [SQL Reference Material] > [Data Types]
	/** The Constant MAX_VARCHAR_LENGTH. */
	protected static final int						MAX_VARCHAR_LENGTH				= 16374;
	
	protected static final char						IDENTIFIER_DELIMITER			= '"';
	
	public static final Cache2009Syntax				SYNTAX							= new Cache2009Syntax();
	
	// /////////////////////////////////////////////////////////////////////////
	// static fields //
	// ///////////////////
	/** The Constant INSERT. */
	protected static final Event					INSERT							= SqlTrigger.eINSERT;
	
	/** The Constant DELETE. */
	protected static final Event					DELETE							= SqlTrigger.eDELETE;
	
	/** The Constant UPDATE. */
	protected static final Event					UPDATE							= SqlTrigger.eUPDATE;
	
	/** The Constant BEFORE. */
	protected static final Cache2009Trigger.Time	BEFORE							= Cache2009Trigger.Time.BEFORE;
	
	/** The Constant AFTER. */
	protected static final Cache2009Trigger.Time	AFTER							= Cache2009Trigger.Time.AFTER;
	
	/** The Constant sqlSingleLineComment. */
	public static final String						sqlSingleLineComment			= "--";
	
	/** The Constant defaultPackage. */
	public static final String						defaultPackage					= "User";
	
	/** The Constant defaultSchema. */
	public static final String						defaultSchema					= "SQLUser";
	
	/** The Constant schemaSeperator. */
	public static final char						schemaSeperator					= '_';
	
	/** The Constant packageSeperator. */
	public static final char						packageSeperator				= '.';
	
	/** The Constant sqlProc_STATIC_TableToClassname. */
	protected static final String					sqlProc_STATIC_TableToClassname	= "TableToClassname";
	
	
	// /////////////////////////////////////////////////////////////////////////
	// static methods //
	// /////////////////
	
	/**
	 * Single connection.
	 * 
	 * @param host
	 *            the host
	 * @param port
	 *            the port
	 * @param user
	 *            the user
	 * @param password
	 *            the password
	 * @param namespace
	 *            the namespace
	 * @param properties
	 * @return the connection provider
	 */
	public static ConnectionProvider<Cache2009Dbms> singleConnection(final String host,
			final int port, final String user, final String password, final String namespace, final String properties)
	{
		return new ConnectionProvider.Body<>(new Cache2009ConnectionInformation(host,
				port,user,password,namespace,properties, new Cache2009Dbms()));
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////
	/**
	 * Instantiates a new cache2009 dbms.
	 */
	public Cache2009Dbms()
	{
		super(new Cache2009SQLExceptionParser(),false);
		this.setRetrospectionAccessor(new Cache2009RetrospectionAccessor(this));
		this.setDMLAssembler(new Cache2009DMLAssembler(this));
		this.setDdlMapper(new Cache2009DDLMapper(this));
		this.setSyntax(SYNTAX);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#updateSelectivity(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return tuneTable(this.getDatabaseGateway(),table.toString());
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	
	/**
	 * @param bytes
	 * @param sb
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#assembleTransformBytes(byte[],
	 *      java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, StringBuilder sb)
	{
		if(bytes == null)
		{
			if(sb == null)
			{
				sb = new StringBuilder(4);
			}
			return sb.append(SQL.LANG.NULL);
		}
		if(sb == null)
		{
			sb = new StringBuilder(bytes.length + 2);
		}
		sb.append(apo);
		byte b = 0;
		for(int i = 0; i < bytes.length; i++)
		{
			b = bytes[i];
			if(b == 39)
			{
				sb.append("''");
			}
			else
			{
				sb.append((char)(b < 0 ? 0x100 + b : b));
			}
		}
		sb.append(apo);
		return sb;
	}
	
	
	/**
	 * Sql proc_ table to classname.
	 * 
	 * @param sb
	 *            the sb
	 * @param tablename
	 *            the tablename
	 * @return the string builder
	 */
	public static StringBuilder sqlProc_TableToClassname(StringBuilder sb, final String tablename)
	{
		if(sb == null)
		{
			sb = new StringBuilder(1024);
		}
		return sb.append(SELECT).append(_).append(sqlProc_STATIC_TableToClassname).append(par)
				.append(apo).append(tablename).append(apo).append(rap);
	}
	
	
	/**
	 * Wipe all tables.
	 * 
	 * @param dbc
	 *            the dbc
	 * @return the int
	 */
	public static int wipeAllTables(final DatabaseGateway<Cache2009Dbms> dbc)
	{
		final String query = "SELECT " + NEW_LINE + "SqlQualifiedNameQ As Tablename" + NEW_LINE
				+ "--,KillExtent(SqlQualifiedNameQ) As KillExtent" + NEW_LINE
				+ "--,PurgeIndices(SqlQualifiedNameQ) As PurgeIndices" + NEW_LINE
				+ ",DropTable(SqlQualifiedNameQ) AS DropTable" + NEW_LINE
				+ "FROM %Dictionary.CompiledClass" + NEW_LINE + "WHERE System = 0" + NEW_LINE
				+ "  AND PrimarySuper LIKE '%Persistent%'" + NEW_LINE;
		final ResultSet rs = dbc.execute(SqlExecutor.query,query);
		final ResultTable rt = new ResultTable(rs);
		
		return rt.getRowCount();
		
	}
	
	
	/**
	 * Schema2package string.
	 * 
	 * @param schema
	 *            the schema
	 * @return the string
	 */
	public static final String schema2packageString(final String schema)
	{
		if(schema == null || schema.equals(defaultSchema))
		{
			return defaultPackage;
		}
		return schema == null ? null : schema.replace(schemaSeperator,packageSeperator);
	}
	
	
	/**
	 * Convert schema string to package string.
	 * 
	 * @param sqlSchemaString
	 *            the sql schema string
	 * @return the string
	 */
	protected static String convertSchemaStringToPackageString(final String sqlSchemaString)
	{
		if(sqlSchemaString == null || sqlSchemaString == "")
		{
			return defaultPackage;
		}
		return sqlSchemaString.replace(schemaSeperator,packageSeperator);
	}
	
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return false;
	}
	
	
	/**
	 * @param host
	 * @param port
	 * @param user
	 * @param password
	 * @param catalog
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#createConnectionInformation(java.lang.String,
	 *      int, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Cache2009ConnectionInformation createConnectionInformation(final String host,
			final int port, final String user, final String password, final String catalog, final String properties)
	{
		return new Cache2009ConnectionInformation(host,port,user,password,catalog,properties, this);
	}
	
	
	/**
	 * @param dbgw
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#initialize(com.xdev.jadoth.sqlengine.internal.DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<Cache2009Dbms> dbgw)
	{
		try
		{
			this.setDatabaseGateway(dbgw);
			this.getDdlMapper().initializeCallStatments();
		}
		catch(final Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor.Implementation#truncate(com.xdev.jadoth.sqlengine.internal.tables.SqlTable)
	 */
	@Override
	public Object truncate(final SqlTable table)
	{
		return killExtent(this.getDatabaseGateway(),table.toString());
	}
	
	
	/**
	 * @param fullQualifiedTableName
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#rebuildAllIndices(java.lang.String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return buildIndices(this.getDatabaseGateway(),fullQualifiedTableName);
	}
	
	
	/**
	 * Calculate selectivity.
	 * 
	 * @param fullQualifiedTableName
	 *            the full qualified table name
	 * @return the object
	 */
	public Object calculateSelectivity(final String fullQualifiedTableName)
	{
		return tuneTable(this.getDatabaseGateway(),fullQualifiedTableName);
	}
	
	/**
	 * Call singleton sql proc.
	 * 
	 * @param dbc
	 *            the dbc
	 * @param procName
	 *            the proc name
	 * @param params
	 *            the params
	 * @return the object
	 */
	public static Object callSingletonSqlProc(final DatabaseGateway<Cache2009Dbms> dbc,
			final String procName, final String... params)
	{
		final StringBuilder sb = new StringBuilder(1024).append(SELECT).append(_).append(procName)
				.append(par);
		if(params != null)
		{
			for(int i = 0; i < params.length; i++)
			{
				if(i > 0)
				{
					sb.append(comma_);
				}
				sb.append(params[i]);
			}
		}
		sb.append(rap);
		try
		{
			return dbc.execute(SqlExecutor.singleResultQuery,sb.toString());
		}
		catch(final Exception e)
		{
			throw new SQLEngineRuntimeException(e);
		}
	}
	
	
	/**
	 * Kill extent.
	 * 
	 * @param dbc
	 *            the dbc
	 * @param fullQualifiedTableName
	 *            the full qualified table name
	 * @return the object
	 */
	public static Object killExtent(final DatabaseGateway<Cache2009Dbms> dbc,
			final String fullQualifiedTableName)
	{
		return callSingletonSqlProc(dbc,"KillExtent",apo + fullQualifiedTableName + apo);
	}
	
	
	/**
	 * Builds the indices.
	 * 
	 * @param dbc
	 *            the dbc
	 * @param fullQualifiedTableName
	 *            the full qualified table name
	 * @return the object
	 */
	public static Object buildIndices(final DatabaseGateway<Cache2009Dbms> dbc,
			final String fullQualifiedTableName)
	{
		return callSingletonSqlProc(dbc,"BuildIndices",apo + fullQualifiedTableName + apo);
	}
	
	
	/**
	 * Tune table.
	 * 
	 * @param dbc
	 *            the dbc
	 * @param fullQualifiedTableName
	 *            the full qualified table name
	 * @return the object
	 */
	public static Object tuneTable(final DatabaseGateway<Cache2009Dbms> dbc,
			final String fullQualifiedTableName)
	{
		return callSingletonSqlProc(dbc,"TuneTable",apo + fullQualifiedTableName + apo);
	}
	
	
	/**
	 * Purge cached queries.
	 * 
	 * @param dbc
	 *            the dbc
	 * @param fullQualifiedTableName
	 *            the full qualified table name
	 * @return the object
	 */
	public static Object purgeCachedQueries(final DatabaseGateway<Cache2009Dbms> dbc,
			final String fullQualifiedTableName)
	{
		return callSingletonSqlProc(dbc,"PurgeCachedQueriesForTable",apo + fullQualifiedTableName
				+ apo);
	}
	
	
	/**
	 * Purge cached queries.
	 * 
	 * @param dbc
	 *            the dbc
	 * @return the object
	 */
	public static Object purgeCachedQueries(final DatabaseGateway<Cache2009Dbms> dbc)
	{
		return callSingletonSqlProc(dbc,"PurgeCachedQueries");
	}
	
	
	/**
	 * Gets the retrospection code generation note.
	 * 
	 * @return the retrospection code generation note
	 */
	public String getRetrospectionCodeGenerationNote()
	{
		return "SQLEngine Retrospection for Intersystems Cach� has the following limitations:"
				+ NEW_LINE
				+ "- precision and scale of data types are ingored so far (will be implemented some time in the future)";
	}
	
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}
