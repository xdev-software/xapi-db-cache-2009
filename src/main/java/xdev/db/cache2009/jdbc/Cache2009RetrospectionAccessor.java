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

import static com.xdev.jadoth.sqlengine.SQL.LANG.AND;
import static com.xdev.jadoth.sqlengine.SQL.LANG.ASC;
import static com.xdev.jadoth.sqlengine.SQL.LANG.FROM;
import static com.xdev.jadoth.sqlengine.SQL.LANG.IS_NOT_NULL;
import static com.xdev.jadoth.sqlengine.SQL.LANG.ORDER_BY;
import static com.xdev.jadoth.sqlengine.SQL.LANG.SELECT;
import static com.xdev.jadoth.sqlengine.SQL.LANG.WHERE;
import static com.xdev.jadoth.sqlengine.SQL.LANG._AS_;
import static com.xdev.jadoth.sqlengine.SQL.LANG.__AND;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.NEW_LINE;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.TAB;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._eq_;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.comma;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.dot;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.par;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.qt;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.quote;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.rap;

import java.util.HashMap;

import com.xdev.jadoth.sqlengine.SQL;
import com.xdev.jadoth.sqlengine.SQL.DATATYPE;
import com.xdev.jadoth.sqlengine.SQL.INDEXTYPE;
import com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.interfaces.SqlExecutor;
import com.xdev.jadoth.sqlengine.internal.SqlField;
import com.xdev.jadoth.sqlengine.internal.tables.SqlIndex;
import com.xdev.jadoth.sqlengine.internal.tables.SqlPrimaryKey;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;
import com.xdev.jadoth.sqlengine.util.ResultTable;



/**
 * The Class Cache2009RetrospectionAccessor.
 */
public class Cache2009RetrospectionAccessor extends
		DbmsRetrospectionAccessor.Implementation<Cache2009Dbms>
{
	
	/** The Constant selectItem. */
	protected static final String			selectItem							= _ + "" + _;
	
	/** The Constant emptyQuotes. */
	protected static final String			emptyQuotes							= quote + quote;
	
	/** The Constant sch_Dictionary. */
	public static final String				sch_Dictionary						= "%Dictionary";
	
	/** The Constant tbl_IndexDefinition. */
	public static final String				tbl_IndexDefinition					= "IndexDefinition";
	
	/** The Constant tbl_CompiledClass. */
	public static final String				tbl_CompiledClass					= "CompiledClass";
	
	/** The Constant tbl_PropertyDefinition. */
	public static final String				tbl_PropertyDefinition				= "PropertyDefinition";
	
	/** The Constant col_DefaultIDColumnName. */
	public static final String				col_DefaultIDColumnName				= "ID";
	
	/** The Constant col_SqlSchemaName. */
	public static final String				col_SqlSchemaName					= "SqlSchemaName";
	
	/** The Constant col_SqlTableName. */
	public static final String				col_SqlTableName					= "SqlTableName";
	
	/** The Constant col_SqlQualifiedNameQ. */
	public static final String				col_SqlQualifiedNameQ				= "SqlQualifiedNameQ";
	
	/** The Constant col_System. */
	public static final String				col_System							= "System";
	
	/** The Constant SYSTEMTABLE_TABLES. */
	public static final SqlTableIdentity	SYSTEMTABLE_TABLES					= new SqlTableIdentity(
																						sch_Dictionary,
																						tbl_CompiledClass,
																						"CC");
	
	/** The Constant SYSTEMTABLE_COLUMNS. */
	public static final SqlTableIdentity	SYSTEMTABLE_COLUMNS					= new SqlTableIdentity(
																						sch_Dictionary,
																						tbl_PropertyDefinition,
																						"PRPD");
	
	/** The Constant SYSTEMTABLE_INDICES. */
	public static final SqlTableIdentity	SYSTEMTABLE_INDICES					= new SqlTableIdentity(
																						sch_Dictionary,
																						tbl_IndexDefinition,
																						"INDD");
	
	/** The Constant table_Dictionary_IndexDefinition. */
	public static final String				table_Dictionary_IndexDefinition	= sch_Dictionary
																						+ dot
																						+ tbl_IndexDefinition;
	
	/** The Constant table_Dictionary_CompiledClass. */
	public static final String				table_Dictionary_CompiledClass		= sch_Dictionary
																						+ dot
																						+ tbl_CompiledClass;
	
	/** The Constant table_Dictionary_PropertyDefinition. */
	public static final String				table_Dictionary_PropertyDefinition	= sch_Dictionary
																						+ dot
																						+ tbl_PropertyDefinition;
	
	
	// /////////////////////////////////////////////////////////////////////////
	// static methods //
	// ///////////////////
	/**
	 * Parses the column parameters.
	 * 
	 * @param parameterString
	 *            the parameter string
	 * @return the hash map
	 */
	protected static HashMap<String, String> parseColumnParameters(final String parameterString)
	{
		if(parameterString == null)
		{
			return null;
		}
		
		String[] paramChunks = parameterString.split(",");
		final HashMap<String, String> parameters = new HashMap<>(paramChunks.length);
		for(String p : paramChunks)
		{
			final String[] kv = p.split("=",2);
			final String key = kv[0];
			final String value = kv.length > 1 ? kv[1] : null;
			if(key.length() > 1)
			{
				parameters.put(key,value);
			}
		}
		return parameters;
	}
	
	
	/**
	 * @param rawDefaultValue
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor.Implementation#parseColumnDefaultValue(java.lang.Object)
	 */
	@Override
	public Object parseColumnDefaultValue(final Object rawDefaultValue)
	{
		if(rawDefaultValue == null)
		{
			return null;
		}
		
		final String defValString = rawDefaultValue.toString();
		/*
		 * (21.08.2009 TM)NOTE: Cache does buggily not distinguish between ""
		 * and NULL on Object Script level. Sadly, Dictionary returns default
		 * value in OS form ("..." for strings and "" for null) So retrospection
		 * cannot distinguish between "" and NULL either
		 */
		if(defValString.equals(emptyQuotes))
		{
			return null;
		}
		
		final int len = defValString.length();
		final boolean isEscapedString = defValString.charAt(0) == qt
				&& defValString.charAt(len - 1) == qt;
		if(isEscapedString)
		{
			return defValString.substring(1,len - 1);
		}
		return defValString;
	}
	
	
	/**
	 * Gets the max length parameter.
	 * 
	 * @param columnParameters
	 *            the column parameters
	 * @return the max length parameter
	 */
	protected static int getMaxLengthParameter(final HashMap<String, String> columnParameters)
	{
		if(columnParameters != null)
		{
			String maxLen = columnParameters.get("MAXLEN");
			if(maxLen != null)
			{
				try
				{
					return Integer.parseInt(maxLen);
				}
				catch(Exception e)
				{
					// jump to return 0
				}
			}
		}
		return 0;
	}
	
	
	/**
	 * Select_ table to classname.
	 * 
	 * @param sb
	 *            the sb
	 * @param tablename
	 *            the tablename
	 * @return the string builder
	 */
	public static StringBuilder select_TableToClassname(StringBuilder sb, final String tablename)
	{
		if(sb == null)
		{
			sb = new StringBuilder(1024);
		}
		return sb.append(SELECT).append(_).append(col_DefaultIDColumnName).append(_).append(FROM)
				.append(_).append(table_Dictionary_CompiledClass).append(_).append(WHERE).append(_)
				.append(col_SqlQualifiedNameQ).append(_eq_)
				.append(SQL.util.escapeIfNecessary(tablename));
	}
	
	/** The Constant queryLoadTables_WHERE_System_etc. */
	private static final String	queryLoadTables_WHERE_System_etc	= SELECT
																			+ NEW_LINE
																			+ _
																			+ _
																			+ col_SqlSchemaName
																			+ _AS_
																			+ DbmsRetrospectionAccessor.Column_TABLE_SCHEMA
																			+ comma
																			+ NEW_LINE
																			+ _
																			+ _
																			+ col_SqlTableName
																			+ _AS_
																			+ DbmsRetrospectionAccessor.Column_TABLE_NAME
																			+ NEW_LINE
																			+
																			
																			FROM
																			+ _
																			+ table_Dictionary_CompiledClass
																			+ NEW_LINE
																			+
																			
																			WHERE
																			+ _
																			+ col_System
																			+ _eq_
																			+ 0
																			+ NEW_LINE
																			+ __AND
																			+ _
																			+ "ClassType = 'persistent'";
	
	/** The Constant QueryLoadIndices_WHERE_Table_eq_. */
	private static final String	QueryLoadIndices_WHERE_Table_eq_	= SELECT
																			+ NEW_LINE
																			+ "  NVL(SqlName,Name)"
																			+ _AS_
																			+ "Name"
																			+ comma
																			+ NEW_LINE
																			+ "  Type"
																			+ comma
																			+ NEW_LINE
																			+ "  Properties"
																			+ _AS_
																			+ "Columns"
																			+ comma
																			+ NEW_LINE
																			+ "  _Unique"
																			+ _AS_
																			+ "IsUnique"
																			+ comma
																			+ NEW_LINE
																			+ "  PrimaryKey"
																			+ comma
																			+ NEW_LINE
																			+ "  Parameters"
																			+ NEW_LINE
																			+
																			
																			FROM
																			+ _
																			+ table_Dictionary_IndexDefinition
																			+ NEW_LINE +
																			
																			WHERE + _ + "Type" + _
																			+ IS_NOT_NULL
																			+ NEW_LINE + __AND + _
																			+ "parent = ";
	
	/** The Constant QueryLoadColumns_start. */
	private static final String	QueryLoadColumns_start				= SELECT
																			+ NEW_LINE
																			+ selectItem
																			+ "PROP.Name"
																			+ comma
																			+ NEW_LINE
																			+ selectItem
																			+ "PROP.Type"
																			+ _AS_
																			+ "Type"
																			+ comma
																			+ NEW_LINE
																			+ 
	                                                         /*
	                                                          *  Only %Library Type (+mapping) allows reconstruction of DDL-Type
	                                                          *  
	                                                          *  sqlComment+"TYPES.OdbcType"+_AS_+"Type"+comma+NEW_LINE+OdbcType
	                                                          *  is DOUBLE for %Float (because %Float is no real Float)
	                                                          *  
	                                                          *  sqlComment+"TYPES.ClientDataType"+_AS_+"Type"+comma+NEW_LINE+ClientDataType
	                                                          *  is VARCHAR for %TinyInt
	                                                         */
																			selectItem
																			+ "PROP.Required"
																			+ _AS_
																			+ "NotNull"
																			+ comma
																			+ NEW_LINE
																			+ selectItem
																			+ "CASE WHEN ("
																			+ NEW_LINE
																			+ TAB
																			+ SELECT
																			+ _
																			+ "COUNT(*)"
																			+ _
																			+ NEW_LINE
																			+ TAB
																			+ FROM
																			+ _
																			+ table_Dictionary_IndexDefinition
																			+ NEW_LINE
																			+ TAB
																			+ WHERE
																			+ _
																			+ "parent"
																			+ _eq_
																			+ "PROP.parent"
																			+ NEW_LINE
																			+ TAB
																			+ __AND
																			+ _
																			+ "Properties"
																			+ _eq_
																			+ "PROP.name"
																			+ NEW_LINE
																			+ TAB
																			+ __AND
																			+ _
																			+ "_Unique"
																			+ _eq_
																			+ 1
																			+ NEW_LINE
																			+ selectItem
																			+ ") > 0 THEN 1 ELSE 0 END"
																			+ _AS_
																			+ "IsUnique"
																			+ comma
																			+ NEW_LINE
																			+ selectItem
																			+ "PROP.InitialExpression"
																			+ _AS_
																			+ "DefaultValue"
																			+ comma
																			+ NEW_LINE
																			+ selectItem
																			+ "PROP.Parameters"
																			+ comma
																			+ NEW_LINE
																			+ selectItem
																			+ "PROP.Description"
																			+ NEW_LINE
																			+
																			
																			FROM
																			+ _
																			+ table_Dictionary_PropertyDefinition
																			+ _ + "PROP"
																			+ NEW_LINE
																			+
																			
																			WHERE 
																			+ _
																			+ "PROP.parent" + _eq_;
	
	/** The Constant QueryLoadColumns_end. */
	private static final String	QueryLoadColumns_end				= NEW_LINE
																			+ ORDER_BY
																			+ _
																			+ "PROP.SqlColumnNumber"
																			+ _ + ASC;
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	/**
	 * Instantiates a new cache2009 retrospection accessor.
	 * 
	 * @param dbmsadaptor
	 *            the dbmsadaptor
	 */
	public Cache2009RetrospectionAccessor(final Cache2009Dbms dbmsadaptor)
	{
		super(dbmsadaptor);
	}
	
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_INDICES(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_INDICES(final SqlTableIdentity table)
	{
		final StringBuilder sb = new StringBuilder(1024);
		sb.append(QueryLoadIndices_WHERE_Table_eq_);
		sb.append(par);
		select_TableToClassname(sb,table.toString());
		sb.append(rap);
		
		return sb.toString();
	}
	
	
	/**
	 * @param schemaInclusionPatterns
	 * @param schemaExcluionPatterns
	 * @param tableIncluionPatterns
	 * @param tableExcluionPatterns
	 * @param additionalWHERECondition
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_TABLES(java.lang.String[],
	 *      java.lang.String[], java.lang.String[], java.lang.String[],
	 *      java.lang.String)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_TABLES(final String[] schemaInclusionPatterns,
			final String[] schemaExcluionPatterns, final String[] tableIncluionPatterns,
			final String[] tableExcluionPatterns, final String additionalWHERECondition)
	{
		final StringBuilder sb = new StringBuilder(1024);
		sb.append(queryLoadTables_WHERE_System_etc);
		appendIncludeExcludeConditions(sb,schemaInclusionPatterns,schemaExcluionPatterns,
				tableIncluionPatterns,tableExcluionPatterns,col_SqlSchemaName,col_SqlTableName);
		if(additionalWHERECondition != null && additionalWHERECondition.length() > 0)
		{
			sb.append(NEW_LINE).append(AND).append(_).append(additionalWHERECondition);
		}
		
		return sb.toString();
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#getSystemTable_COLUMNS()
	 */
	@Override
	public SqlTableIdentity getSystemTable_COLUMNS()
	{
		return SYSTEMTABLE_COLUMNS;
	}
	
	
	/**
	 * Gets the system table_ indices.
	 * 
	 * @return the system table_ indices
	 */
	public SqlTableIdentity getSystemTable_INDICES()
	{
		return SYSTEMTABLE_INDICES;
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#getSystemTable_TABLES()
	 */
	@Override
	public SqlTableIdentity getSystemTable_TABLES()
	{
		return SYSTEMTABLE_TABLES;
	}
	
	
	/**
	 * @param table
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#createSelect_INFORMATION_SCHEMA_COLUMNS(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String createSelect_INFORMATION_SCHEMA_COLUMNS(final SqlTableIdentity table)
	{
		final StringBuilder sb = new StringBuilder(1024);
		sb.append(QueryLoadColumns_start);
		sb.append(par);
		select_TableToClassname(sb,table.toString());
		sb.append(rap);
		sb.append(QueryLoadColumns_end);
		return sb.toString();
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#getRetrospectionCodeGenerationNote()
	 */
	@Override
	public String getRetrospectionCodeGenerationNote()
	{
		return null;
	}
	
	
	/**
	 * @param table
	 * @return
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor.Implementation#loadColumns(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public SqlField[] loadColumns(final SqlTableIdentity table) throws SQLEngineException
	{
		final String selectInformationSchemaColumns = createSelect_INFORMATION_SCHEMA_COLUMNS(table);
		final ResultTable rt = new ResultTable(getDbmsAdaptor().getDatabaseGateway().execute(
				SqlExecutor.query,selectInformationSchemaColumns));
		final int rowCount = rt.getRowCount();
		final SqlField[] columns = new SqlField[rowCount];
		final Cache2009DDLMapper ddlMapper = this.getDbmsAdaptor().getDdlMapper();
		
		String name = null;
		DATATYPE type = null;
		Boolean notNull = false;
		Object defaultValue = null;
		HashMap<String, String> parameters = null;
		int length = 0;
		for(int i = 0; i < rowCount; i++)
		{
			name = rt.getValue(i,0).toString();
			type = ddlMapper.mapDataType(rt.getValue(i,1).toString());
			notNull = SQL.util.recognizeBoolean(rt.getValue(i,2));
			defaultValue = this.parseColumnDefaultValue(rt.getValue(i,4));
			parameters = parseColumnParameters((String)rt.getValue(i,5));
			length = getMaxLengthParameter(parameters);
			try
			{
				columns[i] = new SqlField(name,type,length,null,null,notNull,false,defaultValue);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			columns[i].setOwner(table);
		}
		return columns;
	}
	
	
	/**
	 * @param table
	 * @return
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsRetrospectionAccessor#loadIndices(com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public SqlIndex[] loadIndices(final SqlTableIdentity table) throws SQLEngineException
	{
		final String selectInformationSchemaIndices = createSelect_INFORMATION_SCHEMA_INDICES(table);
		final Cache2009DDLMapper ddlMapper = this.getDbmsAdaptor().getDdlMapper();
		
		final ResultTable rt = new ResultTable(this.getDbmsAdaptor().getDatabaseGateway()
				.execute(SqlExecutor.query,selectInformationSchemaIndices));
		final int rowCount = rt.getRowCount();
		
		final SqlIndex[] indices = new SqlIndex[rowCount];
		INDEXTYPE type = null;
		for(int i = 0; i < rowCount; i++)
		{
			final String[] columnList = rt.getValue(i,2).toString().split(",");
			if((Boolean)rt.getValue(i,4))
			{
				indices[i] = new SqlPrimaryKey((Object[])columnList);
				indices[i].setName(rt.getValue(i,0).toString());
				indices[i].setOwner(table);
			}
			else
			{
				type = ddlMapper.mapIndexType(rt.getValue(i,1).toString().toUpperCase());
				indices[i] = new SqlIndex(rt.getValue(i,0).toString(),table,type,
						(Object[])columnList);
			}
		}
		return indices;
	}
	
}
