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

import static com.xdev.jadoth.sqlengine.SQL.LANG.CALL;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.cma;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Hashtable;

import com.xdev.jadoth.sqlengine.SQL;
import com.xdev.jadoth.sqlengine.SQL.DATATYPE;
import com.xdev.jadoth.sqlengine.SQL.INDEXTYPE;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDDLMapper;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.internal.SqlField;
import com.xdev.jadoth.sqlengine.internal.tables.SqlDdlTable;
import com.xdev.jadoth.sqlengine.internal.tables.SqlIndex;
import com.xdev.jadoth.sqlengine.internal.tables.SqlProcedure;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTable;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger.Event;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger.Time;

import xdev.db.cache2009.jdbc.Cache2009Trigger.Language;


/**
 * The Class Cache2009DDLMapper.
 * 
 * @author Thomas Muenz
 */
@SuppressWarnings("deprecation")
public class Cache2009DDLMapper extends StandardDDLMapper<Cache2009Dbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	/** The Constant sqlProcReturnValue. */
	protected static final int							sqlProcReturnValue							= 1;
	
	/** The Constant sqlProcParam_package. */
	protected static final int							sqlProcParam_package						= 2;
	
	/** The Constant sqlProcParam_className. */
	protected static final int							sqlProcParam_className						= 3;
	
	/** The Constant sqlProcParam_createTable. */
	protected static final int							sqlProcParam_createTable					= 4;
	
	/** The Constant sqlProcParam_name. */
	protected static final int							sqlProcParam_name							= 4;
	
	/** The Constant sqlProcParam_Property_defaultValue. */
	protected static final int							sqlProcParam_Property_defaultValue			= 5;
	
	/** The Constant sqlProcParam_Property_required. */
	protected static final int							sqlProcParam_Property_required				= 6;
	
	/** The Constant sqlProcParam_Property_unique. */
	protected static final int							sqlProcParam_Property_unique				= 7;
	
	/** The Constant sqlProcParam_Property_columnNumber. */
	protected static final int							sqlProcParam_Property_columnNumber			= 8;
	
	/** The Constant sqlProcParam_Property_type. */
	protected static final int							sqlProcParam_Property_type					= 9;
	
	/** The Constant sqlProcParam_Property_maxLen. */
	protected static final int							sqlProcParam_Property_maxLen				= 10;
	
	/** The Constant sqlProcParam_Property_isCacheType. */
	protected static final int							sqlProcParam_Property_isCacheType			= 11;
	
	/** The Constant sqlProcParam_Index_propertyList. */
	protected static final int							sqlProcParam_Index_propertyList				= 5;
	
	/** The Constant sqlProcParam_Index_type. */
	protected static final int							sqlProcParam_Index_type						= 6;
	
	/** The Constant sqlProcParam_Index_unique. */
	protected static final int							sqlProcParam_Index_unique					= 7;
	
	/** The Constant sqlProcParam_Index_description. */
	protected static final int							sqlProcParam_Index_description				= 8;
	
	/** The Constant sqlProcParam_Trigger_trgEvent. */
	protected static final int							sqlProcParam_Trigger_trgEvent				= 5;
	
	/** The Constant sqlProcParam_Trigger_trgLanguage. */
	protected static final int							sqlProcParam_Trigger_trgLanguage			= 6;
	
	/** The Constant sqlProcParam_Trigger_trgOrder. */
	protected static final int							sqlProcParam_Trigger_trgOrder				= 7;
	
	/** The Constant sqlProcParam_Trigger_trgTime. */
	protected static final int							sqlProcParam_Trigger_trgTime				= 8;
	
	/** The Constant sqlProcParam_Trigger_trgCode. */
	protected static final int							sqlProcParam_Trigger_trgCode				= 9;
	
	/** The Constant sqlProcParam_Trigger_description. */
	protected static final int							sqlProcParam_Trigger_description			= 10;
	
	/** The Constant sqlProcParam_Procedure_returnType. */
	protected static final int							sqlProcParam_Procedure_returnType			= 5;
	
	/** The Constant sqlProcParam_Procedure_parameters. */
	protected static final int							sqlProcParam_Procedure_parameters			= 6;
	
	/** The Constant sqlProcParam_Procedure_returnsResultSets. */
	protected static final int							sqlProcParam_Procedure_returnsResultSets	= 7;
	
	/** The Constant sqlProcParam_Procedure_code. */
	protected static final int							sqlProcParam_Procedure_code					= 8;
	
	/** The Constant sqlProc_CreateClass. */
	public static final String							sqlProc_CreateClass							= "CreateClass";
	
	/** The Constant sqlProc_CreateProperty. */
	public static final String							sqlProc_CreateProperty						= "CreateProperty";
	
	/** The Constant sqlProc_CreateIndex. */
	public static final String							sqlProc_CreateIndex							= "CreateIndex";
	
	/** The Constant sqlProc_CreateTrigger. */
	public static final String							sqlProc_CreateTrigger						= "CreateTrigger";
	
	/** The Constant sqlProc_CreateProcedure. */
	public static final String							sqlProc_CreateProcedure						= "CreateProcedure";
	
	/** The Constant sqlProc_MapDDLTypeToCacheType. */
	public static final String							sqlProc_MapDDLTypeToCacheType				= "MapDDLTypeToCacheType";
	
	/** The Constant sqlProc_CompileClass. */
	public static final String							sqlProc_CompileClass						= "CompileClass";
	
	/** The Constant DATATYPE_BIT. */
	public static final String							DATATYPE_BIT								= "BIT";
	
	public static final String							DATATYPE_GLOBAL_BINARY_STREAM				= "%GlobalBinaryStream";
	
	// /////////////////////////////////////////////////////////////////////////
	// instance fields //
	// ///////////////////
	
	/** The create tables by dictionary. */
	private boolean										createTablesByDictionary					= true;
	
	/** The create single index by dictionary. */
	private boolean										createSingleIndexByDictionary				= true;
	
	/** The create single trigger by dictionary. */
	private boolean										createSingleTriggerByDictionary				= true;
	
	/** The create single procedure by dictionary. */
	private boolean										createSingleProcedureByDictionary			= true;
	
	/** The call_ create class. */
	private CallableStatement							call_CreateClass							= null;
	
	/** The call_ create property. */
	private CallableStatement							call_CreateProperty							= null;
	
	/** The call_ create index. */
	private CallableStatement							call_CreateIndex							= null;
	
	/** The call_ create trigger. */
	private CallableStatement							call_CreateTrigger							= null;
	
	/** The call_ create procedure. */
	private CallableStatement							call_CreateProcedure						= null;
	
	/** The call_ map ddl type to cache type. */
	private CallableStatement							call_MapDDLTypeToCacheType					= null;
	
	/** The call_ compile class. */
	private CallableStatement							call_CompileClass							= null;
	
	/** The Constant cacheLibraryTypes. */
	private static final Hashtable<String, DATATYPE>	cacheLibraryTypes							= createCacheLibraryTypes();
	
	
	/**
	 * Creates the cache library types.
	 * 
	 * @return the hashtable
	 */
	private static final Hashtable<String, DATATYPE> createCacheLibraryTypes()
	{

		final Hashtable<String, DATATYPE> c = new Hashtable<>(20);
		
		c.put("%Library.Boolean",SQL.DATATYPE.BOOLEAN);
		
		c.put("%Library.TinyInt",SQL.DATATYPE.TINYINT);
		c.put("%Library.SmallInt",SQL.DATATYPE.SMALLINT);
		c.put("%Library.Integer",SQL.DATATYPE.INT);
		c.put("%Library.BigInt",SQL.DATATYPE.BIGINT);
		
		c.put("%Library.Float",SQL.DATATYPE.FLOAT);
		c.put("%Library.Double",SQL.DATATYPE.DOUBLE);
		
		c.put("%Library.Date",SQL.DATATYPE.DATE);
		c.put("%Library.TimeStamp",SQL.DATATYPE.TIMESTAMP);
		
		c.put("%Library.String",SQL.DATATYPE.VARCHAR);
		
		c.put("%Library.GlobalBinaryStream",SQL.DATATYPE.BLOB);
		
		return c;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	/**
	 * Instantiates a new cache2009 ddl mapper.
	 * 
	 * @param dbmsAdaptor
	 *            the dbms adaptor
	 */
	protected Cache2009DDLMapper(final Cache2009Dbms dbmsAdaptor)
	{
		super(dbmsAdaptor);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	
	/**
	 * Checks if is creates the tables by dictionary.
	 * 
	 * @return the createTablesByDictionary
	 */
	public boolean isCreateTablesByDictionary()
	{
		return this.createTablesByDictionary;
	}
	
	
	/**
	 * Checks if is creates the single index by dictionary.
	 * 
	 * @return the createSingleIndexByDictionary
	 */
	public boolean isCreateSingleIndexByDictionary()
	{
		return this.createSingleIndexByDictionary;
	}
	
	
	/**
	 * Checks if is creates the single trigger by dictionary.
	 * 
	 * @return the createSingleTriggerByDictionary
	 */
	public boolean isCreateSingleTriggerByDictionary()
	{
		return this.createSingleTriggerByDictionary;
	}
	
	
	/**
	 * Checks if is creates the single procedure by dictionary.
	 * 
	 * @return the createSingleProcedureByDictionary
	 */
	public boolean isCreateSingleProcedureByDictionary()
	{
		return this.createSingleProcedureByDictionary;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	
	/**
	 * Sets the creates the tables by dictionary.
	 * 
	 * @param createTablesByDictionary
	 *            the createTablesByDictionary to set
	 */
	public void setCreateTablesByDictionary(final boolean createTablesByDictionary)
	{
		this.createTablesByDictionary = createTablesByDictionary;
	}
	
	
	/**
	 * Sets the creates the single index by dictionary.
	 * 
	 * @param createSingleIndexByDictionary
	 *            the createSingleIndexByDictionary to set
	 */
	public void setCreateSingleIndexByDictionary(final boolean createSingleIndexByDictionary)
	{
		this.createSingleIndexByDictionary = createSingleIndexByDictionary;
	}
	
	
	/**
	 * Sets the creates the single trigger by dictionary.
	 * 
	 * @param createSingleTriggerByDictionary
	 *            the createSingleTriggerByDictionary to set
	 */
	public void setCreateSingleTriggerByDictionary(final boolean createSingleTriggerByDictionary)
	{
		this.createSingleTriggerByDictionary = createSingleTriggerByDictionary;
	}
	
	
	/**
	 * Sets the creates the single procedure by dictionary.
	 * 
	 * @param createSingleProcedureByDictionary
	 *            the createSingleProcedureByDictionary to set
	 */
	public void setCreateSingleProcedureByDictionary(final boolean createSingleProcedureByDictionary)
	{
		this.createSingleProcedureByDictionary = createSingleProcedureByDictionary;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * Enhances mapDataType() by mapping for Cach� Library types
	 * (%Library.String etc).<br>
	 * If this mapping yields no result, the super class method is called.
	 * 
	 * @param dataTypeString
	 *            Cach� Library type or standard DDL type
	 * @return the dATATYPE
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper.Implementation#mapDataType(java.lang.String)
	 */
	@Override
	public DATATYPE mapDataType(final String dataTypeString)
	{
		DATATYPE mappedType = cacheLibraryTypes.get(dataTypeString);
		if(mappedType != null)
		{
			return mappedType;
		}
		
		mappedType = super.mapDataType(dataTypeString);
		
		return mappedType != null ? mappedType : SQL.DATATYPE.VARCHAR;
	}
	
	
	/**
	 * Lookup ddbms data type mapping.
	 * 
	 * @param dataType
	 *            the data type
	 * @param table
	 *            the table
	 * @return the string
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper#lookupDdbmsDataTypeMapping(com.xdev.jadoth.sqlengine.SQL.DATATYPE,
	 *      com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String lookupDdbmsDataTypeMapping(final DATATYPE dataType, final SqlTableIdentity table)
	{
		switch(dataType)
		{
			case BOOLEAN:
				return DATATYPE_BIT;    // Although Cache has a %Boolean type,
										// BOOLEAN is not recognized.
			case BLOB:
				return DATATYPE_GLOBAL_BINARY_STREAM;
			default:
				return super.lookupDdbmsDataTypeMapping(dataType,table);
		}
	}
	
	
	/**
	 * @param table
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper.Implementation#createTable(com.xdev.jadoth.sqlengine.internal.tables.SqlDdlTable)
	 */
	@Override
	public void createTable(final SqlDdlTable table) throws SQLEngineException
	{
		if(this.getDbmsAdaptor().getDatabaseGateway() == null || table == null)
		{
			return;
		}
		
		if(!this.createTablesByDictionary)
		{
			super.createTable(table);
			return;
		}
		
		final SqlField[] fields = table.util.getSqlFields();
		try
		{
			this.callCreateTable(table,false);
			for(int i = 0; i < fields.length; i++)
			{
				final SqlField f = fields[i];
				final Object defVal = f.getDefaultValue();
				this.callCreateProperty(table,f.getName(),
						defVal == null ? null : defVal.toString(),f.isNotNull(),f.isUnique(),i + 1,
						f.getType(),f.getTypeLength());
			}
		}
		catch(final SQLException e)
		{
			throw new SQLEngineException(e);
		}
	}
	
	
	/**
	 * @param index
	 * @param tableCreation
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper.Implementation#createIndex(com.xdev.jadoth.sqlengine.internal.tables.SqlIndex,
	 *      boolean)
	 */
	@Override
	public void createIndex(final SqlIndex index, final boolean tableCreation)
			throws SQLEngineException
	{
		final boolean singleIndexCreationByDictionary = !tableCreation
				&& this.createSingleIndexByDictionary;
		
		if((tableCreation && this.createTablesByDictionary) || singleIndexCreationByDictionary)
		{
			try
			{
				this.callCreateIndex(index.getOwner().sql().schema,index.getOwner().sql().name,
						index.getName(),index.getColumnListString(),index.getType());
				if(singleIndexCreationByDictionary)
				{
					this.compile((SqlTable)index.getOwner());
				}
			}
			catch(final SQLException e)
			{
				throw new SQLEngineException(e);
			}
		}
		else
		{
			// use standard DDL command (CREATE INDEX)
			super.createIndex(index,tableCreation);
		}
	}
	
	
	/**
	 * @param trigger
	 * @param tableCreation
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper.Implementation#createTrigger(com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger,
	 *      boolean)
	 */
	@Override
	public void createTrigger(final SqlTrigger trigger, final boolean tableCreation)
			throws SQLEngineException
	{
		final boolean singleTriggerCreationByDictionary = !tableCreation
				&& this.createSingleTriggerByDictionary;
		
		if((tableCreation && this.createTablesByDictionary) || singleTriggerCreationByDictionary)
		{
			Language language = Language.SQL;
			int order = 1;
			final String code = trigger.assembleActionStatements(null).toString();
			if(trigger instanceof Cache2009Trigger)
			{
				language = ((Cache2009Trigger)trigger).getLanguage();
				order = ((Cache2009Trigger)trigger).getOrder();
			}
			try
			{
				this.callCreateTrigger(trigger.getTable().sql().schema,
						trigger.getTable().sql().name,trigger.getName(),trigger.getEvent(),
						language,order,trigger.getTime(),"\t" + code.replaceAll("\n","\n\t")
				);
				if(singleTriggerCreationByDictionary)
				{
					this.compile((SqlTable)trigger.getTable());
				}
			}
			catch(final SQLException e)
			{
				throw new SQLEngineException(e);
			}
		}
		else
		{
			// use standard DDL command (CREATE TRIGGER)
			super.createTrigger(trigger,tableCreation);
		}
	}
	
	
	/**
	 * @param table
	 * @return
	 * @throws SQLEngineException
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper.Implementation#postCreateTableActions(com.xdev.jadoth.sqlengine.internal.tables.SqlDdlTable)
	 */
	@Override
	public String postCreateTableActions(final SqlDdlTable table) throws SQLEngineException
	{
		if(this.createTablesByDictionary)
		{
			return this.compile(table);
		}
		return null;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// declared methods //
	// ///////////////////
	
	/**
	 * Compile.
	 * 
	 * @param table
	 *            the table
	 * @return the string
	 * @throws SQLEngineException
	 *             the sQL engine exception
	 */
	public String compile(final SqlTable table) throws SQLEngineException
	{
		try
		{
			this.call_CompileClass.setString(sqlProcParam_package,table.sql().schema);
			this.call_CompileClass.setString(sqlProcParam_className,table.sql().name);
			this.call_CompileClass.execute();
			return this.call_CompileClass.getString(1);
		}
		catch(final SQLException e)
		{
			throw new SQLEngineException(e);
		}
	}
	
	
	/**
	 * Sets the dictionary creation enabled.
	 * 
	 * @param enabled
	 *            the enabled
	 * @return the cache2009 ddl mapper
	 */
	public Cache2009DDLMapper setDictionaryCreationEnabled(final boolean enabled)
	{
		this.createTablesByDictionary          = enabled;
		this.createSingleIndexByDictionary     = enabled;
		this.createSingleTriggerByDictionary   = enabled;
		this.createSingleProcedureByDictionary = enabled;
		return this;
	}
	
	
	/**
	 * Initialize call statments.
	 * 
	 * @throws SQLException
	 *             the sQL exception
	 */
	protected void initializeCallStatments() throws SQLException
	{
		try
		{
			final Connection con = this.getDbmsAdaptor().getJdbcConnection();
			
			this.call_CreateClass = con.prepareCall("? = " + CALL + " " + sqlProc_CreateClass
					+ "(?,?,?)");
			this.call_CreateClass.registerOutParameter(1,Types.VARCHAR);
			
			this.call_CreateProperty = con.prepareCall("? = " + CALL + " " + sqlProc_CreateProperty
					+ "(?,?,?,?,?,?,?,?,?)");
			this.call_CreateProperty.registerOutParameter(1,Types.VARCHAR);
			
			this.call_CreateIndex = con.prepareCall("? = " + CALL + " " + sqlProc_CreateIndex
					+ "(?,?,?,?,?,?)");
			this.call_CreateIndex.registerOutParameter(1,Types.VARCHAR);
			
			this.call_CreateTrigger = con.prepareCall("? = " + CALL + " " + sqlProc_CreateTrigger
					+ "(?,?,?,?,?,?,?,?)");
			this.call_CreateTrigger.registerOutParameter(1,Types.VARCHAR);
			
			this.call_CreateProcedure = con.prepareCall("? = " + CALL + " "
					+ sqlProc_CreateProcedure + "(?,?,?,?,?,?,?)");
			this.call_CreateProcedure.registerOutParameter(1,Types.VARCHAR);
			
			this.call_CompileClass = con.prepareCall("? = " + CALL + " " + sqlProc_CompileClass
					+ "(?,?)");
			this.call_CompileClass.registerOutParameter(1,Types.VARCHAR);
			
			this.call_MapDDLTypeToCacheType = con.prepareCall("? = " + CALL + " "
					+ sqlProc_MapDDLTypeToCacheType + "(?)");
			this.call_MapDDLTypeToCacheType.registerOutParameter(1,Types.VARCHAR);
		}
		catch(final SQLException e)
		{
			this.setDictionaryCreationEnabled(false);
		}
	}
	
	
	/**
	 * Call create table.
	 * 
	 * @param table
	 *            the table
	 * @param onlyClass
	 *            the only class
	 * @return the string
	 * @throws SQLException
	 *             the sQL exception
	 */
	public String callCreateTable(final SqlTableIdentity table, final boolean onlyClass)
			throws SQLException
	{
		this.call_CreateClass.setString(sqlProcParam_package,table.sql().schema);
		this.call_CreateClass.setString(sqlProcParam_className,table.sql().name);
		this.call_CreateClass.setBoolean(sqlProcParam_createTable,!onlyClass);
		this.call_CreateClass.execute();
		final String result = this.call_CreateClass.getString(sqlProcReturnValue);
		return result;
	}
	
	
	/**
	 * Call create property.
	 * 
	 * @param table
	 *            the table
	 * @param propertyName
	 *            the property name
	 * @param defaultValue
	 *            the default value
	 * @param required
	 *            the required
	 * @param unique
	 *            the unique
	 * @param columnNumber
	 *            the column number
	 * @param type
	 *            the type
	 * @param length
	 *            the length
	 * @return the string
	 * @throws SQLException
	 *             the sQL exception
	 */
	public String callCreateProperty(final SqlTableIdentity table, final String propertyName,
			final String defaultValue, final boolean required, final boolean unique,
			final int columnNumber, final DATATYPE type, final int length) throws SQLException
	{
		final CallableStatement c = this.call_CreateProperty;
		c.setString(sqlProcParam_package,table.sql().schema);
		c.setString(sqlProcParam_className,table.sql().name);
		c.setString(sqlProcParam_name,propertyName);
		c.setString(sqlProcParam_Property_defaultValue,defaultValue);
		c.setBoolean(sqlProcParam_Property_required,required);
		c.setBoolean(sqlProcParam_Property_unique,unique);
		c.setInt(sqlProcParam_Property_columnNumber,columnNumber);
		c.setString(sqlProcParam_Property_type,this.getDataTypeDDLString(type,table));
		c.setInt(sqlProcParam_Property_maxLen,length);
		c.execute();
		final String result = c.getString(sqlProcReturnValue);
		return result;
	}
	
	
	/**
	 * Call create procedure.
	 * 
	 * @param table
	 *            the table
	 * @param propertyName
	 *            the property name
	 * @param type
	 *            the type
	 * @param returnsResultSets
	 *            the returns result sets
	 * @param code
	 *            the code
	 * @param parameterList
	 *            the parameter list
	 * @return the string
	 * @throws SQLException
	 *             the sQL exception
	 */
	public String callCreateProcedure(final SqlTableIdentity table, final String propertyName,
			final DATATYPE type, final boolean returnsResultSets, final String code,
			final SqlProcedure.Parameter<Cache2009Dbms>... parameterList) throws SQLException
	{
		final StringBuilder sb = new StringBuilder(512);
		String params = null;
		if(parameterList != null)
		{
			for(int i = 0; i < parameterList.length; i++)
			{
				if(i > 0)
				{
					sb.append(cma);
				}
				sb.append(parameterList[i].toDdlString(this));
			}
			params = sb.toString();
		}
		
		final CallableStatement c = this.call_CreateProcedure;
		c.setString(sqlProcParam_package,table.sql().schema);
		c.setString(sqlProcParam_className,table.sql().name);
		c.setString(sqlProcParam_name,propertyName);
		c.setString(sqlProcParam_Procedure_returnType,
				type == null ? null : this.getDataTypeDDLString(type,table));
		c.setString(sqlProcParam_Procedure_parameters,params);
		c.setBoolean(sqlProcParam_Procedure_returnsResultSets,returnsResultSets);
		c.setString(sqlProcParam_Procedure_code,code.replaceAll("\r\n","\n")
				.replaceAll("\n","\r\n"));
		c.execute();
		final String result = c.getString(sqlProcReturnValue);
		return result;
	}
	
	
	/**
	 * Call create index.
	 * 
	 * @param schema
	 *            the schema
	 * @param tablename
	 *            the tablename
	 * @param indexName
	 *            the index name
	 * @param propertyList
	 *            the property list
	 * @param type
	 *            the type
	 * @return the string
	 * @throws SQLException
	 *             the sQL exception
	 */
	public String callCreateIndex(final String schema, final String tablename,
			final String indexName, final String propertyList, final INDEXTYPE type)
			throws SQLException
	{
		final CallableStatement c = this.call_CreateIndex;
		c.setString(sqlProcParam_package,schema);
		c.setString(sqlProcParam_className,tablename);
		c.setString(sqlProcParam_name,indexName);
		c.setString(sqlProcParam_Index_propertyList,propertyList);
		c.setString(sqlProcParam_Index_type,type != null ? type.name() : null);
		c.setBoolean(sqlProcParam_Index_unique,type == INDEXTYPE.UNIQUE);
		c.execute();
		final String result = c.getString(sqlProcReturnValue);
		return result;
	}
	
	
	/**
	 * Call create trigger.
	 * 
	 * @param schema
	 *            the schema
	 * @param tablename
	 *            the tablename
	 * @param triggerName
	 *            the trigger name
	 * @param event
	 *            the event
	 * @param language
	 *            the language
	 * @param order
	 *            the order
	 * @param time
	 *            the time
	 * @param code
	 *            the code
	 * @return the string
	 * @throws SQLException
	 *             the sQL exception
	 */
	public String callCreateTrigger(final String schema, final String tablename,
			final String triggerName, final Event event, final Language language, final int order,
			final Time time, final String code) throws SQLException
	{
		final CallableStatement c = this.call_CreateTrigger;
		c.setString(sqlProcParam_package,schema);
		c.setString(sqlProcParam_className,tablename);
		c.setString(sqlProcParam_name,triggerName);
		c.setString(sqlProcParam_Trigger_trgEvent,event.toString());
		c.setString(sqlProcParam_Trigger_trgLanguage,language.toDictionaryString());
		c.setInt(sqlProcParam_Trigger_trgOrder,order);
		c.setString(sqlProcParam_Trigger_trgTime,time.toString());
		// first consolidate \r\n and \n to only \n and then replace all \n with
		// \r\n
		c.setString(sqlProcParam_Trigger_trgCode,
				code.replaceAll("\r\n","\n").replaceAll("\n","\r\n"));
		c.execute();
		final String result = c.getString(sqlProcReturnValue);
		return result;
	}
	
}
