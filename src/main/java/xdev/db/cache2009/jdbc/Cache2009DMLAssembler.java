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

import static com.xdev.jadoth.sqlengine.SQL.LANG.TOP;
import static com.xdev.jadoth.sqlengine.SQL.LANG.UNION;
import static com.xdev.jadoth.sqlengine.SQL.LANG.UNION_ALL;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.apo;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.dot;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.ASEXPRESSION;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.indent;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isOmitAlias;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isSingleLine;
import static com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression.Utils.getAlias;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.DbmsConfiguration;
import com.xdev.jadoth.sqlengine.dbms.DbmsSyntax;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineInvalidIdentifier;
import com.xdev.jadoth.sqlengine.internal.QueryPart;
import com.xdev.jadoth.sqlengine.internal.SqlColumn;
import com.xdev.jadoth.sqlengine.internal.SqlExpression;
import com.xdev.jadoth.sqlengine.internal.SqlIdentifier;
import com.xdev.jadoth.sqlengine.internal.SqlTimestamp;
import com.xdev.jadoth.sqlengine.internal.SqlxAggregateCOLLECT_asString;
import com.xdev.jadoth.sqlengine.internal.interfaces.SelectItem;
import com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


/**
 * The Class Cache2009DMLAssembler.
 */
public class Cache2009DMLAssembler extends StandardDMLAssembler<Cache2009Dbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	/** The Constant _TOP_. */
	protected static final String	_TOP_				= _ + TOP + _;
	
	/** The Constant FUNCTION_AGG_LIST. */
	public static final String		FUNCTION_AGG_LIST	= "LIST";
	
	/** The Constant FUNCTION_AGG_XMLAGG. */
	public static final String		FUNCTION_AGG_XMLAGG	= "XMLAGG";
	
	protected static final char		DELIM				= '"';
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	/**
	 * Instantiates a new cache2009 dml assembler.
	 * 
	 * @param cache2009Dbms
	 *            the cache2009 dbms
	 */
	public Cache2009DMLAssembler(final Cache2009Dbms cache2009Dbms)
	{
		super(cache2009Dbms);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @param selectItem
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSelectItem(com.xdev.jadoth.sqlengine.internal.interfaces.SelectItem,
	 *      java.lang.StringBuilder, int, int)
	 */
	@Override
	public void assembleSelectItem(final SelectItem selectItem, final StringBuilder sb,
			final int indentLevel, final int flags)
	{
		if(selectItem instanceof SqlExpression)
		{
			this.assembleExpression((SqlExpression)selectItem,sb,indentLevel,flags);
		}
		else
		{
			super.assembleSelectItem(selectItem,sb,indentLevel,flags);
		}
	}
	
	
	/**
	 * @param expression
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleExpression(com.xdev.jadoth.sqlengine.internal.SqlExpression,
	 *      java.lang.StringBuilder, int, int)
	 */
	@Override
	public void assembleExpression(final SqlExpression expression, final StringBuilder sb,
			final int indentLevel, final int flags)
	{
		if(expression instanceof SqlxAggregateCOLLECT_asString)
		{
			sb.append(QueryPart.function(this,FUNCTION_AGG_LIST,flags,
					(Object[])((SqlxAggregateCOLLECT_asString)expression).getParameters()));
		}
		else
		{
			super.assembleExpression(expression,sb,indentLevel,flags);
		}
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSELECT(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleSELECT(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		indent(sb,indentLevel,isSingleLine(flags)).append(query.keyword());
		this.assembleSelectDISTINCT(query,sb,indentLevel,flags);
		this.assembleSelectRowLimit(query,sb,flags,clauseSeperator,newLine,indentLevel);
		this.assembleSelectItems(query,sb,flags,indentLevel,newLine);
		this.assembleSelectSqlClauses(query,sb,indentLevel,flags | ASEXPRESSION,clauseSeperator,
				newLine);
		this.assembleAppendSELECTs(query,sb,indentLevel,flags,clauseSeperator,newLine);
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleAppendSELECTs(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleAppendSELECTs(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		SELECT appendSelect = query.getUnionSelect();
		if(appendSelect != null)
		{
			this.assembleAppendSelect(appendSelect,sb,indentLevel,flags,clauseSeperator,newLine,
					UNION);
			return sb;
		}
		
		appendSelect = query.getUnionAllSelect();
		if(appendSelect != null)
		{
			this.assembleAppendSelect(appendSelect,sb,indentLevel,flags,clauseSeperator,newLine,
					UNION_ALL);
			return sb;
		}
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSelectRowLimit(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, java.lang.String, java.lang.String,
	 *      int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(final SELECT query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		final Integer top = query.getFetchFirstRowCount();
		if(top != null)
		{
			sb.append(_TOP_).append(top);
		}
		return sb;
	}
	
	/** The Constant DATE. */
	private static final String				DATE			= "yyyy-MM-dd";
	
	/** The Constant TIME. */
	private static final String				TIME			= "HH:mm:ss";
	
	/** The Constant TIMESTAMP. */
	private static final String				TIMESTAMP		= DATE + " " + TIME;
	
	/** The Constant date2TIMESTAMP. */
	private static final SimpleDateFormat	dateToTIMESTAMP	= new SimpleDateFormat(TIMESTAMP);
	
	/** The Constant date2DATE. */
	private static final SimpleDateFormat	dateToDATE		= new SimpleDateFormat(DATE);
	
	/** The Constant date2TIME. */
	private static final SimpleDateFormat	dateToTIME		= new SimpleDateFormat(TIME);
	
	
	/**
	 * @return
	 */
	@Override
	public DateFormat getDateFormatDATE()
	{
		return dateToDATE;
	}
	
	
	/**
	 * @return
	 */
	@Override
	public DateFormat getDateFormatTIME()
	{
		return dateToTIME;
	}
	
	
	/**
	 * @return
	 */
	@Override
	public DateFormat getDateFormatTIMESTAMP()
	{
		return dateToTIMESTAMP;
	}
	
	
	@Override
	public StringBuilder assembleDateTimeExpression(final SqlTimestamp dateTimeExpression,
			final StringBuilder sb)
	{
		// No preceding keyword in Cache AFAIK.
		sb.append(apo)
				.append(dateTimeExpression.getDateFormat(this).format(dateTimeExpression.getDate()))
				.append(apo);
		return sb;
	}
	
	
	@Override
	public StringBuilder assembleTableIdentifier(SqlTableIdentity table, StringBuilder sb,
			int indentLevel, int flags)
	{
		final DbmsAdaptor<?> dbms = this.getDbmsAdaptor();
		final DbmsSyntax<?> syntax = dbms.getSyntax();
		final DbmsConfiguration<?> config = dbms.getConfiguration();
		
		final SqlTableIdentity.Sql sql = table.sql();
		final String schema = sql.schema;
		final String name = sql.name;
		
		if(schema != null)
		{
			boolean delim = needsDelimiter(schema);
			if(delim)
			{
				sb.append(DELIM);
			}
			sb.append(schema);
			if(delim)
			{
				sb.append(DELIM);
			}
			sb.append(dot);
		}
		
		boolean delim = needsDelimiter(name);
		if(delim)
		{
			sb.append(DELIM);
		}
		sb.append(name);
		if(delim)
		{
			sb.append(DELIM);
		}
		
		if(!isOmitAlias(flags))
		{
			final String alias = sql.alias;
			if(alias != null && alias.length() > 0)
			{
				sb.append(_);
				if(config.isDelimitAliases() || config.isAutoEscapeReservedWords()
						&& syntax.isReservedWord(alias))
				{
					sb.append(DELIM).append(alias).append(DELIM);
				}
				else
				{
					sb.append(alias);
				}
			}
		}
		return sb;
	}
	
	
	@Override
	public StringBuilder assembleColumnQualifier(final SqlColumn column, final StringBuilder sb,
			final int flags)
	{
		final TableExpression owner = column.getOwner();
		String qualifier = getAlias(owner);
		if(qualifier == null || QueryPart.isQualifyByTable(flags))
		{
			qualifier = owner.toString();
		}
		
		if(needsDelimiter(qualifier))
		{
			qualifier = DELIM + qualifier + DELIM;
		}
		
		return sb.append(qualifier).append(dot);
	}
	
	
	private boolean needsDelimiter(String name)
	{
		if(Cache2009Dbms.SYNTAX.isKeyword(name))
		{
			return true;
		}
		
		try
		{
			SqlIdentifier.validateIdentifierString(name);
			return false;
		}
		catch(SQLEngineInvalidIdentifier e)
		{
			return true;
		}
	}
	
}
