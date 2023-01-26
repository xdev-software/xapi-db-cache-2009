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

import static com.xdev.jadoth.sqlengine.SQL.LANG.ORDER;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.NEW_LINE;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;

import com.xdev.jadoth.sqlengine.internal.SqlCondition;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger;


/**
 * The Class Cache2009Trigger.
 * 
 * @author Thomas Muenz
 */
public class Cache2009Trigger extends SqlTrigger
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	/** The Constant LANGUAGE. */
	protected static final String	LANGUAGE	= "LANGUAGE";
	
	// /////////////////////////////////////////////////////////////////////////
	// instance fields //
	// ///////////////////
	/** The order. */
	private Integer					order		= null;
	
	/** The language. */
	private Language				language	= Language.SQL;
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	/**
	 * Instantiates a new cache2009 trigger.
	 * 
	 * @param name
	 *            the name
	 * @param time
	 *            the time
	 * @param event
	 *            the event
	 * @param table
	 *            the table
	 * @param action
	 *            the action
	 */
	public Cache2009Trigger(final String name, final Time time, final Event event,
			final SqlTableIdentity table, final Object action)
	{
		super(name,time,event,table,action);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	/**
	 * Gets the order.
	 * 
	 * @return the order
	 */
	public Integer getOrder()
	{
		return this.order;
	}
	
	
	/**
	 * Gets the language.
	 * 
	 * @return the language
	 */
	public Language getLanguage()
	{
		return this.language;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	/**
	 * Sets the order.
	 * 
	 * @param order
	 *            the new order
	 */
	public void setOrder(final Integer order)
	{
		this.order = order;
	}
	
	
	/**
	 * Sets the language.
	 * 
	 * @param language
	 *            the new language
	 */
	public void setLanguage(final Language language)
	{
		this.language = language;
	}
	
	
	/**
	 * ORDER.
	 * 
	 * @param order
	 *            the order
	 * @return the cache2009 trigger
	 */
	public Cache2009Trigger ORDER(final Integer order)
	{
		setOrder(order);
		return this;
	}
	
	
	/**
	 * LANGUAGE.
	 * 
	 * @param language
	 *            the language
	 * @return the cache2009 trigger
	 */
	public Cache2009Trigger LANGUAGE(final Language language)
	{
		setLanguage(language);
		return this;
	}
	
	
	/**
	 * Assemble order.
	 * 
	 * @param sb
	 *            the sb
	 * @return the string builder
	 */
	protected StringBuilder assembleOrder(final StringBuilder sb)
	{
		if(this.order != null)
		{
			sb.append(ORDER).append(_).append(this.order).append(NEW_LINE);
		}
		return sb;
	}
	
	
	/**
	 * Assemble language.
	 * 
	 * @param sb
	 *            the sb
	 * @return the string builder
	 */
	protected StringBuilder assembleLanguage(final StringBuilder sb)
	{
		sb.append(LANGUAGE).append(_).append(this.language).append(NEW_LINE);
		return sb;
	}
	
	
	/**
	 * This is only used for Cach� Object Script action code. SQL code is
	 * assembled via super class method.
	 * 
	 * @param sb
	 *            the sb
	 * @return the string builder
	 */
	@Override
	protected StringBuilder assemblePartActions(final StringBuilder sb)
	{
		assembleFOR_EACH(sb);
		assembleWhen(sb);
		assembleLanguage(sb);
		
		if(this.language == Language.SQL)
		{

			super.assembleTriggerCode(sb);
		}
		else
		{
			this.assembleTriggerCode(sb);
		}
		
		return sb;
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger#getActionStatementSeperator()
	 */
	@Override
	protected String getActionStatementSeperator()
	{
		return this.language == Language.SQL ? super.getActionStatementSeperator() : NEW_LINE;
	}
	
	
	/**
	 * @param sb
	 * @return
	 * @see com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger#assembleTriggerCode(java.lang.StringBuilder)
	 */
	@Override
	protected StringBuilder assembleTriggerCode(final StringBuilder sb)
	{
		sb.append("{").append(NEW_LINE);
		assembleActionStatements(sb).append(NEW_LINE);
		sb.append("}");
		return sb;
	}
	
	
	/**
	 * @param sb
	 * @return
	 * @see com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger#assemblePartON(java.lang.StringBuilder)
	 */
	@Override
	protected StringBuilder assemblePartON(final StringBuilder sb)
	{
		assembleONtable(sb);
		if(this.language == Language.SQL)
		{
			assembleREFERENCING(sb);
		}
		sb.append(NEW_LINE);
		return sb;
	}
	
	
	/**
	 * @param sb
	 * @return
	 * @see com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger#assembleString(java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleString(StringBuilder sb)
	{
		if(sb == null)
		{
			sb = new StringBuilder(2048);
		}
		assemblePartCREATE(sb);
		assembleOrder(sb);
		assemblePartON(sb);
		assemblePartActions(sb);
		return sb;
	}
	
	
	/**
	 * @param when
	 * @return
	 * @see com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger#WHEN(com.xdev.jadoth.sqlengine.internal.SqlCondition)
	 */
	@Override
	public Cache2009Trigger WHEN(final SqlCondition when)
	{
		setWhen(when);
		return this;
	}
	
	
	/**
	 * @param oldRowAlias
	 * @return
	 * @see com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger#OLD_ROW_AS(java.lang.String)
	 */
	@Override
	public Cache2009Trigger OLD_ROW_AS(final String oldRowAlias)
	{
		setOldRowAlias(oldRowAlias);
		return this;
	}
	
	
	/**
	 * @param newRowAlias
	 * @return
	 * @see com.xdev.jadoth.sqlengine.internal.tables.SqlTrigger#NEW_ROW_AS(java.lang.String)
	 */
	@Override
	public Cache2009Trigger NEW_ROW_AS(final String newRowAlias)
	{
		setNewRowAlias(newRowAlias);
		return this;
	}
	
	
	
	/**
	 * The Enum Language.
	 */
	public static enum Language
	{
		
		/** The SQL. */
		SQL("sql"),
		
		/** The OBJECTSCRIPT. */
		OBJECTSCRIPT("cache");
		
		/** The dictionary string. */
		final private String	dictionaryString;
		
		
		/**
		 * Instantiates a new language.
		 * 
		 * @param dictionaryString
		 *            the dictionary string
		 */
		private Language(final String dictionaryString)
		{
			this.dictionaryString = dictionaryString;
		}
		
		
		/**
		 * To dictionary string.
		 * 
		 * @return the string
		 */
		public String toDictionaryString()
		{
			return this.dictionaryString;
		}
		
	}	
}
