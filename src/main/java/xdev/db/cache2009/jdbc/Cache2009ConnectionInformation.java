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

import xdev.db.ConnectionInformation;


/**
 * The Class Cache2009ConnectionInformation.
 */
public class Cache2009ConnectionInformation extends ConnectionInformation<Cache2009Dbms>
{
	/**
	 * Instantiates a new cache2009 connection information.
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
	 * @param urlExtension
	 *            the extended url properties
	 * @param dbmsAdaptor
	 *            the dbms adaptor
	 */
	public Cache2009ConnectionInformation(final String host, final int port, final String user,
			final String password, final String namespace, final String urlExtension,
			final Cache2009Dbms dbmsAdaptor)
	{
		super(host,port,user,password,namespace,urlExtension,dbmsAdaptor);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	/**
	 * Gets the namespace.
	 * 
	 * @return the namespace
	 */
	public String getNamespace()
	{
		return this.getCatalog();
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	/**
	 * Sets the namespace.
	 * 
	 * @param namespace
	 *            the namespace to set
	 */
	public void setNamespace(final String namespace)
	{
		this.setCatalog(namespace);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#createJdbcConnectionUrl(java.lang.String)
	 */
	@Override
	public String createJdbcConnectionUrl()
	{
		String url = "jdbc:Cache://" + this.getHost() + ":" + this.getPort() + "/"
				+ this.getNamespace();
		return appendUrlExtension(url);
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#getJdbcDriverClassName()
	 */
	@Override
	public String getJdbcDriverClassName()
	{
		return "com.intersys.jdbc.CacheDriver";
	}
	
}
