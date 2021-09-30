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


import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class Cache2009JDBCDataSource extends JDBCDataSource<Cache2009JDBCDataSource, Cache2009Dbms>
{
	public Cache2009JDBCDataSource()
	{
		super(new Cache2009Dbms());
	}
	
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{HOST.clone(),PORT.clone(1972),USERNAME.clone("_SYSTEM "),
				PASSWORD.clone(),CATALOG.clone("samples"),URL_EXTENSION.clone(),
				IS_SERVER_DATASOURCE.clone(),SERVER_URL.clone(),AUTH_KEY.clone()};
	}
	
	
	@Override
	protected Cache2009ConnectionInformation getConnectionInformation()
	{
		return new Cache2009ConnectionInformation(getHost(),getPort(),getUserName(),getPassword()
				.getPlainText(),getCatalog(),getUrlExtension(),getDbmsAdaptor());
	}
	
	
	@Override
	public Cache2009JDBCConnection openConnectionImpl() throws DBException
	{
		return new Cache2009JDBCConnection(this);
	}
	
	
	@Override
	public Cache2009JDBCMetaData getMetaData() throws DBException
	{
		return new Cache2009JDBCMetaData(this);
	}
	
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
