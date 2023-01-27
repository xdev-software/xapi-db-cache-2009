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

import com.xdev.jadoth.sqlengine.dbms.DbmsSyntax;


/**
 * @author TM
 * 
 */
public class Cache2009Syntax extends DbmsSyntax.Implementation<Cache2009Dbms>
{
	protected Cache2009Syntax()
	{
		super(wordSet(
				"%AFTERHAVING","%ALPHAUP","%ALTER","%ALTER_USER","%BEGTRANS","%CHECKPRIV",
				"%CREATE_ROLE","%CREATE_USER","%DBUGFULL","%DELDATA","%DESCRIPTION",
				"%DROP_ANY_ROLE","%DROP_USER","%EXACT","%EXTERNAL","%FILE","%FOREACH","%FULL",
				"%GRANT_ANY_PRIVILEGE","%GRANT_ANY_ROLE","%INORDER","%INTERNAL","%INTEXT",
				"%INTRANS","%INTRANSACTION","%MCODE","%NOCHECK","%NODELDATA","%NOINDEX","%NOLOCK",
				"%NOTRIGGER","%NUMROWS","%ODBCOUT","%ROUTINE","%ROWCOUNT","%STARTSWITH","%STRING",
				"%THRESHOLD","%UPPER","ABSOLUTE","ADD","ALL","ALLOCATE","ALTER","AND","ANY","ARE",
				"AS","ASC","ASSERTION","AT","AUTHORIZATION","AVG","BEGIN","BETWEEN","BIT",
				"BIT_LENGTH","BOTH","BY","CASCADE","CASE","CAST","CATALOG","CHAR","CHARACTER",
				"CHARACTER_LENGTH","CHAR_LENGTH","CHECK","CLOSE","COALESCE","COBOL","COLLATE",
				"COLLATION","COLUMN","COMMIT","CONNECT","CONNECTION","CONSTRAINT","CONSTRAINTS",
				"CONTINUE","CONVERT","CORRESPONDING","COUNT","CREATE","CROSS","CURRENT",
				"CURRENT_DATE","CURRENT_TIME","CURRENT_TIMESTAMP","CURRENT_USER","CURSOR","DATE",
				"DAY","DEALLOCATE","DEC","DECIMAL","DECLARE","DEFAULT","DEFERRABLE","DEFERRED",
				"DELETE","DESC","DESCRIBE","DESCRIPTOR","DIAGNOSTICS","DISCONNECT","DISTINCT",
				"DOMAIN","DOUBLE","DROP","ELSE","END","ENDEXEC","ESCAPE","EXCEPT","EXCEPTION",
				"EXEC","EXECUTE","EXISTS","EXTERNAL","EXTRACT","FALSE","FETCH","FILE","FIRST",
				"FLOAT","FOR","FOREIGN","FORTRAN","FOUND","FROM","FULL","GET","GLOBAL","GO","GOTO",
				"GRANT","GROUP","HAVING","HOUR","IDENTITY","IMMEDIATE","IN","INDICATOR",
				"INITIALLY","INNER","INPUT","INSENSITIVE","INSERT","INT","INTEGER","INTERSECT",
				"INTERVAL","INTO","IS","ISOLATION","JOIN","KEY","LANGUAGE","LAST","LEADING","LEFT",
				"LEVEL","LIKE","LOCAL","LOWER","MATCH","MAX","MIN","MINUTE","MODULE","MONTH",
				"NAMES","NATIONAL","NATURAL","NCHAR","NEXT","NO","NOT","NULL","NULLIF","NUMERIC",
				"OCTET_LENGTH","OF","ON","ONLY","OPEN","OPTION","OR","ORDER","OUTER","OUTPUT",
				"OVERLAPS","PAD","PARTIAL","PASCAL","PLI","PRECISION","PREPARE","PRESERVE",
				"PRIMARY","PRIOR","PRIVILEGES","PROCEDURE","PUBLIC","READ","REAL","REFERENCES",
				"RELATIVE","RESTRICT","REVOKE","RIGHT","ROLE","ROLLBACK","ROWS","SCHEMA","SCROLL",
				"SECOND","SECTION","SELECT","SESSION_USER","SET","SIZE","SMALLINT","SOME","SPACE",
				"SQL","SQLCODE","SQLERROR","SQLSTATE","STATISTICS","SUBSTRING","SUM","SYSDATE",
				"SYSTEM_USER","TABLE","TEMPORARY","THEN","TIME","TIMESTAMP","TIMEZONE_HOUR",
				"TIMEZONE_MINUTE","TO","TOP","TRAILING","TRANSACTION","TRANSLATE","TRANSLATION",
				"TRIM","TRUE","UNION","UNIQUE","UNKNOWN","UPDATE","UPPER","USAGE","USER","USING",
				"VALUES","VARCHAR","VARYING","VIEW","WHEN","WHENEVER","WHERE","WITH","WORK",
				"WRITE","YEAR","ZONE"),wordSet(
		// none afaik
				));
	}
	
}
