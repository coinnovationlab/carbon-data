package org.wso2.carbon.dataservices.core.odata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class makes it easy for multiple actors to alter the same query, while keeping the query readable
 */
public class RDBMSODataQuery {
	public static final String MYSQL_MAX_LIMIT = "18446744073709551615"; // Necessary for MySQL, which does not allow OFFSET without LIMIT
	public static final String H2_MAX_LIMIT = "-1"; // Same reason as above but for H2, which interprets -1 as no limit
	public static final String WHERESEPARATOR = "'#WhereSubQuerySeparator#'"; // H2 and MSSQL do not support multi-column IN sub-queries, so we have to concatenate the PK columns
	
	private String dbType;
	private List<String> select;
	private String from;
	private String where;
	private RDBMSODataGroupBy groupBy;
	private List<String> orderBy;
	private int limit;
	private int offset;
	
	public RDBMSODataQuery() {
		dbType = "";
		select = null;
		from = "";
		where = "";
		groupBy = null;
		orderBy = null;
		limit = -1;
		offset = 0;
	}
	
	public RDBMSODataQuery(String dbType, List<String> select, String from, String where, RDBMSODataGroupBy groupBy, List<String> orderBy, int limit, int offset) {
		this.dbType = dbType;
		this.select = select;
		this.from = from;
		this.where = where;
		this.groupBy = groupBy;
		this.orderBy = orderBy;
		this.limit = limit;
		this.offset = offset;
	}
	
	public RDBMSODataQuery(RDBMSODataQuery toClone) { // copy constructor
		if (toClone == null) {
			new RDBMSODataQuery();
			return;
		}
		
		dbType = toClone.getDBType();
		select = new ArrayList<String>(toClone.getSelect());
		from = toClone.getFrom();
		where = toClone.getWhere();
		groupBy = new RDBMSODataGroupBy();
		RDBMSODataGroupBy toCloneGroupBy = toClone.getGroupBy();
		if (toCloneGroupBy != null) {
			groupBy.setGroupByList(new ArrayList<String>(toCloneGroupBy.getGroupByList()));
			groupBy.setHaving(toCloneGroupBy.getHaving());
		}
		orderBy = new ArrayList<String>(toClone.getOrderBy());
		limit = toClone.getLimit();
		offset = toClone.getOffset();
	}
	
	/**
	 * Returns the database type. The database type determines the syntax for ORDER BY, LIMIT and OFFSET
	 * 
	 * @return		Database type
	 */
	public String getDBType() {
		return dbType;
	}
	
	/**
	 * Sets the database type.
	 * 
	 * @param dbType	Database type
	 */
	public void setDBType(String dbType) {
		this.dbType = dbType;
	}
	
	/**
	 * Returns the list of columns to select. As they are listed as strings, they make include AS specifications.
	 * 
	 * @return		The list of columns to SELECT
	 */
	public List<String> getSelect() {
		return select;
	}
	
	/**
	 * Add a column to be selected.
	 * 
	 * @param s		The column to add
	 */
	public void addSelect(String s) {
		if (select == null)
			select = new ArrayList<String>();
		select.add(s);
	}
	
	/**
	 * Allows to redefine all at once the list of columns to select. Call with null (or an empty list) to empty it.
	 * 
	 * @param select	The list of columns that make up the SELECT
	 */
	public void setSelect(List<String> select) {
		this.select = select;
	}
	
	/**
	 * Returns a string containing the SELECT part of the query.
	 * 
	 * @return		A string containing the SELECT part of the query.
	 */
	public String printSelect() {
		String result = "";
		if (select != null && select.size() > 0) {
			result += "SELECT ";
			if (select.contains(WHERESEPARATOR)) // H2 and MSSQL do not support multi-column IN sub-queries, so we have to concatenate the PK columns
				result += "CONCAT(";
			Iterator<String> iter = select.iterator();
			while (iter.hasNext()) {
				result += iter.next();
				if (iter.hasNext())
					result += ",";
			}
		}
		if (!result.equals("") && select.contains(WHERESEPARATOR)) // H2 and MSSQL do not support multi-column IN sub-queries, so we have to concatenate the PK columns
			result += ")";
		return result;
	}
	
	/**
	 * Returns the tables (and possible joins) involved in the query. Since it may contain JOIN, ON and AND commands, a simple String type is used.
	 * 
	 * @return		A string containing the tables involved in the query.
	 */
	public String getFrom() {
		return from;
	}
	
	/**
	 * Appends a string to the FROM part.
	 * 
	 * @param s		String to append
	 */
	public void appendFrom(String s) {
		from += s;
	}
	
	/**
	 * Sets the string that defines the tables involved in the query.
	 * 
	 * @param s		The string that defines the tables involved
	 */
	public void setFrom(String s) {
		from = s;
	}
	
	/**
	 * Returns a string containing the FROM part of the query.
	 * 
	 * @return		A string containing the FROM part of the query.
	 */
	public String printFrom() {
		String result = "";
		if (from != null && !from.equals(""))
			result = " FROM " + from;
		return result;
	}
	
	/**
	 * Returns the filters applied to the query. Since many modifiers may be involved (AND, OR, and so on), a simple String type is used.
	 * 
	 * @return		A string containing the filters.
	 */
	public String getWhere() {
		return where;
	}
	
	/**
	 * Appends a string to the WHERE part.
	 * 
	 * @param s		String to append
	 */
	public void appendWhere(String s) {
		where += s;
	}
	
	
	/**
	 * Sets the string that contains the filters to apply to the query.
	 * 
	 * @param s		The string that contains the filters to apply
	 */
	public void setWhere(String s) {
		where = s;
	}
	
	/**
	 * Returns a string containing the WHERE part of the query.
	 * 
	 * @return		A string containing the WHERE part of the query
	 */
	public String printWhere() {
		String result = "";
		if (where != null && !where.equals(""))
			result = " WHERE " + where;
		return result;
	}
	
	/**
	 * Returns the list of arguments to group by. Custom class RDBMSGroupBy is used, in case the arguments contain HAVING clauses.
	 * Note this class does not support ROLLUP and CUBE options, as they are currently unnecessary for OData.
	 * In the future, it may be necessary to alter this class or, if it becomes too complicated, replace its occurrences with a more simple String type.
	 * 
	 * @return		The list of arguments to group by
	 */
	public RDBMSODataGroupBy getGroupBy() {
		return groupBy;
	}
	
	/**
	 * Adds an element to group by.
	 * 
	 * @param column	The element to be added
	 */
	public void addGroupByColumn(String column) {
		if (groupBy == null)
			groupBy = new RDBMSODataGroupBy();
		groupBy.addGroupBy(column);
	}
	
	/**
	 * Allows to redefine all at once the list of arguments to group by. Call with null (or an empty list) to remove the GROUP BY option.
	 * 
	 * @param groupBy	Columns that make up the GROUP BY
	 */
	public void setGroupBy(RDBMSODataGroupBy groupBy) {
		this.groupBy = groupBy;
	}
	
	/**
	 * Sets the HAVING clause of the GROUP BY.
	 * 
	 * @param having	The having clause
	 */
	public void setGroupByHaving(String having) {
		if (groupBy == null)
			groupBy = new RDBMSODataGroupBy();
		groupBy.setHaving(having);
	}
	
	/**
	 * Returns a string containing the GROUP BY part of the query.
	 * 
	 * @return		A string containing the GROUP BY part of the query
	 */
	public String printGroupBy() {
		String result = "";
		if (groupBy != null)
			result = groupBy.toString();
		return result;
	}
	
	/**
	 * Returns the list of arguments to order by. Elements may specify ASC or DESC.
	 * 
	 * @return		The list of arguments to order by
	 */
	public List<String> getOrderBy() {
		return orderBy;
	}
	
	/**
	 * Adds an element to order by.
	 * 
	 * @param s		The element to be added.
	 */
	public void addOrderBy(String s) {
		if (orderBy == null)
			orderBy = new ArrayList<String>();
		orderBy.add(s);
	}
	
	/**
	 * Allows to redefine all at once the list of arguments to order by. Call with null (or an empty list) to empty it.
	 * 
	 * @param orderBy	The list of columns that make up the ORDER BY
	 */
	public void setOrderBy(List<String> orderBy) {
		this.orderBy = orderBy;
	}
	
	/**
	 * Returns a string containing the ORDER BY part of the query.
	 * 
	 * @return		A string containing the ORDER BY part of the query
	 */
	public String printOrderBy() {
		String result = "";
		if (orderBy != null && orderBy.size() > 0) {
			result += " ORDER BY ";
			Iterator<String> iter = orderBy.iterator();
			while (iter.hasNext()) {
				result += iter.next();
				if (iter.hasNext())
					result += ",";
			}
		}
		
		if (dbType.equalsIgnoreCase(RDBMSDataHandler.MSSQL_SERVER)) {
			if(result.equals("") && (offset != 0 || limit > 0)) {
				result = " ORDER BY (SELECT 1) ";
			}
		}
		return result;
	}
	
	/**
	 * Returns the LIMIT to apply.
	 * 
	 * @return		The LIMIT to apply
	 */
	public int getLimit() {
		return limit;
	}
	
	/**
	 * Sets the LIMIT to apply.
	 * 
	 * @param limit		The LIMIT to apply
	 */
	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	/** 
	 * Returns a string containing the LIMIT part of the query.
	 * 
	 * @return		The LIMIT part of the query
	 */
	public String printLimit() {
		String result = "";
		
		switch (dbType.toLowerCase()) {
			case (RDBMSDataHandler.ORACLE_SERVER):
				if (limit > 0) {
					result = " FETCH FIRST " + limit + " ROWS ONLY ";
					if (offset != 0)
						result = " FETCH NEXT " + limit + " ROWS ONLY ";
				}
				break;
			case (RDBMSDataHandler.MSSQL_SERVER):
				if (limit > 0)
					result = " FETCH NEXT " + limit + " ROWS ONLY ";
				break;
			case (RDBMSDataHandler.POSTGRESQL): // fall through
			case (RDBMSDataHandler.MYSQL): // fall through
			default:
				if (limit >= 0)
					result = " LIMIT " + limit;
				else if (offset != 0) {
					if (dbType.contains(RDBMSDataHandler.MYSQL))
						result = " LIMIT " + MYSQL_MAX_LIMIT;
					else if (dbType.contains(RDBMSDataHandler.H2))
						result = " LIMIT " + H2_MAX_LIMIT;
				}
		}
		return result;
	}
	
	/**
	 * Returns the OFFSET to apply.
	 * 
	 * @return		The OFFSET to apply
	 */
	public int getOffset() {
		return offset;
	}
	
	/**
	 * Sets the OFFSET to apply.
	 * 
	 * @param offset	The OFFSET to apply
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	/** 
	 * Returns a string containing the OFFSET part of the query.
	 * 
	 * @return 		The OFFSET part of the query
	 */
	public String printOffset() {
		String result = "";
		switch (dbType.toLowerCase()) {
		case (RDBMSDataHandler.ORACLE_SERVER):
			if (offset != 0)
				result = " OFFSET " + offset + " ROWS";
			break;
		case (RDBMSDataHandler.MSSQL_SERVER):
			if (offset != 0 || limit > 0 || (orderBy != null && orderBy.size() > 0))
				result = " OFFSET " + offset + " ROWS";
			break;
		case (RDBMSDataHandler.POSTGRESQL): // fall through
		case (RDBMSDataHandler.MYSQL): // fall through
		default:
			if (offset != 0)
				result = " OFFSET " + offset;
		}
		return result;
	}
	
	/**
	 * Prints LIMIT and OFFSET together. Necessary because different database types have different syntax involving these two.
	 * 
	 * @return A string containing the LIMIT and OFFSET clauses combined depending on the database type.
	 */
	public String printLimitAndOffset() {
		String result = "";
		switch (dbType.toLowerCase()) {
			case (RDBMSDataHandler.ORACLE_SERVER): // fall through
			case (RDBMSDataHandler.MSSQL_SERVER): // executes printOffset first, then printLimit
				result += printOffset();
				result += printLimit();
				break;
			case (RDBMSDataHandler.POSTGRESQL): // fall through
			case (RDBMSDataHandler.MYSQL): // fall through
			default: // executes printLimit first, then printOffset
				result += printLimit();
				result += printOffset();
		}
		return result;
	}
	
	
	/**
	 * Returns a string containing the whole query, currently in MySQL notation.
	 * 
	 * @return		A string containing the whole query
	 */
	@Override
	public String toString() {
		String result = "";
		
		// SELECT
		result += printSelect();
		// FROM
		result += printFrom();
		// WHERE
		result += printWhere();
		// GROUP BY
		result += printGroupBy();
		// ORDER BY
		result += printOrderBy();
		// LIMIT and OFFSET
		result += printLimitAndOffset();
		
		return result;
	}
}