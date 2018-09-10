package org.wso2.carbon.dataservices.core.odata;

import java.util.ArrayList;
import java.util.List;

/**
 * Class used by RDBMSODataQuery, to represent the GROUP BY part of the query.
 * This class does not support ROLLUP and CUBE options, as they are currently unnecessary for OData.
 * In the future, it may be necessary to alter this class or, if it becomes too complicated, replace its occurrences with a more simple String type.
 */
public class RDBMSODataGroupBy {
	private List<String> groupBy; // List of columns to group by
	private String having; // HAVING clause
	
	public RDBMSODataGroupBy() {
		groupBy = null;
		having = null;
	}
	
	public RDBMSODataGroupBy(List<String> groupBy, String having) {
		this.groupBy = groupBy;
		this.having = having;
	}
	
	/**
	 * Returns the list of columns to group by.
	 * 
	 * @return	The list of columns to group by
	 */
	public List<String> getGroupByList() {
		return groupBy;
	}
	
	/**
	 * Add a column to group by.
	 * 
	 * @param column	The column to add
	 */
	public void addGroupBy(String column) {
		if (groupBy == null)
			groupBy = new ArrayList<String>();
		groupBy.add(column);
	}
	
	/**
	 * Allows to redefine all at once the list of columns to group by. Call with null (or an empty list) to empty it.
	 * 
	 * @param groupBY	The list of columns that make up the GROUP BY
	 */
	public void setGroupByList(List<String> groupBy) {
		this.groupBy = groupBy;
	}
	
	/**
	 * Returns the HAVING clause.
	 * 
	 * @return	The HAVING clause
	 */
	public String getHaving() {
		return having;
	}
	
	/**
	 * Sets the string that represents the HAVING clause.
	 * 
	 * @param having	The HAVING clause to set
	 */
	public void setHaving(String having) {
		this.having = having;
	}
	
	/**
	 * Returns a string containing the GROUP BY part of the query.
	 * 
	 * @return	A string containing the GROUP BY part of the query.
	 */
	public String toString() {
		String result = "";
		if (groupBy == null || groupBy.size() == 0)
			return result;
		
		result = " GROUP BY ";
		result += String.join(",", groupBy);
		if (having != null && !having.equals(""))
			result += " HAVING " + having;
		
		return result;
	}
}
