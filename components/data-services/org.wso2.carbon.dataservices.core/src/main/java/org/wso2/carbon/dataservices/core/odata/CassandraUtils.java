package org.wso2.carbon.dataservices.core.odata;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class CassandraUtils {
	
	private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"; // to display timestamps in Cassandra
	public static final SimpleDateFormat SDF = new SimpleDateFormat(DATE_FORMAT, Locale.getDefault()); // format to present dates consistently between Cassandra and OData
	private static final String PREFIX_BINARY = "binary'"; // this prefix is used by OData to signal binary data; it's the OData equivalent of Cassandra's blob type
	
	/**
	* Converts OData notation of certain types to a notation compatible with Cassandra's CQL
	*
	* @param obj	The notation (most commonly a String) to convert
	* @return		An object (most commonly a String) representing a notation fit for Cassandra
	*/
	public static Object oDataConversionForDBQuery(Object obj) {
		Object res = obj;
		
		if (obj instanceof String) {
			String s = (String) obj;
			
			if (s.startsWith(PREFIX_BINARY)) { // blob
				res = "0x" + s.substring(PREFIX_BINARY.length(), s.length()-1); // remove the word binary and the quotes, add 0x at the beginning
			} else if (s.charAt(0) > '0' && s.charAt(0) <= '9' && s.length() >= 5 && s.charAt(4) == '-') { // date
				Date date = CassandraUtils.SDF.parse(s, new ParsePosition(0)); // converts String to Date
				res = date.getTime(); // converts to milliseconds
			} else if (s.charAt(0) >= '0' && s.charAt(0) <= '2' && s.length() >= 3 && s.charAt(2) == ':') { // time of day
				res = "'" + s + "'"; // cannot convert this to milliseconds, as the comparison would fail, instead quotes are needed
			}
		}
		return res;
	}
	
	/**
	* Cassandra is case sensitive, yet it converts column names to lower case unless they're within double quotes.
	* This method ensures that the string is within double quotes, so that its case will be preserved.
	*
	* @param	s	The case-sensitive string
	* @return		The string within double quotes, if they weren't present already
	*/
	public static String preserveCase(String s) {
		if (!s.startsWith("\"")) // checks if double quotes are already present
			s = "\"" + s + "\""; // adds double quotes
		return s;
	}
}