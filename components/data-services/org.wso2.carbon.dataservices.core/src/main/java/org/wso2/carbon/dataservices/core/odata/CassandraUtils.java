package org.wso2.carbon.dataservices.core.odata;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

import org.apache.axis2.databinding.types.Time;
import org.apache.commons.codec.binary.Hex;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.wso2.carbon.dataservices.core.odata.expression.operand.TypedOperand;
import org.wso2.carbon.dataservices.core.odata.expression.operand.VisitorOperand;

public class CassandraUtils {
	
	private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"; // to display timestamps in Cassandra
	public static final SimpleDateFormat SDF = new SimpleDateFormat(DATE_FORMAT, Locale.getDefault()); // format to present dates consistently between Cassandra and OData
	private static final String PREFIX_BINARY = "binary'"; // this prefix is used by OData to signal binary data; it's the OData equivalent of Cassandra's blob type
	
	private static final HashMap<BinaryOperatorKind, String> SUPPORTED_BIN_OPERATORS = new HashMap<BinaryOperatorKind, String>() {{
		// List of OData binary operators natively supported by Cassandra
		// Boolean operations - Edm.Boolean
        put(BinaryOperatorKind.AND, " AND ");
        // Comparison Logical operations - numeric or Edm.String
        put(BinaryOperatorKind.EQ, " = ");
        put(BinaryOperatorKind.GE, " >= ");
        put(BinaryOperatorKind.GT, " > ");
        put(BinaryOperatorKind.LE, " =< ");
        put(BinaryOperatorKind.LT, " < ");    
    }};
	
    /**
     * Converts Cassandra notation of certain types to a notation suitable for comparisons
     *
     * @param vo	The notation to convert
     * @return		A representation using a notation suitable for comparisons
     */
	public static VisitorOperand cassandraConversion(VisitorOperand vo) {
		VisitorOperand res = vo;
		Object voValue = vo.getValue();
		if (voValue instanceof byte[]) { // blob, byte array needs to be converted to Hex string
			String toHex = Hex.encodeHexString((byte[]) voValue);
			res = new TypedOperand(toHex, ODataConstants.primitiveString);
		} else if (voValue instanceof Calendar) { // date or time of day
			Date date = ((Calendar) voValue).getTime(); // converts Calendar to Date
			res = new TypedOperand(date.getTime(), ODataConstants.primitiveInt64); // converts to milliseconds for easy comparisons
		}
		return res;
	}
	
	/**
     * Converts OData notation of certain types to a notation suitable for comparisons
     *
     * @param vo	The notation to convert
     * @return		A representation using a notation suitable for comparisons
     */
	public static VisitorOperand oDataConversion(VisitorOperand vo) {
		VisitorOperand res = vo;
		Object voValue = vo.getValue();
		
		if (voValue instanceof String) {
			String s = (String) voValue;
			
			if (s.startsWith(PREFIX_BINARY)) { // blob, OData encloses the string within the word binary and quotes, for example: binary'50ab'
				s = s.substring(PREFIX_BINARY.length(), s.length()-1); // remove the word binary and the quotes
				s = s.toLowerCase(); // convert to lower case
				res = new TypedOperand(s, ODataConstants.primitiveString);
			} else if (s.charAt(0) > '0' && s.charAt(0) <= '9' && s.contains("-")) { // date
				Date date = CassandraUtils.SDF.parse(s, new ParsePosition(0)); // converts String to Date
				res = new TypedOperand(date.getTime(), ODataConstants.primitiveInt64); // converts to milliseconds
			} else if (s.charAt(0) >= '0' && s.charAt(0) <= '2' && s.contains(":")) { // time of day
				Date date = new Time(s).getAsCalendar().getTime(); // converts String to Date
				res = new TypedOperand(date.getTime(), ODataConstants.primitiveInt64); // converts to milliseconds for easy comparisons
			}
		}
		return res;
	}
	
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
			} else if (s.charAt(0) > '0' && s.charAt(0) <= '9' && s.contains("-")) { // date
				Date date = CassandraUtils.SDF.parse(s, new ParsePosition(0)); // converts String to Date
				res = date.getTime(); // converts to milliseconds
			} else if (s.charAt(0) >= '0' && s.charAt(0) <= '2' && s.contains(":")) { // time of day
				res = "'" + s + "'"; // cannot convert this to milliseconds, as the comparison would fail, instead quotes are needed
			}
		}
		return res;
	}
	
	/**
     * Returns a string representing the requested operator if it is supported, null otherwise
     *
     * @param operator	The operator to retrieve
     * @return			The string representing the operator if supported, otherwise null
     */
	public static String getBinOperator(BinaryOperatorKind operator) {
		return SUPPORTED_BIN_OPERATORS.get(operator); // operator is returned if supported, otherwise null
	}
	
}