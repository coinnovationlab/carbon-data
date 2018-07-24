package org.wso2.carbon.dataservices.core.odata.expression;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.wso2.carbon.dataservices.core.odata.CassandraUtils;

public class CassandraFilterExpressionVisitor extends FilterExpressionVisitor {

	private static final HashMap<BinaryOperatorKind, String> SUPPORTED_BIN_OPERATORS = new HashMap<BinaryOperatorKind, String>() {
		private static final long serialVersionUID = 1L;

	{
		// List of OData binary operators natively supported by Cassandra
		// Boolean operations - Edm.Boolean
        put(BinaryOperatorKind.AND, " AND ");
        // Comparison Logical operations - numeric or Edm.String
        put(BinaryOperatorKind.EQ, " = ");
        put(BinaryOperatorKind.GE, " >= ");
        put(BinaryOperatorKind.GT, " > ");
        put(BinaryOperatorKind.LE, " <= ");
        put(BinaryOperatorKind.LT, " < ");
    }};
	
	@Override
	public Object visitBinaryOperator(BinaryOperatorKind operator, Object left, Object right)
	       				throws ExpressionVisitException, ODataApplicationException {
			
		String strOperator = SUPPORTED_BIN_OPERATORS.get(operator); // retrieves a string that represents the operator
		if (strOperator == null) { // operator is not supported
			throw new ODataApplicationException("The following binary operator is currently not supported for Cassandra: " + operator.name(),
					HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
		}
		
		if (left instanceof String) 
			left = CassandraUtils.preserveCase((String) left); // Cassandra is case sensitive, yet it converts column names to lower case unless they're within double quotes
		right = CassandraUtils.oDataConversionForDBQuery(right); // adapt type from OData notation
		
		return left + strOperator + right;
	}

	@Override
	public Object visitMethodCall(MethodKind methodCall, List<Object> parameters)
			throws ExpressionVisitException, ODataApplicationException {

		switch(methodCall) {
			// List of methods natively supported by Cassandra
			case NOW:
				return "dateof(now())";
			default:
				throw new ODataApplicationException("The following method is currently not supported for Cassandra: " + methodCall.name(),
						HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
		}
	}
}
