package org.wso2.carbon.dataservices.core.odata.expression;

import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.wso2.carbon.dataservices.core.odata.CassandraUtils;

public class CassandraDBFilterExpressionVisitor extends FilterExpressionVisitor {

	@Override
	public Object visitBinaryOperator(BinaryOperatorKind operator, Object left, Object right)
	       				throws ExpressionVisitException, ODataApplicationException {
			
		String strOperator = CassandraUtils.getBinOperator(operator);
		if (strOperator == null) { // operator is not supported
			throw new ODataApplicationException("The following binary operator is currently not supported for Cassandra: " + operator.name(),
					HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
		}
		
		if (left instanceof String && !((String) left).startsWith("\"")) // In some queries (e.g. WHERE conditions with AND), this check is needed to avoid inserting erroneous quotes 
        	left = "\"" + left + "\""; // Cassandra is case sensitive, yet it converts column names to lower case unless they're within double quotes
		right = CassandraUtils.oDataConversionForDBQuery(right); // adapt type from OData notation
		
		return left + strOperator + right;
	}

	@Override
	public Object visitMethodCall(MethodKind methodCall, List<Object> parameters)
			throws ExpressionVisitException, ODataApplicationException {

		switch(methodCall) {
			// currently, no OData method is natively supported by Cassandra...
			default:
				throw new ODataApplicationException("The following method is currently not supported for Cassandra: " + methodCall.name(),
						HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
		}
	}
}
