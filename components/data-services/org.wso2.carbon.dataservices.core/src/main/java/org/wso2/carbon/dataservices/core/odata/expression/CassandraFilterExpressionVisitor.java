package org.wso2.carbon.dataservices.core.odata.expression;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmBindingTarget;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.wso2.carbon.dataservices.core.odata.expression.operand.VisitorOperand;
import org.wso2.carbon.dataservices.core.odata.CassandraUtils;

public class CassandraFilterExpressionVisitor extends ExpressionVisitorImpl {
	public CassandraFilterExpressionVisitor(final Entity entity, final EdmBindingTarget bindingTarget) {
        super(entity, bindingTarget);
    }
	
	@Override
    public VisitorOperand visitBinaryOperator(final BinaryOperatorKind operator, final VisitorOperand left,
                                              final VisitorOperand right)
            throws ExpressionVisitException, ODataApplicationException {
		
		VisitorOperand convertedLeft = CassandraUtils.cassandraConversion(left); // adapt type from Cassandra notation
		VisitorOperand convertedRight = CassandraUtils.oDataConversion(right); // adapt type from OData notation

		System.out.println("Row to compare: " + convertedLeft.getValue());
	    System.out.println("Filter: " + convertedRight.getValue());
	    
		return super.visitBinaryOperator(operator, convertedLeft, convertedRight);
	}
	
	
}
/*public class CassandraFilterExpressionVisitor extends FilterExpressionVisitor{
	private static final HashMap<BinaryOperatorKind, String> BINARY_OPERATORS = new HashMap<BinaryOperatorKind, String>() {{
		
		// Boolean operations - Edm.Boolean
		put(BinaryOperatorKind.OR, " OR ");
        put(BinaryOperatorKind.AND, " AND ");
        //Arithmetic operations
        put(BinaryOperatorKind.ADD, " + ");
        put(BinaryOperatorKind.SUB, " - ");
        put(BinaryOperatorKind.DIV, " / ");
        put(BinaryOperatorKind.MOD, " % ");
        put(BinaryOperatorKind.MUL, " * ");
        //Comparison Logical operations - numeric or Edm.String
        put(BinaryOperatorKind.EQ, " = ");
        put(BinaryOperatorKind.NE, " <> ");
        put(BinaryOperatorKind.GE, " >= ");
        put(BinaryOperatorKind.GT, " > ");
        put(BinaryOperatorKind.LE, " =< ");
        put(BinaryOperatorKind.LT, " < ");    
    }};
	
	@Override
	public Object visitBinaryOperator(BinaryOperatorKind operator, Object left, Object right)
			throws ExpressionVisitException, ODataApplicationException {
		String strOperator = BINARY_OPERATORS.get(operator);
        if (strOperator == null) {
            throw new ODataApplicationException("Unsupported binary operation: " + operator.name(),
                    operator == BinaryOperatorKind.HAS ?
                            HttpStatusCode.NOT_IMPLEMENTED.getStatusCode() :
                            HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
        }
        
        if (left instanceof String && !((String) left).startsWith("\"")) // In some queries (e.g. WHERE conditions with AND), this check is needed to avoid inserting erroneous quotes 
        	left = "\"" + left + "\""; // Cassandra is case sensitive, yet it converts column names to lower case unless they're within double quotes
        right = typeConversion(right); // OData's representation of some types is different from Cassandra's
        
        return left + strOperator + right;
	}
	
	private static Object typeConversion(Object obj) {
		Object res = obj;
		final String prefixBinary = "binary"; // blobs are represented as: binary'1234ab'
		final String prefixX = "X"; // blobs may also be represented as: X'1234ab'
		
		if (obj instanceof String) {
			String s = (String) obj;
			
			if (s.startsWith(prefixBinary)) { // blob
				res = "0x" + s.substring(prefixBinary.length()+1, s.length()-1); // remove the word binary and the quotes
			} else if (s.charAt(0) >= '0' && s.charAt(0) <= '9' && s.contains("-")) { // date
				res = "'" + s + "'"; // add quotes
			} else if (s.charAt(0) >= '0' && s.charAt(0) <= '2' && s.contains(":")) { // time of day
				res = "'" + s + "'"; // add quotes
			}
		}
		
		return res;
	}
}*/
