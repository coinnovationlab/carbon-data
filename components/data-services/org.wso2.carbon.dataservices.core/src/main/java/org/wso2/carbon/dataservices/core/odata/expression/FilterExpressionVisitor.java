package org.wso2.carbon.dataservices.core.odata.expression;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceLambdaAll;
import org.apache.olingo.server.api.uri.UriResourceLambdaAny;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.apache.olingo.server.api.uri.queryoption.expression.UnaryOperatorKind;
import org.wso2.carbon.dataservices.core.odata.ODataConstants;
import org.wso2.carbon.dataservices.core.odata.ODataServiceFault;
import org.wso2.carbon.dataservices.core.odata.RDBMSDataHandler;
import org.wso2.carbon.dataservices.core.odata.expression.operand.TypedOperand;
import org.wso2.carbon.dataservices.core.odata.expression.operand.VisitorOperand;

public class FilterExpressionVisitor implements ExpressionVisitor<Object> {
	
	private static final Log log = LogFactory.getLog(FilterExpressionVisitor.class);
	private String dbType;
	public static final String POSTGRESQL = "postgresql";

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
    
    public FilterExpressionVisitor(String dbType) {
    	this.dbType = dbType;
    }
    
	@Override
	public String visitAlias(String arg0) throws ExpressionVisitException, ODataApplicationException {
		return throwNotImplemented("visitAlias");
	}

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
        if(right.toString().contains("/") || right.toString().contains("-")) {
        	//date comparison
        	right = "'"+right+"'";
        }
        return left + strOperator + right;
	}

	@Override
	public String visitEnum(EdmEnumType arg0, List<String> arg1)
			throws ExpressionVisitException, ODataApplicationException {
		return throwNotImplemented("visitEnum");
	}

	@Override
	public String visitLambdaExpression(String arg0, String arg1, Expression arg2)
			throws ExpressionVisitException, ODataApplicationException {
		return throwNotImplemented("visitLambdaExpression");
	}

	@Override
	public String visitLambdaReference(String arg0) throws ExpressionVisitException, ODataApplicationException {
		return throwNotImplemented("visitLambdaReference");
	}

	@Override
	public Object visitLiteral(Literal literal) throws ExpressionVisitException, ODataApplicationException {
		Object literalValue = literal.getText();
		if(literal.getType() instanceof EdmInt32) {
	    	literalValue = Integer.parseInt((String) literalValue);
	    }
	    else if(literal.getType() instanceof EdmDouble) {
	    	literalValue = Double.parseDouble((String) literalValue);
	    }
		return literalValue;
	}

	@Override
	public Object visitMember(Member member) throws ExpressionVisitException, ODataApplicationException {
		final List<UriResource> uriResourceParts = member.getResourcePath().getUriResourceParts();
        int size = uriResourceParts.size();
        if (uriResourceParts.get(0) instanceof UriResourceProperty) {
            EdmProperty currentEdmProperty = ((UriResourceProperty) uriResourceParts.get(0)).getProperty();
            return currentEdmProperty.getName();
        } else if (uriResourceParts.get(size - 1) instanceof UriResourceLambdaAll) {
            return throwNotImplemented("visitMember");
        } else if (uriResourceParts.get(size - 1) instanceof UriResourceLambdaAny) {
            return throwNotImplemented("visitMember");
        } else {
            return throwNotImplemented("visitMember");
        }
	}

	@Override
	public Object visitMethodCall(MethodKind methodCall, List<Object> parameters)
			throws ExpressionVisitException, ODataApplicationException {
		Object firsEntityParam = parameters.get(0);
		Object param = null;
		if(parameters.size()>1) {
			param = parameters.get(1);
		}
		log.info(" Method Call "+methodCall);
		log.info(parameters.size());
		switch (methodCall) {
	        case ENDSWITH:
	            return firsEntityParam + " LIKE '%" + extractValue(param) + "'";
	        case INDEXOF:
	            return " strpos ("+firsEntityParam+",'"+extractValue(param)+"')" ; // PostgreSQL
	            //return " CHARINDEX ("+extractFromStringValue(param)+","+firsEntityParam+")" ; // TODO SQL
	        case STARTSWITH:
	            return firsEntityParam + " LIKE '" + extractValue(param) + "%'";
	        case CONTAINS:
	            return firsEntityParam + " LIKE '%" + extractValue(param) + "%'";
	        case TOLOWER:
	            return "lower("+ firsEntityParam +") ";
	        case TOUPPER:
	            return "upper("+ firsEntityParam +") ";
	        case TRIM:
	            return "trim("+ firsEntityParam +") ";
	        case SUBSTRING:
	            return substring(parameters);
	        case CONCAT:
	            return "concat("+ firsEntityParam +","+ param + ")";
	        case LENGTH:
	            return "length("+ firsEntityParam +")";
	            
	        case YEAR:
	            return dateMethodCall("year",firsEntityParam); 
	        case MONTH:
	            return dateMethodCall("month",firsEntityParam);
	        case DAY:
	            return dateMethodCall("day",firsEntityParam);
	        case HOUR:
	            return dateMethodCall("hour",firsEntityParam);
	        case MINUTE:
	            return dateMethodCall("minute",firsEntityParam);
	        case SECOND:
	            return dateMethodCall("second",firsEntityParam);
	        case ROUND:
	            return "round("+ firsEntityParam +")";
	        case FLOOR:
	            return "floor("+ firsEntityParam +")";
	        case CEILING:
	            return "ceiling("+ firsEntityParam +")";
	        default:
	            return throwNotImplemented(" visitMethodCall " + methodCall);
	    }
	}

	private String dateMethodCall(String type,Object value) {
		String query = "";
		log.info("database type: "+this.dbType);
		
		switch(this.dbType) {
	    	case POSTGRESQL: 
	    		query = "extract("+type+" from "+value+")";
	    		break;
	    	default: 
	    		query = type+"("+ value +")";
	    }
		return query;
	}
	
	@Override
	public String visitTypeLiteral(EdmType arg0) throws ExpressionVisitException, ODataApplicationException {
		return throwNotImplemented("visitTypeLiteral");
	}

	@Override
	public Object visitUnaryOperator(UnaryOperatorKind operator, Object operand)
			throws ExpressionVisitException, ODataApplicationException {
		switch (operator) {
	        case NOT:
	            return "NOT " + operand;
	        case MINUS:
	            return "-" + operand;
        }
		return throwNotImplemented("Invalid type for unary operator");		    
	}

	private String throwNotImplemented(String type ) throws ODataApplicationException {
        throw new ODataApplicationException("Not implemented: "+type, HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                                            Locale.ENGLISH);
    }
	
	private  String substring(List<Object> parameters) throws ODataApplicationException {
        Object valueOperand = parameters.get(0);
        Object startOperand = parameters.get(1);
        log.info(valueOperand);log.info(startOperand);
        if (valueOperand==null || startOperand==null) {
            return null;
        } else if (valueOperand instanceof String) {
            final String value = valueOperand.toString();
            int start = Integer.parseInt((String) startOperand);
            start = start < 0 ? 0 : start;
            int end = value.length();
            if (parameters.size() == 3) {
            	Object lengthOp= parameters.get(2);
            	if(lengthOp !=null) {
	                int lengthOperand = Integer.parseInt((String) parameters.get(2));
	                log.info(lengthOperand);
                    end = lengthOperand;
                    end = end < 0 ? 0 : end;
                } else {
                    throw new ODataApplicationException("Third substring parameter should be Integer",
                                                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
                }
            	return "SUBSTRING("+ value +", "+start+ ","+ end+ ")";
            }
            return "SUBSTRING("+ value +", "+start+")";
        } else {
            throw new ODataApplicationException(
                    "Substring has invalid parameters. First parameter should be String," +
                    " second parameter should be Edm.Int32", HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
        }
    }

	private Object extractValue(Object val) {
		String value= (String) val;
		if(value.length() > 2) {
			value = ((String) val).substring(1, ((String) val).length() - 1);
		}
        return value;
    }

}
