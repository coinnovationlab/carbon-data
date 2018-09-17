package org.wso2.carbon.dataservices.core.odata.expression;

import java.math.BigInteger;
import java.util.Date;
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
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
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
import org.apache.olingo.server.core.uri.UriResourceCountImpl;
import org.apache.olingo.server.core.uri.UriResourceLambdaVarImpl;
import org.apache.olingo.server.core.uri.UriResourceNavigationPropertyImpl;
import org.apache.olingo.server.core.uri.UriResourcePrimitivePropertyImpl;
import org.wso2.carbon.dataservices.core.odata.ForeignKey;
import org.wso2.carbon.dataservices.core.odata.ODataConstants;
import org.wso2.carbon.dataservices.core.odata.ODataServiceFault;
import org.wso2.carbon.dataservices.core.odata.ODataUtils;
import org.wso2.carbon.dataservices.core.odata.RDBMSDataHandler;
import org.wso2.carbon.dataservices.core.odata.RDBMSODataGroupBy;
import org.wso2.carbon.dataservices.core.odata.RDBMSODataQuery;
import org.wso2.carbon.dataservices.core.odata.expression.operand.TypedOperand;
import org.wso2.carbon.dataservices.core.odata.expression.operand.VisitorOperand;
import org.wso2.carbon.dataservices.core.odata.expression.operation.BinaryOperator;

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
		put(BinaryOperatorKind.LE, " <= ");
		put(BinaryOperatorKind.LT, " < ");
	}};
	
	private static enum SubQueryType {COUNT, ALL, ANY}
	
	private String tableName;
	private Map<String, List<String>> primaryKeys;
	private Map<String, Map<String, List<ForeignKey>>> foreignKeys;
	private boolean invertLogicalOperators;
	
	public FilterExpressionVisitor() {
		tableName = null;
		primaryKeys = null;
		foreignKeys = null;
		invertLogicalOperators = false;
	}
	
	public FilterExpressionVisitor(String dbType) {
		this.dbType = dbType;
	}
	
	public FilterExpressionVisitor(String tableName, Map<String, List<String>> primaryKeys, Map<String, Map<String, List<ForeignKey>>> foreignKeys, boolean invertLogicalOperators, String dbType) {
		this.tableName = tableName;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.invertLogicalOperators = invertLogicalOperators;
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
		if (invertLogicalOperators) // in case of lambda operator ALL, logical operators must be inverted, due to the sub-query involved
			strOperator = BINARY_OPERATORS.get(invertLogicalOperator(operator)); // if it is not a logical operator, it is left unchanged
		String binaryExpression = left + strOperator + right; // String to return
		
		String tablePrefix = ""; // table identifier is useful when querying multiple tables that have columns with the same name
		if (tableName != null && !tableName.equals(""))
			tablePrefix = tableName + ".";
		boolean addExists = false;
		switch (operator) {
			case EQ: case NE: case GE: case GT: case LE: case LT: // to avoid adding it multiple times, the identifier should only be added for some operators
				if ((dbType.contains(RDBMSDataHandler.H2) || dbType.contains(RDBMSDataHandler.MSSQL_SERVER))&& left instanceof String && ((String) left).contains(RDBMSODataQuery.WHERESEPARATOR))
					tablePrefix = "CONCAT(" + tablePrefix;
				binaryExpression = tablePrefix + binaryExpression; // falls through
			case OR: case AND: // enclose the expression in brackets to preserve order of operation specified by the OData query
				binaryExpression = "(" + binaryExpression + ")";
				if (addExists) // needed for sub-queries in MicroSoft SQL Server
					binaryExpression = "EXISTS " + binaryExpression;
				break;
			default:
				break;
		}
		return binaryExpression;
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
		if (literal.getType() instanceof EdmInt32) {
			literalValue = Integer.parseInt((String) literalValue);
		} else if (literal.getType() instanceof EdmDouble) {
			literalValue = Double.parseDouble((String) literalValue);
		} else if (literal.getType() instanceof EdmDate) {
			literalValue = "'"+literalValue+"'";
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
        } else if (uriResourceParts.get(size - 1) instanceof UriResourceLambdaAll) { // ALL operator, denotes that all elements must fit a specific condition
            Expression lambdaExpression = ((UriResourceLambdaAll) uriResourceParts.get(size - 1)).getExpression(); // contains the condition to apply to the elements
            return whereSubQuery(dbType, uriResourceParts.get(0).getSegmentValue(), SubQueryType.ALL, lambdaExpression);
        } else if (uriResourceParts.get(size - 1) instanceof UriResourceLambdaAny) { // ANY operator, denotes that at least 1 element must fit a specific condition
            Expression lambdaExpression = ((UriResourceLambdaAny) uriResourceParts.get(size - 1)).getExpression(); // contains the condition to apply to the elements
            return whereSubQuery(dbType, uriResourceParts.get(0).getSegmentValue(), SubQueryType.ANY, lambdaExpression);
        } else if (uriResourceParts.get(0) instanceof UriResourceLambdaVarImpl) { // entered when inspecting the lambda condition
            return uriResourceParts.get(1); // simply returns the element to apply the condition on
        } else if (uriResourceParts.get(0) instanceof UriResourceNavigationPropertyImpl) { // filter based on a related table
            if (size != 2)
                throw new ExpressionVisitException("NavigationProperty " + uriResourceParts.get(0) + " found, but the filter condition appears to be missing.");
            if (uriResourceParts.get(1) instanceof UriResourceCountImpl) { // filter is based on the count of related entities
                return whereSubQuery(dbType, uriResourceParts.get(0).getSegmentValue(), SubQueryType.COUNT, null);
            } else {
                throw new ODataApplicationException("The '" + uriResourceParts.get(1) + "' filter condition is either incorrect or not yet implemented.", HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
            }
        } else {
            return throwNotImplemented("visitMember: " + uriResourceParts.get(0).getClass());
        }
    }
    
	/**
	 * Builds a sub-query when the OData query applies a filter based on related entities.
	 * Example: [...]office?$filter=employee/any(d:d/EmployeeID lt 2) means that only offices in which all employees have EmployeeID < 2 must be returned.
	 * Other example: [..]office?$filter=employee/$count ge 3 means that only the offices that have 3 or more employees must be returned.
	 * Note that, for the second example, the sub-query will not actually be complete: the final part (>= 3 in the example) will be added
	 * by the visitBinaryOperator method, which will also add brackets before and after the string generated by this query, making the sub-query complete. It will also add CONCAT in case the database is H2.
	 * 
	 * @param foreignTable		The related table on which to execute a JOIN
	 * @param queryType			The type of sub-query, determined by the UriResource type
	 * @lambdaExpression		The lambda expression, for when the OData query contained lambda operators that made the sub-query necessary
	 * @return					The sub-query, which will be automatically completed by the visitBinaryOperator in case of COUNT, as described above
	 * @throws ExpressionVisitException
	 * @throws ODataApplicationException 
	 */
	private String whereSubQuery(String dbType, String foreignTable, SubQueryType queryType, Expression lambdaExpression) throws ExpressionVisitException, ODataApplicationException {
		if (primaryKeys == null)
			throw new ODataApplicationException("The requested query cannot be satisfied as it uses options that have yet to be implemented.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
		List<String> tablePKs = primaryKeys.get(tableName); // PK of the table. Used to check that the records are contained (or not contained, in the case of ALL) in the results of the sub-query
		Map<String, List<ForeignKey>> tableFKs = foreignKeys.get(tableName); // Map: Foreign table name -> Foreign keys to reach such foreign table
		List<ForeignKey> toForeignTableFKs = null; // Foreign keys to reach foreignTable
		if (tableFKs != null)
			toForeignTableFKs = tableFKs.get(foreignTable);
		
		// If information is missing, the sub-query cannot be applied.
		if (tableName == null || tableName.equals("") || tablePKs == null || tablePKs.size() == 0
				|| foreignTable == null || foreignTable.equals("") || toForeignTableFKs == null || toForeignTableFKs.size() == 0)
			throw new ExpressionVisitException("The sub-query is missing necessary information.");
		
		// Builds the piece that will go before the sub-query, which indicates the PK to check if the records are contained (or not contained) in the results of the sub-query
		String preSubQuery = "";
		String joinSep = "," + tableName + ".";
		if (dbType.contains(RDBMSDataHandler.H2) || dbType.contains(RDBMSDataHandler.MSSQL_SERVER)) // H2 and MSSQL do not support multi-column IN sub-queries, so we have to concatenate the PK columns
			joinSep = "," + RDBMSODataQuery.WHERESEPARATOR + "," + tableName + ".";
		if (queryType == SubQueryType.COUNT) { // filter on the amount of related entities
			preSubQuery = String.join(joinSep, tablePKs);
			if (tablePKs.size() > 1 && (dbType.contains(RDBMSDataHandler.H2) || dbType.contains(RDBMSDataHandler.MSSQL_SERVER)))
				preSubQuery += ")"; // closes the brackets that will be opened by the visitBinaryOperator method. These brackets refer to the CONCAT operation.
			preSubQuery += ") IN ("; // note the ), which closes brackets that have not been opened yet: as described above, the opening brackets (and table prefix) will be added by the visitBinaryOperator method
		} else if (queryType == SubQueryType.ALL) {
			if (tablePKs.size() > 1 && (dbType.contains(RDBMSDataHandler.H2)  || dbType.contains(RDBMSDataHandler.MSSQL_SERVER)))
				preSubQuery = "CONCAT";
			preSubQuery += "(" + tableName + "." + String.join(joinSep, tablePKs);
			preSubQuery += ") NOT IN ("; // in case of ALL, instead of checking that all elements fit the condition, it checks that none of the elements fit the opposite condition, hence NOT IN
		} else if (queryType == SubQueryType.ANY) {
			if (tablePKs.size() > 1 && (dbType.contains(RDBMSDataHandler.H2)  || dbType.contains(RDBMSDataHandler.MSSQL_SERVER)))
				preSubQuery = "CONCAT";
			preSubQuery += "(" + tableName + "." + String.join(joinSep, tablePKs);
			preSubQuery += ") IN (";
		}
		
		String columnPrefix = tableName + "."; // used in case different tables have columns with the same name
		RDBMSODataQuery rdbmsQuery = new RDBMSODataQuery();
		rdbmsQuery.setDBType(dbType);
		int i = 0;
		for (String pk : tablePKs) {
			rdbmsQuery.addSelect(columnPrefix + pk); // sub-query must SELECT the PK, much like all columns of the PK appear in the preSubQuery variable
			if ((dbType.contains(RDBMSDataHandler.H2)  || dbType.contains(RDBMSDataHandler.MSSQL_SERVER)) && i < tablePKs.size()-1)
				rdbmsQuery.addSelect(RDBMSODataQuery.WHERESEPARATOR);
			if (queryType == SubQueryType.COUNT) // executing a count requires to use GROUP BY on each PK column
				rdbmsQuery.addGroupByColumn(columnPrefix + pk);
			i++;
		}
		
		boolean addJoin = true;
		for (ForeignKey fk : toForeignTableFKs) {
			if (addJoin) { // add join command
				rdbmsQuery.appendFrom(ODataUtils.dbPrefix(fk.getDatabase(), dbType) + tableName + " LEFT OUTER JOIN " + ODataUtils.dbPrefix(fk.getForeignDatabase(), dbType) + foreignTable + " ON ");
				addJoin = false;
			} else // composite foreign key
				rdbmsQuery.appendFrom(" AND "); // additional foreign key
			rdbmsQuery.appendFrom(fk.getFullName() + " = " + fk.getFullForeignName());
		}
		
		if (queryType == SubQueryType.COUNT) // applies HAVING clause of GROUP BY
			rdbmsQuery.setGroupByHaving("COUNT(" + toForeignTableFKs.get(0).getFullForeignName() + ")");
		else if (queryType == SubQueryType.ALL && lambdaExpression != null) { // the logical operator must be inverted for ALL-type sub-queries, hence 'true' passed to the FilterExpressionVisitor constructor
			rdbmsQuery.setWhere("" + lambdaExpression.accept(new FilterExpressionVisitor(foreignTable, primaryKeys, foreignKeys, true, dbType)));
			rdbmsQuery.appendWhere(")"); // closes the sub-query; technically this closed bracket should not be part of the sub-query, but it works due to it being printed as a String
		} else if (queryType == SubQueryType.ANY && lambdaExpression != null) {
			rdbmsQuery.setWhere("" + lambdaExpression.accept(new FilterExpressionVisitor(foreignTable, primaryKeys, foreignKeys, false, dbType)));
			rdbmsQuery.appendWhere(")"); // adding the closed bracket separately would require an additional, separate 'if' check on queryType
		}
		
		return preSubQuery + rdbmsQuery; // returns a String containing the prefix with the PK that have to be IN (or NOT IN) the result set of the sub-query
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
	
	/**
	 * Returns the opposite logical operator of the one passed. Currently only used when lambda operator ALL is used, due to the sub-query created.
	 * 
	 * @param operator		The operator to invert. If it is not a logical operator, this method has no effect.
	 * @return				The inverted operator. If a non-logical operator is passed as input, it is returned unchanged.
	 */
	private static BinaryOperatorKind invertLogicalOperator(BinaryOperatorKind operator) {
		switch (operator) {
			case EQ:
				return BinaryOperatorKind.NE;
			case NE:
				return BinaryOperatorKind.EQ;
			case GE:
				return BinaryOperatorKind.LT;
			case GT:
				return BinaryOperatorKind.LE;
			case LE:
				return BinaryOperatorKind.GT;
			case LT:
				return BinaryOperatorKind.GE;
			default: // not a logical operator, return input
				return operator;
		}
	}
}
