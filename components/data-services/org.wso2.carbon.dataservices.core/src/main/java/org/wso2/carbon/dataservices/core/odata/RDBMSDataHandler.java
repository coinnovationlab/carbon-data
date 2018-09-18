/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.dataservices.core.odata;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.axis2.databinding.utils.ConverterUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.wso2.carbon.dataservices.common.DBConstants;
import org.wso2.carbon.dataservices.common.conf.DynamicODataConfig;
import org.wso2.carbon.dataservices.common.conf.ODataColumnsConfig;
import org.wso2.carbon.dataservices.core.DBUtils;
import org.wso2.carbon.dataservices.core.DataServiceFault;
import org.wso2.carbon.dataservices.core.engine.DataEntry;
import org.wso2.carbon.dataservices.core.odata.DataColumn.ODataDataType;
import org.wso2.carbon.dataservices.core.odata.expression.ExpressionVisitorImpl;
import org.wso2.carbon.dataservices.core.odata.expression.FilterExpressionVisitor;
import org.wso2.carbon.dataservices.core.odata.expression.operand.TypedOperand;
import org.wso2.carbon.dataservices.core.odata.expression.operand.VisitorOperand;
import org.wso2.carbon.dataservices.core.odata.RDBMSDataHandler;

import javax.sql.DataSource;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This class implements RDBMS datasource related operations for ODataDataHandler.
 *
 * @see ODataDataHandler
 */
public class RDBMSDataHandler implements ODataDataHandler {
    private static final Log log = LogFactory.getLog(RDBMSDataHandler.class);
    /**
     * Table metadata.
     */
    private Map<String, Map<String, Integer>> rdbmsDataTypes;

    private Map<String, Map<String, DataColumn>> tableMetaData;

    /**
     * Primary Keys of the tables (Map<Table Name, List>).
     */
    private Map<String, List<String>> primaryKeys;

    /**
     * Foreign Keys of the tables (Map<Table name, Map<Foreign table name, Foreign keys to such table>>).
     */
    private Map<String, Map<String, List<ForeignKey>>> foreignKeys;

    /**
     * Config ID.
     */
    private final String configID;

    /**
     * RDBMS datasource.
     */
    private final DataSource dataSource;

    /**
     * List of tables in the database.
     */
    private List<String> tableList;
    private List<String> oDataTableList;
    private Map<String,String> oDataTableSchema = new HashMap<String,String>();
    private Map<String,List<ODataColumnsConfig>> oDataColumnsConfig = new HashMap<String,List<ODataColumnsConfig>>();
    private int oDataMaxLimit;
    private String dbType;

    public static final String TABLE_CAT = "TABLE_CAT";
    public static final String TABLE_NAME = "TABLE_NAME";
    public static final String TABLE = "TABLE";
    public static final String VIEW = "VIEW";
    public static final String ORACLE_SERVER = "oracle";
    public static final String MSSQL_SERVER = "microsoft sql server";
    public static final String MYSQL = "mysql";
    public static final String POSTGRESQL = "postgresql";
    public static final String H2 = "h2";

    private ThreadLocal<Connection> transactionalConnection = new ThreadLocal<Connection>() {
        protected synchronized Connection initialValue() {
            return null;
        }
    };

    private boolean defaultAutoCommit;
    private int defaultTransactionalIsolation;

    /**
     * Navigation properties map <Target Table Name, Map<Source Table Name, List<String>).
     */
    private Map<String, NavigationTable> navigationProperties;

    public RDBMSDataHandler(DataSource dataSource, String configId, String odataConfig) throws ODataServiceFault {
    	
    	this.dataSource = dataSource;
        this.configID = configId;
        try {
	        OMElement dynTableODataConfEl = AXIOMUtil.stringToOM(odataConfig);
	        ArrayList<String> dynamicTableList = new ArrayList<String>();
	        if(dynTableODataConfEl != null) {
	            DynamicODataConfig dynamicODataTableConfiguration = new DynamicODataConfig();
	            Iterator<OMElement> dynamicODataTablesConfigs = dynTableODataConfEl.getChildrenWithName(new QName("tblname"));
	            this.oDataMaxLimit = Integer.parseInt(dynTableODataConfEl.getAttributeValue(new QName("maxLimit")) );
	            ODataColumnsConfig columnsConf = new ODataColumnsConfig();
	            List<ODataColumnsConfig> columnsConfAll = new ArrayList<ODataColumnsConfig>();
	            while (dynamicODataTablesConfigs.hasNext()) {
	                OMElement dynamicOdataConfig = dynamicODataTablesConfigs.next();
	                String tblname = dynamicOdataConfig.getAttributeValue(new QName("name"));
	                String schemaname = dynamicOdataConfig.getAttributeValue(new QName("schema"));
	                dynamicTableList.add(tblname);
	                this.oDataTableSchema.put(tblname, schemaname);
	                String key = schemaname+"."+tblname;
	                //TODO
	                Iterator<OMElement> dynamicColConfigs = dynamicOdataConfig.getChildrenWithName(new QName("column"));
	                columnsConfAll = new ArrayList<ODataColumnsConfig>();
	                while (dynamicColConfigs.hasNext()) {
		                OMElement dynamicColConfig = dynamicColConfigs.next();
		                String type = dynamicColConfig.getAttributeValue(new QName("type"));
		                String colName = dynamicColConfig.getText();
		                columnsConf = new ODataColumnsConfig();
		                columnsConf.setColumnName(colName);
		                columnsConf.setType(type);
		                columnsConfAll.add(columnsConf);
		            }
	                this.oDataColumnsConfig.put(key,columnsConfAll);
	            }
	        }
            this.oDataTableList=dynamicTableList;
            this.tableList = generateTableList(dynamicTableList);
            this.rdbmsDataTypes = new HashMap<>(this.tableList.size());
            this.dbType = getDBType();
        }
        catch (XMLStreamException e) {
        	
        }
        initializeMetaData();
    }

    @Override
    public Map<String, NavigationTable> getNavigationProperties() {
        return this.navigationProperties;
    }

    @Override
    public void openTransaction() throws ODataServiceFault {
        try {
            if (getTransactionalConnection() == null) {
                Connection connection = this.dataSource.getConnection();
                this.defaultAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                this.defaultTransactionalIsolation = connection.getTransactionIsolation();
                try {
                    connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                } catch (SQLException e) {
                    // Some Databases are not supported REPEATABLE_READ Isolation level.
                    connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                }
                transactionalConnection.set(connection);
            }
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Connection Error occurred. :" + e.getMessage());
        }
    }

    @Override
    public void commitTransaction() throws ODataServiceFault {
        Connection connection = getTransactionalConnection();
        try {
            connection.commit();
            connection.setTransactionIsolation(defaultTransactionalIsolation);
            connection.setAutoCommit(defaultAutoCommit);
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Connection Error occurred while committing. :" + e.getMessage());
        } finally {
        /* close the connection */
            try {
                connection.close();
                transactionalConnection.set(null);
            } catch (Exception ignore) {
                // ignore
            }
        }
    }

    private Connection getTransactionalConnection() {
        return transactionalConnection.get();
    }

    @Override
    public void rollbackTransaction() throws ODataServiceFault {
        Connection connection = getTransactionalConnection();
        try {
            connection.rollback();
            connection.setTransactionIsolation(defaultTransactionalIsolation);
            connection.setAutoCommit(defaultAutoCommit);
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Connection Error occurred while rollback. :" + e.getMessage());
        } finally {
		/* close the connection */
            try {
                connection.close();
                transactionalConnection.set(null);
            } catch (Exception ignore) {
                // ignore
            }
        }
    }

    @Override
    public void updateReference(String rootTable, ODataEntry rootTableKeys, String navigationTable,
                                ODataEntry navigationTableKeys) throws ODataServiceFault {
		/* To add a reference first we need to find the foreign key values of the tables,
		and therefore we need to identify which table has been exported */
        // Identifying the exported table and change the imported tables' column value
        NavigationTable navigation = navigationProperties.get(rootTable);
        boolean rootTableExportedColumns = false;
        if (navigation != null && navigation.getTables().contains(navigationTable)) {
            // that means rootTable is the exportedTable -confirmed
            rootTableExportedColumns = true;
        }
        String exportedTable;
        String importedTable;
        ODataEntry exportedTableKeys;
        ODataEntry importedTableKeys;
        List<NavigationKeys> keys;
        if (rootTableExportedColumns) {
            exportedTable = rootTable;
            importedTable = navigationTable;
            exportedTableKeys = rootTableKeys;
            importedTableKeys = navigationTableKeys;
        } else {
            exportedTable = navigationTable;
            importedTable = rootTable;
            exportedTableKeys = navigationTableKeys;
            importedTableKeys = rootTableKeys;
        }
        keys = navigationProperties.get(exportedTable).getNavigationKeys(importedTable);
        ODataEntry exportedKeyValues = getForeignKeysValues(exportedTable, exportedTableKeys, keys);
        modifyReferences(keys, importedTable, exportedTable, exportedKeyValues, importedTableKeys);
    }

    @Override
    public void deleteReference(String rootTable, ODataEntry rootTableKeys, String navigationTable,
                                ODataEntry navigationTableKeys) throws ODataServiceFault {
		/* To add a reference first we need to find the foreign key values of the tables,
		and therefore we need to identify which table has been exported */
        // Identifying the exported table and change the imported tables' column value
        NavigationTable navigation = navigationProperties.get(rootTable);
        boolean rootTableExportedColumns = false;
        if (navigation != null && navigation.getTables().contains(navigationTable)) {
            // that means rootTable is the exportedTable -confirmed
            rootTableExportedColumns = true;
        }
        String exportedTable;
        String importedTable;
        ODataEntry importedTableKeys;
        List<NavigationKeys> keys;
        if (rootTableExportedColumns) {
            exportedTable = rootTable;
            importedTable = navigationTable;
            importedTableKeys = navigationTableKeys;
        } else {
            exportedTable = navigationTable;
            importedTable = rootTable;
            importedTableKeys = rootTableKeys;
        }
        keys = navigationProperties.get(exportedTable).getNavigationKeys(importedTable);
        ODataEntry nullReferenceValues = new ODataEntry();
        for (NavigationKeys key : keys) {
            nullReferenceValues.addValue(key.getForeignKey(), null);
        }
        modifyReferences(keys, importedTable, exportedTable, nullReferenceValues, importedTableKeys);
    }

    private void modifyReferences(List<NavigationKeys> keys, String importedTable, String exportedTable,
                                  ODataEntry modifyValues, ODataEntry primaryKeys) throws ODataServiceFault {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = initializeConnection();
            String query = createAddReferenceSQL(importedTable, keys);
            statement = connection.prepareStatement(query);
            int index = 1;
            for (String column : modifyValues.getNames()) {
                String value = modifyValues.getValue(column);
                bindValuesToPreparedStatement(this.rdbmsDataTypes.get(exportedTable).get(column), value, index,
                                              statement);
                index++;
            }
            for (String column : primaryKeys.getNames()) {
                String value = primaryKeys.getValue(column);
                bindValuesToPreparedStatement(this.rdbmsDataTypes.get(importedTable).get(column), value, index,
                                              statement);
                index++;
            }
            statement.execute();
            commitExecution(connection);
        } catch (SQLException | ParseException e) {
            log.warn("modify value count - " + modifyValues.getNames().size() + ", primary keys size - " +
                     primaryKeys.getNames().size() + ", Error - " + e.getMessage(), e); //todo remove this later
            throw new ODataServiceFault(e, "Error occurred while updating foreign key values. :" + e.getMessage());
        } finally {
            releaseResources(null, statement);
            releaseConnection(connection);
        }
    }

    private ODataEntry getForeignKeysValues(String tableName, ODataEntry keys, List<NavigationKeys> columns)
            throws ODataServiceFault {
        ResultSet resultSet = null;
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            connection = initializeConnection();
            String query = createSelectReferenceKeyFromExportedTable(tableName, keys, columns);
            statement = connection.prepareStatement(query);
            int index = 1;
            for (String column : keys.getNames()) {
                String value = keys.getValue(column);
                bindValuesToPreparedStatement(this.rdbmsDataTypes.get(tableName).get(column), value, index, statement);
                index++;
            }
            resultSet = statement.executeQuery();
            ODataEntry values = new ODataEntry();
            String value;
            for (NavigationKeys column : columns) {
                String columnName = column.getPrimaryKey();
                while (resultSet.next()) {
                    value = getValueFromResultSet(this.rdbmsDataTypes.get(tableName).get(columnName), columnName,
                                                  resultSet);
                    values.addValue(columnName, value);
                }
            }
            return values;
        } catch (SQLException | ParseException e) {
            throw new ODataServiceFault(e, "Error occurred while retrieving foreign key values. :" + e.getMessage());
        } finally {
            releaseResources(resultSet, statement);
            releaseConnection(connection);
        }
    }

    private String createSelectReferenceKeyFromExportedTable(String tableName, ODataEntry keys,
                                                             List<NavigationKeys> columns) {
        StringBuilder sql = new StringBuilder();
        boolean propertyMatch = false;
        sql.append("SELECT ");
        for (NavigationKeys column : columns) {
            if (propertyMatch) {
                sql.append(" , ");
            }
            sql.append(column.getPrimaryKey());
            propertyMatch = true;
        }
        sql.append(" FROM ").append(tableName).append(" WHERE ");
        propertyMatch = false;
        for (String column : this.rdbmsDataTypes.get(tableName).keySet()) {
            if (keys.getValue(column) != null) {
                if (propertyMatch) {
                    sql.append(" AND ");
                }
                sql.append(column).append(" = ").append(" ? ");
                propertyMatch = true;
            }
        }
        return sql.toString();
    }

    private String createAddReferenceSQL(String tableName, List<NavigationKeys> keys) {
        List<String> pKeys = primaryKeys.get(tableName);
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableName).append(" SET ");
        boolean propertyMatch = false;
        for (NavigationKeys column : keys) {
            if (propertyMatch) {
                sql.append(",");
            }
            sql.append(column.getForeignKey()).append(" = ").append(" ? ");
            propertyMatch = true;
        }
        sql.append(" WHERE ");
        // Handling keys
        propertyMatch = false;
        for (String key : pKeys) {
            if (propertyMatch) {
                sql.append(" AND ");
            }
            sql.append(key).append(" = ").append(" ? ");
            propertyMatch = true;
        }
        return sql.toString();
    }

    @Override
    public List<ODataEntry> readTable(String tableName, UriInfo uriInfo, List<Property> navProperties) throws ODataServiceFault, ExpressionVisitException, ODataApplicationException {
        ResultSet resultSet = null;
        Connection connection = null;
        PreparedStatement statement = null;
        FilterOption filterOption = uriInfo.getFilterOption();
        OrderByOption orderByOption = uriInfo.getOrderByOption();
        SkipOption skipOption = uriInfo.getSkipOption();
        TopOption topOption = uriInfo.getTopOption();
        if (topOption != null && topOption.getValue() == 0) // MSSQL and Oracle will throw an exception when queried with $top=0: might as well return an empty set here, since no records must be extracted anyway
            return new ArrayList<ODataEntry>(); 
        ExpandOption expandOption = uriInfo.getExpandOption();
        
        RDBMSODataQuery rdbmsQuery = new RDBMSODataQuery();
        String dbType = this.dbType;
        rdbmsQuery.setDBType(dbType);
        String query = "";
        
        // Without the following, the order of the elements would be inconsistent when the user specifies an $orderby nested within $expand,
        // since a nested $orderby is only referring to the related sub-entries and not to the "main" elements. We apply a default ORDER BY even when $expand is not specified, to maintain a consistent order.
        // If the user specifies a non-nested $orderby that refers to the "main" table, it will override this default one.
        List<String> primaryKeys = getPrimaryKeys().get(tableName);
        if (primaryKeys != null && primaryKeys.size() > 0) {
            for (String s : primaryKeys)
                rdbmsQuery.addOrderBy(tableName + "." + s);
        }
        
        if (topOption != null) {
            rdbmsQuery.setLimit(topOption.getValue());
        }
        if(skipOption != null) {
            rdbmsQuery.setOffset(skipOption.getValue());
        }
        if(orderByOption != null) { // Overrides the default ordering explained above. Not an issue, because it still provides consistency.
            rdbmsQuery.setOrderBy(null);
            for (String s : getOrderBy(orderByOption))
                rdbmsQuery.addOrderBy(tableName + "." + s);
        }
        if (filterOption != null)
            rdbmsQuery.setWhere("" + filterOption.getExpression().accept(new FilterExpressionVisitor(tableName, getPrimaryKeys(), getForeignKeys(), false, dbType)));
        if (navProperties != null && navProperties.size() > 0)
            for (Property p : navProperties) {
                if (rdbmsQuery.getWhere() != null && !rdbmsQuery.getWhere().equals(""))
                	rdbmsQuery.appendWhere(" AND ");
                rdbmsQuery.appendWhere(tableName + "." + p.getName() + " = '" + p.getValue() + "'");
            }
        try {
            connection = initializeConnection();
            // Sets proper SELECT statement if there are JOINs to perform. Also updates ORDER BY, if necessary.
            handleExpand(rdbmsQuery, connection.getCatalog(), tableName, expandOption);
            
            log.info("limit: " + rdbmsQuery.getLimit() + " offset: " + rdbmsQuery.getOffset() + " orderBy: " + rdbmsQuery.getOrderBy() + " where: " + rdbmsQuery.getWhere());
            String schema_prefix = this.oDataTableSchema.get(tableName)+".";
            if(this.oDataTableSchema.get(tableName) == null || this.oDataTableSchema.get(tableName).equals(DBConstants.NO_SCHEMA)) { //takes in consideration dbs that don't use schema like MySQL
                schema_prefix = "";
            }
            if (rdbmsQuery.getSelect() == null || rdbmsQuery.getSelect().size() == 0) { // There is no expand option, so we can just SELECT * from the table
                rdbmsQuery.addSelect(schema_prefix + tableName + ".*");
                rdbmsQuery.setFrom(schema_prefix + tableName);
            }
            query = queryBasedOnDBType(rdbmsQuery.printSelect() + rdbmsQuery.printFrom(), rdbmsQuery.printWhere(), rdbmsQuery.getLimit(), rdbmsQuery.getOffset(), rdbmsQuery.printOrderBy());
            log.info("Generated query: " + query);
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            return createDataEntryCollectionFromRS(tableName, resultSet, expandOption);
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Error occurred while reading entities from " + tableName + " table. :" +
                                           e.getMessage());
        } finally {
            releaseResources(resultSet, statement);
            releaseConnection(connection);
        }
    }
    
	/**
	 * Determines the database type.
	 * 
	 * @return		Database type
	 * @throws ODataServiceFault	Error while determining database type
	 */
	public String getDBType() throws ODataServiceFault {
		Connection connection = null;
		DatabaseMetaData meta = null;
		String dbType= "";
		try {
			connection = initializeConnection();
			meta = connection.getMetaData();
			dbType=meta.getDatabaseProductName().toLowerCase();
			return dbType;
		} catch (SQLException e) {
			throw new ODataServiceFault(e, "Error occurred while detecting db type :" + e.getMessage());
		} finally {
			releaseConnection(connection);
		}
	}
	
	/**
	 * If $expand is present, it generates SELECT, JOIN, and the ORDER BY update to attach to the current ORDER BY
	 * 
	 * @param rdbmsQuery	Object containing the various parts of the query
	 * @param dbName		Name of the database that tableName belongs to
	 * @param tableName		Name of the "main" table, with which to join the other tables
	 * @param expandOption	Contains the names of the table to expand on
	 * @throws ODataApplicationException 	FilterExpressionVisitor: error building JOIN command while visiting the $filter nested within $expand
	 * @throws ExpressionVisitException 	FilterExpressionVisitor: error building JOIN command while visiting the $filter nested within $expand
	 */
	private void handleExpand(RDBMSODataQuery rdbmsQuery, String dbName, String tableName, ExpandOption expandOption) throws ExpressionVisitException, ODataApplicationException {
		if (expandOption == null)
			return;
		
		List<ExpandItem> expandItems = expandOption.getExpandItems(); // tables that are to be expanded on
		FilterOption expandFilter;
		OrderByOption expandOrderBy;
		
		rdbmsQuery.addSelect(tableName + ".*"); // SELECT all fields of the "main" table
		if (dbName != null && !dbName.equals("") && dbType.contains(MYSQL)) // if database was specified, attach it
			rdbmsQuery.appendFrom(dbName + "."); // name of the database plus dot
		rdbmsQuery.appendFrom(tableName); // name of the table
		buildFromSubQuery(rdbmsQuery, dbName, tableName); // Used when FROM needs a subquery, which happens when both $expand and $top/$skip are present in the OData query
		
		for (ExpandItem expandItem : expandItems) { // for each table listed in $expand, reference it and include its columns
			String targetTable = expandItem.getResourcePath().getUriResourceParts().get(0).getSegmentValue(); // name of the table to expand on
			Map<String, List<ForeignKey>> tableFKs = foreignKeys.get(tableName); // Map: Foreign table name -> Foreign keys to reach such foreign table
			if (tableFKs == null || tableFKs.size() == 0)
				throw new ODataApplicationException("Table " + tableName + " has no foreign keys.",HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
			List<ForeignKey> toForeignTableFKs = tableFKs.get(targetTable); // Foreign keys to reach table targetTable
			if (toForeignTableFKs == null || toForeignTableFKs.size() == 0)
				throw new ODataApplicationException("Table " + tableName + " has no foreign key that references " + targetTable + ".",HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
			String foreignDB = toForeignTableFKs.get(0).getForeignDatabase();
			if (expandItem.getExpandOption() != null)
				throw new ODataApplicationException("Nested $expand commands have not been implemented yet.", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
			
			addSelectColumns(rdbmsQuery, foreignDB, targetTable); // adds columns of the foreign table
			boolean addJoin = true;
			for (ForeignKey fk : toForeignTableFKs) {
				if (addJoin) { // add join command
					rdbmsQuery.appendFrom(" LEFT OUTER JOIN " + ODataUtils.dbPrefix(foreignDB, dbType) + targetTable + " ON "); // joins this foreign table
					addJoin = false;
				} else
					rdbmsQuery.appendFrom(" AND "); // additional foreign key
				rdbmsQuery.appendFrom(fk.getFullName() + " = " + fk.getFullForeignName());
			}
			expandFilter = expandItem.getFilterOption();
			if (expandFilter != null) { // if there are filters on the $expand tables, they need to be applied together with the JOIN command
				rdbmsQuery.appendFrom(" AND " + expandFilter.getExpression().accept(new FilterExpressionVisitor(targetTable, getPrimaryKeys(), getForeignKeys(), false, rdbmsQuery.getDBType())));
			}
			List<String> foreignPKs = primaryKeys.get(targetTable);
			expandOrderBy = expandItem.getOrderByOption();
			if (expandOrderBy != null) { // if there are nested $orderby's, adds the parameters to the already present ORDER BY
				for (String s : getOrderBy(expandOrderBy))
					rdbmsQuery.addOrderBy(ODataUtils.dbPrefix(foreignDB, dbType) + targetTable + "." + s);
			} else if (foreignPKs != null) { // adds default ordering for consistency
				for (String s : foreignPKs)
					rdbmsQuery.addOrderBy(ODataUtils.dbPrefix(foreignDB, dbType) + targetTable + "." + s);
			}
		}
	}
	
	/**
	 * Returns the sub-query to use in FROM, when both $expand and $top/$skip are present in the OData query
	 * 
	 * @param rdbmsQuery	The query that will contain the sub-query returned
	 * @param dbName		Name of the database
	 * @param tableName		Name of the table
	 * @return				The FROM sub-query necessary, or null if not necessary
	 */
	private void buildFromSubQuery(RDBMSODataQuery rdbmsQuery, String dbName, String tableName) {
		if (rdbmsQuery.getLimit() < 0 && rdbmsQuery.getOffset() <= 0) // no sub-query necessary, no need to edit FROM statement
			return;
		
		RDBMSODataQuery subQuery = new RDBMSODataQuery(rdbmsQuery); // sub-query is essentially the same as the query would be without the bits added by handling $expand
		rdbmsQuery.setFrom("(" + subQuery + ") " + tableName); // sets FROM statement, containing sub-query and alias for the results of the sub-query
		rdbmsQuery.setLimit(-1); // LIMIT will be applied in the sub-query, remove from "main" query (otherwise it would give incorrect results)
		rdbmsQuery.setOffset(0); // OFFSET will be applied in the sub-query, remove from "main" query (otherwise it would give incorrect results)
		rdbmsQuery.setWhere(""); // WHERE will be applied in the sub-query, remove from "main" query (it would give the same results, we remove it simply because it would be redundant)
	}
	
	/**
	 * Adds the names of the columns belonging to the input table to the SELECT command.
	 * Column names are converted to <table_name>.<column_name> to avoid issues with columns with the same name in different tables.
	 * 
	 * @param rdbmsQuery	Object containing the various parts of the query
	 * @param db		Name of the database the table belongs to
	 * @param table		Name of the table
	 */
	private void addSelectColumns(RDBMSODataQuery rdbmsQuery, String db, String table) {
		String quote = "\""; // default
		if (dbType.contains(MYSQL))
			quote = "`";
		for (DataColumn column : this.tableMetaData.get(table).values()) {
			rdbmsQuery.addSelect(ODataUtils.dbPrefix(db, dbType) + table + "." + column.getColumnName() + " AS " +
					quote + table + "." + column.getColumnName() + quote); // note that there will be a trailing comma
		}
	}
	
	public String queryBasedOnDBType(String select, String where, int row_count, int offset, String orderBy) throws ODataServiceFault {
		String query= "";
		switch(this.dbType) {
			case ORACLE_SERVER:
				query = queryGeneratorOracle(select, where, row_count, offset, orderBy);
				break;
			case MSSQL_SERVER:
				query = queryGeneratorMSSql(select, where, row_count, offset, orderBy);
				break;
			case POSTGRESQL:
				/* fall through */
			case H2: 
				/* fall through */
			case MYSQL:
				query = queryGeneratorSQL(select, where, row_count, offset, orderBy);
				break;
			default:
				throw new ODataServiceFault("DB Type not supported. " );
		}
		return query;
	}
	
    public String handleFilterExpression(FilterOption filterOption) throws ExpressionVisitException, ODataApplicationException {
    	String where = "";
    	Expression exp = filterOption.getExpression();
    	log.info(exp);
    	where = " where " + filterOption.getExpression().accept(new FilterExpressionVisitor(dbType));
    	return where;
    }
    
    /*
     * Query Generator supporting query format for oracle version 12c
     */
    private String queryGeneratorOracle (String select, String where, int row_count, int offset, String orderBy) {
    	String query = "",limit= "";
    	if(row_count > 0) {
        	if(offset != 0) {
        		limit = " OFFSET " + offset + " ROWS";
        		limit += " FETCH NEXT " + row_count + " ROWS ONLY ";
            }
        	else {
        		limit += " FETCH FIRST " + row_count + " ROWS ONLY ";
        	}
        }
        else if(offset != 0 ){
        	limit =" OFFSET " + offset + " ROWS";
        }
    	// ROWNUM <= number;  // old versions
    	query = select + where + orderBy + limit;
    	return query;
    }
    
    /*
     * Query Generator supporting query format for Microsoft SQL Server 2012 and over
     */
    private String queryGeneratorMSSql (String select, String where, int row_count, int offset, String orderBy) {
        String query = "",limit= "";
        if(orderBy.equals("") && (offset != 0 || row_count > 0)) {
            orderBy = " ORDER BY (SELECT 1) ";
        }
        if (offset != 0 || row_count > 0)
            limit = " OFFSET " + offset + " ROWS";
        if (row_count > 0)
            limit += " FETCH NEXT " + row_count + " ROWS ONLY ";
        // Select TOP 3 * // old versions
        query = select + where + orderBy + limit;
        return query;
    }
    
    /*
     * Query Generator supporting query format for MySQL, PostgreSQL 
     */
    private String queryGeneratorSQL (String select, String where, int row_count, int offset, String orderBy) {
        String query = "", limit= "";
        if (row_count >= 0) {
            limit = " limit " + row_count;
            if(offset != 0) {
                limit += " offset " + offset;
            }
        } else if (offset != 0 ) {
            limit = " offset " + offset;
            // Adding 'limit MYSQL_MAX_LIMIT' is necessary for MySQL, which does not allow OFFSET without LIMIT. It is equal to 2^64-1, the highest number allowed by MySQL.
            if (dbType.contains(MYSQL))
                limit = " limit " + RDBMSODataQuery.MYSQL_MAX_LIMIT + limit;
            else if (dbType.contains(H2))
                limit = " limit " + RDBMSODataQuery.H2_MAX_LIMIT + limit;
        }
        query = select + where + orderBy + limit;
        return query;
    }
    
    private String[] getOrderBy(OrderByOption orderByOption) throws ExpressionVisitException, ODataApplicationException {
    	ArrayList<String> orders = new ArrayList<>();
    	String  direction="";
    	for (int i = 0; i < orderByOption.getOrders().size(); i++) {
    		final OrderByItem item = orderByOption.getOrders().get(i);
    		direction = item.isDescending() ? " DESC" : " ASC";
    		String column = (String) item.getExpression().accept(new FilterExpressionVisitor(this.dbType));
    		orders.add(column + direction);
        }
    	String [] order = orders.toArray(new String [orders.size()]);
    	return order;
    }
    
    @Override
    public int countRecords(UriInfo uriInfo, String tableName) throws ODataServiceFault, ExpressionVisitException, ODataApplicationException {
        String query = "" , where = "";
        int total = 0;
        String schema_prefix = this.oDataTableSchema.get(tableName)+".";
        if(this.oDataTableSchema.get(tableName) == null || this.oDataTableSchema.get(tableName).equals(DBConstants.NO_SCHEMA)) { //takes in consideration dbs that don't use schema like MySQL
            schema_prefix = "";
        }
        CountOption countOption = uriInfo.getCountOption();
        FilterOption filterOption = uriInfo.getFilterOption();
        if (filterOption != null) {
            where = " where " + filterOption.getExpression().accept(new FilterExpressionVisitor(tableName, getPrimaryKeys(), getForeignKeys(), false, this.dbType));
        }
        Boolean count = countOption.getValue();
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement statement = null;
        
        if (count) {
            try {
                connection = initializeConnection();
                query = "select count(*) as total from " + schema_prefix + tableName + where;
                log.info("Count query: " + query);
                statement = connection.prepareStatement(query);
                resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    total = resultSet.getInt("total");
                }
            } catch (SQLException e) {
                throw new ODataServiceFault(e, "Error occurred while counting entities from " + tableName + " table. :" +
                                               e.getMessage());
            } finally {
                releaseResources(resultSet, statement);
                releaseConnection(connection);
            }
        }
        return total;
    }
    
    @Override
    public List<String> getTableList() {
        return this.tableList;
    }

    @Override
    public Map<String, List<String>> getPrimaryKeys() {
        return this.primaryKeys;
    }

    private String convertToTimeString(Time sqlTime) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(sqlTime.getTime());
        return new org.apache.axis2.databinding.types.Time(cal).toString();
    }

    private String convertToTimestampString(Timestamp sqlTimestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(sqlTimestamp.getTime());
        return ConverterUtil.convertToString(cal);
    }

    @Override
    public ODataEntry insertEntityToTable(String tableName, ODataEntry entry) throws ODataServiceFault {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = initializeConnection();
            String query = createInsertSQL(tableName, entry);
            boolean isAvailableAutoIncrementColumns = isAvailableAutoIncrementColumns(tableName);
            if(isAvailableAutoIncrementColumns) {
                statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            } else {
                statement = connection.prepareStatement(query);
            }
            int index = 1;
            for (String column : entry.getNames()) {
                if (this.rdbmsDataTypes.get(tableName).keySet().contains(column)) {
                    String value = entry.getValue(column);
                    bindValuesToPreparedStatement(this.rdbmsDataTypes.get(tableName).get(column), value, index,
                                                  statement);
                    index++;
                }
            }
            ODataEntry createdEntry = new ODataEntry();
            if (isAvailableAutoIncrementColumns(tableName)) {
                statement.executeUpdate();
                ResultSet resultSet = statement.getGeneratedKeys();
                String paramValue;
                int i = 1;
                while (resultSet.next()) {
                    for (DataColumn column : this.tableMetaData.get(tableName).values()) {
                        if (column.isAutoIncrement()) {
                            String resultSetColumnName = resultSet.getMetaData().getColumnName(i);
                            String columnName = column.getColumnName();
                            int columnType = this.rdbmsDataTypes.get(tableName).get(columnName);
                            paramValue = getValueFromResultSet(columnType, resultSetColumnName, resultSet);
                            createdEntry.addValue(columnName, paramValue);
                            // Need to add this column to generate the E-tag
                            entry.addValue(columnName, paramValue);
                        }
                    }
                    i++;
                }
                releaseResources(resultSet, null);
            } else {
                statement.execute();
            }
            commitExecution(connection);
            createdEntry.addValue(ODataConstants.E_TAG, ODataUtils.generateETag(this.configID, tableName, entry));
            return createdEntry;
        } catch (SQLException | ParseException e) {
            throw new ODataServiceFault(e, "Error occurred while writing entities to " + tableName + " table. :" +
                                           e.getMessage());
        } finally {
            releaseResources(null, statement);
            releaseConnection(connection);
        }
    }

    private boolean isAvailableAutoIncrementColumns(String table) {
        for (DataColumn column : this.tableMetaData.get(table).values()) {
            if (column.isAutoIncrement()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<ODataEntry> readTableWithKeys(String tableName, ODataEntry keys, UriInfo uriInfo) throws ODataServiceFault {
        ResultSet resultSet = null;
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = initializeConnection();
            ExpandOption expandOption = null;
            if (uriInfo != null)
                expandOption = uriInfo.getExpandOption();
            String query = createReadSqlWithKeys(connection.getCatalog(), tableName, keys, expandOption);
            statement = connection.prepareStatement(query);
            int index = 1;
            
            for (String column : keys.getNames()) {
                if (this.rdbmsDataTypes.get(tableName).keySet().contains(column)) {
                    bindValuesToPreparedStatement(this.rdbmsDataTypes.get(tableName).get(column), keys.getValue(column), index, statement);
                    index++;
                }
            }
            resultSet = statement.executeQuery();
            return createDataEntryCollectionFromRS(tableName, resultSet, expandOption);
        } catch (SQLException | ParseException e) {
            throw new ODataServiceFault(e, "Error occurred while reading entities from " + tableName + " table. :" +
                                           e.getMessage());
        } finally {
            releaseResources(resultSet, statement);
            releaseConnection(connection);
        }
    }

    /**
     * This method bind values to prepared statement.
     *
     * @param type            data Type
     * @param value           String value
     * @param ordinalPosition Ordinal Position
     * @param sqlStatement    Statement
     * @throws SQLException
     * @throws ParseException
     * @throws ODataServiceFault
     */
    private void bindValuesToPreparedStatement(int type, String value, int ordinalPosition,
                                               PreparedStatement sqlStatement)
            throws SQLException, ParseException, ODataServiceFault {
        byte[] data;
        try {
            switch (type) {
                case Types.INTEGER:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setInt(ordinalPosition, ConverterUtil.convertToInt(value));
                    }
                    break;
                case Types.TINYINT:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setByte(ordinalPosition, ConverterUtil.convertToByte(value));
                    }
                    break;
                case Types.SMALLINT:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setShort(ordinalPosition, ConverterUtil.convertToShort(value));
                    }
                    break;
                case Types.DOUBLE:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setDouble(ordinalPosition, ConverterUtil.convertToDouble(value));
                    }
                    break;
                case Types.VARCHAR:
                /* fall through */
                case Types.CHAR:
				/* fall through */
                case Types.LONGVARCHAR:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setString(ordinalPosition, value);
                    }
                    break;
                case Types.CLOB:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setClob(ordinalPosition, new BufferedReader(new StringReader(value)),
                                             value.length());
                    }
                    break;
                case Types.BOOLEAN:
				/* fall through */
                case Types.BIT:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setBoolean(ordinalPosition, ConverterUtil.convertToBoolean(value));
                    }
                    break;
                case Types.BLOB:
				/* fall through */
                case Types.LONGVARBINARY:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        data = this.getBytesFromBase64String(value);
                        sqlStatement.setBlob(ordinalPosition, new ByteArrayInputStream(data), data.length);
                    }
                    break;
                case Types.BINARY:
				/* fall through */
                case Types.VARBINARY:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        data = this.getBytesFromBase64String(value);
                        sqlStatement.setBinaryStream(ordinalPosition, new ByteArrayInputStream(data), data.length);
                    }
                    break;
                case Types.DATE:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setDate(ordinalPosition, DBUtils.getDate(value));
                    }
                    break;
                case Types.DECIMAL:
				/* fall through */
                case Types.NUMERIC:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setBigDecimal(ordinalPosition, ConverterUtil.convertToBigDecimal(value));
                    }
                    break;
                case Types.FLOAT:
				/* fall through */
                case Types.REAL:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setFloat(ordinalPosition, ConverterUtil.convertToFloat(value));
                    }
                    break;
                case Types.TIME:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setTime(ordinalPosition, DBUtils.getTime(value));
                    }
                    break;
                case Types.LONGNVARCHAR:
				/* fall through */
                case Types.NCHAR:
				/* fall through */
                case Types.NVARCHAR:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setNString(ordinalPosition, value);
                    }
                    break;
                case Types.NCLOB:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setNClob(ordinalPosition, new BufferedReader(new StringReader(value)),
                                              value.length());
                    }
                    break;
                case Types.BIGINT:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setLong(ordinalPosition, ConverterUtil.convertToLong(value));
                    }
                    break;
                case Types.TIMESTAMP:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setTimestamp(ordinalPosition, DBUtils.getTimestamp(value));
                    }
                    break;
                default:
                    if (value == null) {
                        sqlStatement.setNull(ordinalPosition, type);
                    } else {
                        sqlStatement.setString(ordinalPosition, value);
                    }
                    break;
            }
        } catch (DataServiceFault e) {
            throw new ODataServiceFault(e, "Error occurred while binding values. :" + e.getMessage());
        }
    }

    private byte[] getBytesFromBase64String(String base64Str) throws SQLException {
        try {
            return Base64.decodeBase64(base64Str.getBytes(DBConstants.DEFAULT_CHAR_SET_TYPE));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public boolean updateEntityInTable(String tableName, ODataEntry newProperties) throws ODataServiceFault {
        List<String> pKeys = this.primaryKeys.get(tableName);
        Connection connection = null;
        PreparedStatement statement = null;
        String value;
        try {
            connection = initializeConnection();
            String query = createUpdateEntitySQL(tableName, newProperties);
            statement = connection.prepareStatement(query);
            int index = 1;
            for (String column : newProperties.getNames()) {
                if (!pKeys.contains(column)) {
                    value = newProperties.getValue(column);
                    bindValuesToPreparedStatement(this.rdbmsDataTypes.get(tableName).get(column), value, index,
                                                  statement);
                    index++;
                }
            }
            for (String column : newProperties.getNames()) {
                if (!pKeys.isEmpty()) {
                    if (pKeys.contains(column)) {
                        value = newProperties.getValue(column);
                        bindValuesToPreparedStatement(this.rdbmsDataTypes.get(tableName).get(column), value, index,
                                                      statement);
                        index++;
                    }
                } else {
                    throw new ODataServiceFault("Error occurred while updating the entity to " + tableName +
                                                " table. couldn't find keys in the table.");
                }
            }
            statement.execute();
            commitExecution(connection);
            return true;
        } catch (SQLException | ParseException e) {
            throw new ODataServiceFault(e, "Error occurred while updating the entity to " + tableName + " table. :" +
                                           e.getMessage());
        } finally {
            releaseResources(null, statement);
            releaseConnection(connection);
        }
    }

    public boolean updateEntityInTableTransactional(String tableName, ODataEntry oldProperties,
                                                    ODataEntry newProperties) throws ODataServiceFault {
        List<String> pKeys = this.primaryKeys.get(tableName);
        PreparedStatement statement = null;
        Connection connection = null;
        String value;
        try {
            connection = initializeConnection();
            String query = createUpdateEntitySQL(tableName, newProperties);
            statement = connection.prepareStatement(query);
            int index = 1;
            for (String column : newProperties.getNames()) {
                if (!pKeys.contains(column)) {
                    value = newProperties.getValue(column);
                    bindValuesToPreparedStatement(this.rdbmsDataTypes.get(tableName).get(column), value, index,
                                                  statement);
                    index++;
                }
            }
            for (String column : oldProperties.getNames()) {
                if (!pKeys.isEmpty()) {
                    if (pKeys.contains(column)) {
                        value = oldProperties.getValue(column);
                        bindValuesToPreparedStatement(this.rdbmsDataTypes.get(tableName).get(column), value, index,
                                                      statement);
                        index++;
                    }
                } else {
                    throw new ODataServiceFault("Error occurred while updating the entity to " + tableName +
                                                " table. couldn't find keys in the table.");
                }
            }
            statement.execute();
            commitExecution(connection);
            return true;
        } catch (SQLException | ParseException e) {
            throw new ODataServiceFault(e, "Error occurred while updating the entity to " + tableName + " table. :" +
                                           e.getMessage());
        } finally {
            releaseResources(null, statement);
            releaseConnection(connection);
        }
    }

    @Override
    public boolean deleteEntityInTable(String tableName, ODataEntry entry) throws ODataServiceFault {
        List<String> pKeys = this.primaryKeys.get(tableName);
        Connection connection = null;
        PreparedStatement statement = null;
        String value;
        try {
            connection = initializeConnection();
            String query = createDeleteSQL(tableName);
            statement = connection.prepareStatement(query);
            int index = 1;
            for (String column : this.rdbmsDataTypes.get(tableName).keySet()) {
                if (pKeys.contains(column)) {
                    value = entry.getValue(column);
                    bindValuesToPreparedStatement(this.rdbmsDataTypes.get(tableName).get(column), value, index,
                                                  statement);
                    index++;
                }
            }
            statement.execute();
            int rowCount = statement.getUpdateCount();
            commitExecution(connection);
            return rowCount > 0;
        } catch (SQLException | ParseException e) {
            throw new ODataServiceFault(e, "Error occurred while deleting the entity from " + tableName + " table. :" +
                                           e.getMessage());
        } finally {
            releaseResources(null, statement);
            releaseConnection(connection);
        }
    }

    private void addDataType(String tableName, String columnName, int dataType) {
        Map<String, Integer> tableMap = this.rdbmsDataTypes.get(tableName);
        if (tableMap == null) {
            tableMap = new HashMap<>();
            this.rdbmsDataTypes.put(tableName, tableMap);
        }
        tableMap.put(columnName, dataType);
    }

    /**
     * This method wraps result set data in to DataEntry and creates a list of DataEntry.
     *
     * @param tableName Name of the table
     * @param resultSet Result set
     * @return List of DataEntry
     * @throws ODataServiceFault
     * @see DataEntry
     */
    private List<ODataEntry> createDataEntryCollectionFromRS(String tableName, ResultSet resultSet, ExpandOption expandOption)
            throws ODataServiceFault {
        List<ODataEntry> entitySet = new ArrayList<>();
        try {
            String paramValue;
            while (resultSet.next()) {
                ODataEntry entry = new ODataEntry();
                // adds columns of tableName to the entry
                for (String column : this.rdbmsDataTypes.get(tableName).keySet()) {
                    int columnType = this.rdbmsDataTypes.get(tableName).get(column);
                    paramValue = getValueFromResultSet(columnType, column, resultSet);
                    entry.addValue(column, paramValue);
                }
                
                List<ExpandItem> expandItems = null; // tables to expand on
                if (expandOption != null) // if $expand was used
                    expandItems = expandOption.getExpandItems(); // retrieves list of tables to expand on
                if (expandItems != null && expandItems.size() > 0) { // adds columns of foreign tables
                    for (ExpandItem expandItem : expandItems) { // each item corresponds to one table to expand on
                        String foreignTable = expandItem.getResourcePath().getUriResourceParts().get(0).getSegmentValue(); // name of the table
                        for (String column : this.rdbmsDataTypes.get(foreignTable).keySet()) { // loop on columns of the foreign table
                            int columnType = this.rdbmsDataTypes.get(foreignTable).get(column);
                            String foreignValue = getValueFromResultSet(columnType, foreignTable + "." + column, resultSet);
                            entry.addValue(foreignTable + "." + column, foreignValue); // adds column and value to the entry
                        }
                    }
                }
                //Set Etag to the entity
                entry.addValue("ETag", ODataUtils.generateETag(this.configID, tableName, entry));
                entitySet.add(entry);
            }
            return entitySet;
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Error in writing the entities to table. :" + e.getMessage());
        }
    }
    
    private String getValueFromResultSet(int columnType, String column, ResultSet resultSet) throws SQLException {
        String paramValue;
        switch (columnType) {
            case Types.INTEGER:
                /* fall through */
            case Types.TINYINT:
                /* fall through */
            case Types.SMALLINT:
                paramValue = ConverterUtil.convertToString(resultSet.getInt(column));
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
            case Types.DOUBLE:
                paramValue = ConverterUtil.convertToString(resultSet.getDouble(column));
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
            case Types.VARCHAR:
                /* fall through */
            case Types.CHAR:
                /* fall through */
            case Types.CLOB:
                /* fall through */
            case Types.LONGVARCHAR:
                paramValue = resultSet.getString(column);
                break;
            case Types.BOOLEAN:
                /* fall through */
            case Types.BIT:
                paramValue = ConverterUtil.convertToString(resultSet.getBoolean(column));
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
            case Types.BLOB:
                Blob sqlBlob = resultSet.getBlob(column);
                if (sqlBlob != null) {
                    paramValue = this.getBase64StringFromInputStream(sqlBlob.getBinaryStream());
                } else {
                    paramValue = null;
                }
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
            case Types.BINARY:
                /* fall through */
            case Types.LONGVARBINARY:
                /* fall through */
            case Types.VARBINARY:
                InputStream binInStream = resultSet.getBinaryStream(column);
                if (binInStream != null) {
                    paramValue = this.getBase64StringFromInputStream(binInStream);
                } else {
                    paramValue = null;
                }
                break;
            case Types.DATE:
                Date sqlDate = resultSet.getDate(column);
                if (sqlDate != null) {
                    paramValue = ConverterUtil.convertToString(sqlDate);
                } else {
                    paramValue = null;
                }
                break;
            case Types.DECIMAL:
                /* fall through */
            case Types.NUMERIC:
                BigDecimal bigDecimal = resultSet.getBigDecimal(column);
                if (bigDecimal != null) {
                    paramValue = ConverterUtil.convertToString(bigDecimal);
                } else {
                    paramValue = null;
                }
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
            case Types.FLOAT:
                paramValue = ConverterUtil.convertToString(resultSet.getFloat(column));
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
            case Types.TIME:
                Time sqlTime = resultSet.getTime(column);
                if (sqlTime != null) {
                    paramValue = this.convertToTimeString(sqlTime);
                } else {
                    paramValue = null;
                }
                break;
            case Types.LONGNVARCHAR:
                /* fall through */
            case Types.NCHAR:
                /* fall through */
            case Types.NCLOB:
                /* fall through */
            case Types.NVARCHAR:
                paramValue = resultSet.getNString(column);
                break;
            case Types.BIGINT:
                paramValue = ConverterUtil.convertToString(resultSet.getLong(column));
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
            case Types.TIMESTAMP:
                Timestamp sqlTimestamp = resultSet.getTimestamp(column);
                if (sqlTimestamp != null) {
                    paramValue = this.convertToTimestampString(sqlTimestamp);
                } else {
                    paramValue = null;
                }
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
            /* handle all other types as strings */
            default:
                paramValue = resultSet.getString(column);
                paramValue = resultSet.wasNull() ? null : paramValue;
                break;
        }
        return paramValue;
    }

    private void releaseResources(ResultSet resultSet, Statement statement) {
        /* close the result set */
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception ignore) {
                // ignore
            }
        }
        /* close the statement */
        if (statement != null) {
            try {
                statement.close();
            } catch (Exception ignore) {
                // ignore
            }
        }

    }
    
    private String getBase64StringFromInputStream(InputStream in) throws SQLException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        String strData;
        try {
            byte[] buff = new byte[512];
            int i;
            while ((i = in.read(buff)) > 0) {
                byteOut.write(buff, 0, i);
            }
            in.close();
            byte[] base64Data = Base64.encodeBase64(byteOut.toByteArray());
            if (base64Data != null) {
                strData = new String(base64Data, DBConstants.DEFAULT_CHAR_SET_TYPE);
            } else {
                strData = null;
            }
            return strData;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * This method reads table column meta data.
     *
     * @param tableName Name of the table
     * @return table MetaData
     * @throws ODataServiceFault
     */
    private Map<String, DataColumn> readTableColumnMetaData(String tableName, DatabaseMetaData meta)
            throws ODataServiceFault {
        ResultSet resultSet = null;
        Map<String, DataColumn> columnMap = new HashMap<>();
        String name = "";
        String type ="";
        try {
            
            int j;
            String schema = this.oDataTableSchema.get(tableName);
            List<ODataColumnsConfig> definedCols = this.oDataColumnsConfig.get(schema+"."+tableName);
            Map<String,String> colTypeMap = new HashMap<String,String>();
            if(definedCols != null) {
                for(j=0;j<definedCols.size();j++) {
                    name = definedCols.get(j).getColumnName();
                    type = definedCols.get(j).getType();
                    colTypeMap.put(name, type);
                }
            }
            if (meta.getDatabaseProductName().toLowerCase().contains(ORACLE_SERVER)) {
                resultSet = meta.getColumns(null, meta.getUserName(), tableName, null);
            } else {
                resultSet = meta.getColumns(null, null, tableName, null);
            }
            int i = 1;
            while (resultSet.next()) {
            	String columnName = resultSet.getString("COLUMN_NAME");
            	if(colTypeMap.keySet().contains(columnName) || colTypeMap.size()==0) { // if no columns specified show all table's columns, otherwise show only the specified ones
	                int columnType = resultSet.getInt("DATA_TYPE");
	                ODataDataType dataType = (colTypeMap.size()==0 ? getODataDataType(columnType) : ODataDataType.valueOf(colTypeMap.get(columnName)) );
	                int size = resultSet.getInt("COLUMN_SIZE");
	                boolean nullable = resultSet.getBoolean("NULLABLE");
	                String columnDefaultVal = resultSet.getString("COLUMN_DEF");
	                String autoIncrement = "";
	                try { // Oracle 11g and older versions have no concept of AUTO_INCREMENT, so this will throw an exception
	                    autoIncrement = resultSet.getString("IS_AUTOINCREMENT").toLowerCase();
	                } catch (SQLException e) { // to allow some compatibility with 11g and older, we enclose it in a try-catch segment that does nothing
	                }
	                boolean isAutoIncrement = false;
	                if (autoIncrement.contains("yes") || autoIncrement.contains("true")) {
	                    isAutoIncrement = true;
	                }
	                DataColumn column = new DataColumn(columnName, dataType, i, nullable, size,
	                                                   isAutoIncrement);
	                if (null != columnDefaultVal) {
	                    column.setDefaultValue(columnDefaultVal);
	                }
	                if (Types.DOUBLE == columnType || Types.FLOAT == columnType || Types.DECIMAL == columnType ||
	                    Types.NUMERIC == columnType || Types.REAL == columnType) {
	                    int scale = resultSet.getInt("DECIMAL_DIGITS");
	                    column.setPrecision(size);
	                    if (scale == 0) {
	                        //setting default scale as 5
	                        scale = 5;
	                        column.setScale(scale);
	                    } else {
	                        column.setScale(scale);
	                    }
	                    if (dbType.contains(ORACLE_SERVER)) { // for Oracle servers, which do not set scale and precision properly
	                        if (size < 1)
	                            column.setPrecision(1);
	                        if (scale < 0)
	                            column.setScale(5);
	                    }
	                }
	                columnMap.put(columnName, column);
	                addDataType(tableName, columnName, columnType);
	                i++;
            	}	
            }
            return columnMap;
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Error in reading table meta data in " + tableName + " table. :" +
                                           e.getMessage());
        } finally {
            releaseResources(resultSet, null);
        }
    }

    /**
     * This method initializes metadata.
     *
     * @throws ODataServiceFault
     */
    private void initializeMetaData() throws ODataServiceFault {
        this.tableMetaData = new HashMap<>();
        this.primaryKeys = new HashMap<>();
        this.navigationProperties = new HashMap<>();
        Connection connection = null;
        try {
            connection = initializeConnection();
            DatabaseMetaData metadata = connection.getMetaData();
            String catalog = connection.getCatalog();
            for (String tableName : this.tableList) {
                this.tableMetaData.put(tableName, readTableColumnMetaData(tableName, metadata));
                this.primaryKeys.put(tableName, readTablePrimaryKeys(tableName, metadata, catalog));
            }
            for (String tableName : this.tableList) { // executes this in a separate loop because it needs the tableMetaData structure to be complete
                NavigationTable nt = readForeignKeys(tableName, metadata, catalog, metadata.storesLowerCaseIdentifiers(), metadata.storesUpperCaseIdentifiers());
                if (nt != null)
                    this.navigationProperties.put(tableName, nt);
            }
            if (dbType.contains(MSSQL_SERVER))
                fillNavPropertiesForMSSQL(metadata, catalog, metadata.storesLowerCaseIdentifiers(), metadata.storesUpperCaseIdentifiers());
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Error in reading tables from the database. :" + e.getMessage());
        } finally {
            releaseConnection(connection);
        }
    }

    /**
     * This method creates a list of tables available in the DB.
     *
     * @return Table List of the DB
     * @throws ODataServiceFault
     */
    private List<String> generateTableList(List<String> oDataTableList) throws ODataServiceFault {
        List<String> tableList = new ArrayList<>();
        Connection connection = null;
        ResultSet rs = null;
        try {
            connection = initializeConnection();
            DatabaseMetaData meta = connection.getMetaData();
            String lowerCaseDB = meta.getDatabaseProductName().toLowerCase();
            
            if (lowerCaseDB.contains(ORACLE_SERVER)) {
                rs = meta.getTables(null, meta.getUserName(), null, new String[] { TABLE, VIEW });
            } else if (lowerCaseDB.contains(MSSQL_SERVER)) {
                rs = meta.getTables(null, connection.getSchema(), null, new String[] { TABLE, VIEW });
            } else if (lowerCaseDB.contains(MYSQL) || lowerCaseDB.contains(POSTGRESQL) || lowerCaseDB.contains(H2)) {
                rs = meta.getTables(connection.getCatalog(), null, "%", new String[] { TABLE, VIEW });
            } else {
                rs = meta.getTables(null, null, null, new String[] { TABLE, VIEW });
            }
            while (rs.next()) {
                String tableName = rs.getString(TABLE_NAME);
                if(oDataTableList.contains(tableName)) {
                    tableList.add(tableName);
                }
            }
            return tableList;
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Error in reading tables from the database. :" + e.getMessage());
        } finally {
            releaseResources(rs, null);
            releaseConnection(connection);
        }
    }

    /**
     * This method reads primary keys of the table.
     *
     * @param tableName Name of the table
     * @return primary key list
     * @throws ODataServiceFault
     */
    private List<String> readTablePrimaryKeys(String tableName, DatabaseMetaData metaData, String catalog)
            throws ODataServiceFault {
        ResultSet resultSet = null;
        List<String> keys = new ArrayList<>();
        try {
            if (metaData.getDatabaseProductName().toLowerCase().contains(ORACLE_SERVER)) {
                resultSet = metaData.getPrimaryKeys(catalog, metaData.getUserName(), tableName);
            } else {
                resultSet = metaData.getPrimaryKeys(catalog, null, tableName);
            }
            while (resultSet.next()) {
                String primaryKey = resultSet.getString("COLUMN_NAME");
                keys.add(primaryKey);
            }
            return keys;
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Error in reading table primary keys in " + tableName + " table. :" +
                                           e.getMessage());
        } finally {
            releaseResources(resultSet, null);
        }
    }

    /**
     * This method reads foreign keys of the table.
     *
     * @param tableName Name of the table
     * @throws ODataServiceFault
     */
    private NavigationTable readForeignKeys(String tableName, DatabaseMetaData metaData, String catalog, boolean storesLower, boolean storesUpper)
            throws ODataServiceFault {
        ResultSet resultSetExp = null;
        ResultSet resultSetImp = null;
        
        if (foreignKeys == null)
            foreignKeys = new HashMap<String, Map<String, List<ForeignKey>>>(); // initializes the Table->Foreign Keys map
        Map<String, List<ForeignKey>> tableFKs = foreignKeys.get(tableName); // Map: Foreign table name -> Foreign keys to reach such foreign table
        if (tableFKs == null) {
            tableFKs = new HashMap<String, List<ForeignKey>>();
            foreignKeys.put(tableName, tableFKs);
        }
        
        try {
            resultSetExp = metaData.getExportedKeys(catalog, null, tableName); // Retrieves the set of foreign keys of other tables that point to this table. MicroSoft SQL does not seem to work with this method, which will always return an empty set.
            resultSetImp = metaData.getImportedKeys(catalog, null, tableName); // Retrieves the set of foreign keys of this table that point to other tables.
            NavigationTable navigationLinks = new NavigationTable();
            if (!dbType.equals(MSSQL_SERVER)) { // getExportedKeys does not work with MicroSoft SQL, so this would just throw exceptions
                while (resultSetExp.next()) { 
                    String primaryKeyColumnName = resultSetExp.getString("PKCOLUMN_NAME");
                    String foreignKeyTableName = resultSetExp.getString("FKTABLE_NAME"); // name of the foreign table related to tableName
                    String foreignKeyColumnName = resultSetExp.getString("FKCOLUMN_NAME");
                    for (String s : tableMetaData.get(tableName).keySet()) { // corrects case
                        if ( (storesLower && s.toLowerCase().equals(primaryKeyColumnName)) ||
                                (storesUpper && s.toUpperCase().equals(primaryKeyColumnName)) )
                            primaryKeyColumnName = s;
                    }
                    List<NavigationKeys> columnList = navigationLinks.getNavigationKeys(foreignKeyTableName);
                    if (columnList == null) {
                        columnList = new ArrayList<>();
                        navigationLinks.addNavigationKeys(foreignKeyTableName, columnList);
                    }
                    columnList.add(new NavigationKeys(primaryKeyColumnName, foreignKeyColumnName));
                    
                    List<ForeignKey> toForeignTableFKs = tableFKs.get(foreignKeyTableName); // foreign keys that connect the two tables
                    if (toForeignTableFKs == null) {
                        toForeignTableFKs = new ArrayList<ForeignKey>();
                        tableFKs.put(foreignKeyTableName,  toForeignTableFKs); // adds the list of foreign keys involved in the connection
                    }
                    ForeignKey fk = new ForeignKey(ForeignKey.FKType.EXPORTED, resultSetExp.getString("PKTABLE_CAT"), resultSetExp.getString("PKTABLE_NAME"), primaryKeyColumnName,
                            resultSetExp.getString("FKTABLE_CAT"), foreignKeyTableName, foreignKeyColumnName);
                    toForeignTableFKs.add(fk); // adds the foreign key to the list
                }
            }
            
            try { // MicroSoft SQL throws an exception when there are no more elements, instead of just returning false, so this try-catch is necessary
                while (resultSetImp.next()) { // thankfully at least getImportedKeys works in MicroSoft SQL
                    String foreignKeyTableName = resultSetImp.getString("PKTABLE_NAME");
                    String foreignKeyColumnName = resultSetImp.getString("PKCOLUMN_NAME");
                    for (String s : tableMetaData.get(foreignKeyTableName).keySet()) { // corrects case
                        if ( (storesLower && s.toLowerCase().equals(foreignKeyColumnName)) ||
                                (storesUpper && s.toUpperCase().equals(foreignKeyColumnName)) )
                         foreignKeyColumnName = s;
                    }
                    
                    List<ForeignKey> toForeignTableFKs = tableFKs.get(foreignKeyTableName); // foreign keys that connect the two tables
                    if (toForeignTableFKs == null) {
                        toForeignTableFKs = new ArrayList<ForeignKey>();
                        tableFKs.put(foreignKeyTableName, toForeignTableFKs); // adds the list of foreign keys involved in the connection
                    }
                    ForeignKey fk = new ForeignKey(ForeignKey.FKType.IMPORTED, resultSetImp.getString("FKTABLE_CAT"), resultSetImp.getString("FKTABLE_NAME"), resultSetImp.getString("FKCOLUMN_NAME"),
                            resultSetImp.getString("PKTABLE_CAT"), foreignKeyTableName, foreignKeyColumnName);
                    toForeignTableFKs.add(fk); // adds the foreign key to the list
                    
                    if (dbType.contains(MSSQL_SERVER)) { // this section is only for MicroSoft SQL: since getExportedKeys does not work with it, we need to build exported keys here
                        Map<String, List<ForeignKey>> foreignTableFKs = foreignKeys.get(foreignKeyTableName);
                        if (foreignTableFKs == null) {
                            foreignTableFKs = new HashMap<String, List<ForeignKey>>();
                            foreignKeys.put(foreignKeyTableName, foreignTableFKs);
                        }
                        List<ForeignKey> toTableFKs = foreignTableFKs.get(tableName); // foreign keys that connect the two tables
                        if (toTableFKs == null) {
                            toTableFKs = new ArrayList<ForeignKey>();
                            foreignTableFKs.put(tableName, toTableFKs); // adds the list of foreign keys involved in the connection
                        }
                        ForeignKey fkMS = new ForeignKey(ForeignKey.FKType.EXPORTED, resultSetImp.getString("PKTABLE_CAT"),foreignKeyTableName,foreignKeyColumnName,
                                resultSetImp.getString("FKTABLE_CAT"), resultSetImp.getString("FKTABLE_NAME"), resultSetImp.getString("FKCOLUMN_NAME"));
                        toTableFKs.add(fkMS);
                    }
                }
            } catch (SQLException e) {
                if (dbType.contains(MSSQL_SERVER)){ // do nothing
                } else
                    throw new ODataServiceFault(e, e.getMessage()); // error occurred
            }
            if (dbType.contains(MSSQL_SERVER))
                return null;
            return navigationLinks;
        } catch (SQLException e) {
            throw new ODataServiceFault(e, "Error in reading " + tableName + " table meta data. :" + e.getMessage());
        } finally {
            releaseResources(resultSetExp, null);
            releaseResources(resultSetImp, null);
        }
    }
    
    private void fillNavPropertiesForMSSQL(DatabaseMetaData metaData, String catalog, boolean storesLower, boolean storesUpper) {
        for (String tableName : tableList) {
            NavigationTable navigationLinks = new NavigationTable();
            Map<String, List<ForeignKey>> tableFKs = foreignKeys.get(tableName);
            for (String foreignTable : tableFKs.keySet()) {
            	List<NavigationKeys> columnList = new ArrayList<>();
                for (ForeignKey fk : tableFKs.get(foreignTable)) {
                    if (fk.getType() == ForeignKey.FKType.EXPORTED) {
                        columnList.add(new NavigationKeys(fk.getName(),fk.getForeignName()));
                    }
                }
                if (columnList.size() > 0)
                    navigationLinks.addNavigationKeys(foreignTable, columnList);
            }
            navigationProperties.put(tableName,  navigationLinks);
        }
    }

    @Override
    public Map<String, Map<String, DataColumn>> getTableMetadata() {
        return this.tableMetaData;
    }

    /**
     * This method creates a SQL query to update data.
     *
     * @param tableName  Name of the table
     * @param properties Properties
     * @return sql Query
     */
    private String createUpdateEntitySQL(String tableName, ODataEntry properties) {
        List<String> pKeys = primaryKeys.get(tableName);
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableName).append(" SET ");
        boolean propertyMatch = false;
        for (String column : properties.getNames()) {
            if (!pKeys.contains(column)) {
                if (propertyMatch) {
                    sql.append(",");
                }
                sql.append(column).append(" = ").append(" ? ");
                propertyMatch = true;
            }
        }
        sql.append(" WHERE ");
        // Handling keys
        propertyMatch = false;
        for (String key : pKeys) {
            if (propertyMatch) {
                sql.append(" AND ");
            }
            sql.append(key).append(" = ").append(" ? ");
            propertyMatch = true;
        }
        return sql.toString();
    }

    /**
     * This method creates a SQL query to insert data in table.
     *
     * @param tableName Name of the table
     * @return sqlQuery
     */
    private String createInsertSQL(String tableName, ODataEntry entry) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");
        boolean propertyMatch = false;
        for (String column : entry.getNames()) {
            if (this.rdbmsDataTypes.get(tableName).keySet().contains(column)) {
                if (propertyMatch) {
                    sql.append(",");
                }
                sql.append(column);
                propertyMatch = true;
            }
        }
        sql.append(" ) VALUES ( ");
        propertyMatch = false;
        for (String column : entry.getNames()) {
            if (this.rdbmsDataTypes.get(tableName).keySet().contains(column)) {
                if (propertyMatch) {
                    sql.append(",");
                }
                sql.append("?");
                propertyMatch = true;
            }
        }
        sql.append(" ) ");
        return sql.toString();
    }

    /**
     * This method creates SQL query to read data with keys.
     *
     * @param dbName		Name of the database the table belongs to
     * @param tableName 	Name of the table
     * @param keys      	Keys
     * @param expandOption	Used to handle the $expand command
     * @return sql 			The SQL query to read data with keys
     * @throws ODataServiceFault	handleExpand: FilterExpressionVisitor: error building JOIN command while visiting the $filter nested within $expand
     */
    private String createReadSqlWithKeys(String dbName, String tableName, ODataEntry keys, ExpandOption expandOption) throws ODataServiceFault {
        StringBuilder sql = new StringBuilder();
        RDBMSODataQuery rdbmsQuery = new RDBMSODataQuery();
        rdbmsQuery.setDBType(this.dbType);
        if (expandOption != null) {
            try {
                handleExpand(rdbmsQuery, dbName, tableName, expandOption); // returns proper SELECT statement if there are JOINs to perform
            } catch ( ODataApplicationException | ExpressionVisitException e) { // error, if triggered it's likely that it was caused by a $filter nested within the $expand
                throw new ODataServiceFault(e, "There was an error while handling the $expand command: " + e.getMessage());
            }
        }
        
        if (rdbmsQuery.getSelect() == null || rdbmsQuery.getSelect().size() == 0) { // There is no expand option, so we can just SELECT * from the table
            rdbmsQuery.addSelect("*");
            rdbmsQuery.setFrom(tableName);
        }
        sql.append(rdbmsQuery.printSelect() + rdbmsQuery.printFrom()).append(" WHERE ");
        String columnPrefix = "";
        if (dbName != null && !dbName.equals("") && dbType.contains(MYSQL))
            columnPrefix = dbName + ".";
        boolean propertyMatch = false;
        for (String column : this.rdbmsDataTypes.get(tableName).keySet()) {
            if (keys.getNames().contains(column)) {
                if (propertyMatch) {
                    sql.append(" AND ");
                }
                sql.append(columnPrefix + tableName + "." + column).append(" = ").append(" ? ");
                propertyMatch = true;
            }
        }
        
        if (rdbmsQuery.getOrderBy() != null && rdbmsQuery.getOrderBy().size() != 0) {
            sql.append(rdbmsQuery.printOrderBy());
        }
        
        return sql.toString();
    }

    /**
     * This method creates SQL query to delete data.
     *
     * @param tableName Name of the table
     * @return sql Query
     */
    private String createDeleteSQL(String tableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName).append(" WHERE ");
        List<String> pKeys = primaryKeys.get(tableName);
        boolean propertyMatch = false;
        for (String key : pKeys) {
            if (propertyMatch) {
                sql.append(" AND ");
            }
            sql.append(key).append(" = ").append(" ? ");
            propertyMatch = true;
        }
        return sql.toString();
    }

    private ODataDataType getODataDataType(int columnType) {
        ODataDataType dataType;
        switch (columnType) {
            case Types.INTEGER:
                dataType = ODataDataType.INT32;
                break;
            case Types.TINYINT:
				/* fall through */
            case Types.SMALLINT:
                dataType = ODataDataType.INT16;
                break;
            case Types.DOUBLE:
                dataType = ODataDataType.DOUBLE;
                break;
            case Types.VARCHAR:
				/* fall through */
            case Types.CHAR:
				/* fall through */
            case Types.LONGVARCHAR:
				/* fall through */
            case Types.CLOB:
				/* fall through */
            case Types.LONGNVARCHAR:
				/* fall through */
            case Types.NCHAR:
				/* fall through */
            case Types.NVARCHAR:
				/* fall through */
            case Types.NCLOB:
				/* fall through */
            case Types.SQLXML:
                dataType = ODataDataType.STRING;
                break;
            case Types.BOOLEAN:
				/* fall through */
            case Types.BIT:
                dataType = ODataDataType.BOOLEAN;
                break;
            case Types.BLOB:
				/* fall through */
            case Types.BINARY:
				/* fall through */
            case Types.LONGVARBINARY:
				/* fall through */
            case Types.VARBINARY:
                dataType = ODataDataType.BINARY;
                break;
            case Types.DATE:
                dataType = ODataDataType.DATE;
                break;
            case Types.DECIMAL:
				/* fall through */
            case Types.NUMERIC:
                dataType = ODataDataType.DECIMAL;
                break;
            case Types.FLOAT:
				/* fall through */
            case Types.REAL:
                dataType = ODataDataType.SINGLE;
                break;
            case Types.TIME:
                dataType = ODataDataType.TIMEOFDAY;
                break;
            case Types.BIGINT:
                dataType = ODataDataType.INT64;
                break;
            case Types.TIMESTAMP:
                dataType = ODataDataType.DATE_TIMEOFFSET;
                break;
            default:
                dataType = ODataDataType.STRING;
                break;
        }
        return dataType;
    }

    private Connection initializeConnection() throws SQLException {
        if (getTransactionalConnection() == null) {
            return this.dataSource.getConnection();
        }
        return getTransactionalConnection();
    }

    private void commitExecution(Connection connection) throws SQLException {
        if (getTransactionalConnection() == null) {
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    private void releaseConnection(Connection connection) {
        if (getTransactionalConnection() == null) {
            /* close the connection */
            try {
                connection.close();
            } catch (Exception ignore) {
                // ignore
            }
        }
    }
    
	/**
	 * Returns the map of foreign keys
	 * 
	 * @return	Map of foreign keys
	 */
	public Map<String, Map<String, List<ForeignKey>>> getForeignKeys() {
		return this.foreignKeys;
	}
	
	/**
	 * Returns the list of columns that make up the foreign key that links the two input tables together
	 * 
	 * @param tableName		First table to link
	 * @param foreignTable	Other table to link
	 * @return				The list of columns that make up the foreign key that links the two tables together
	 */
	public List<ForeignKey> getForeignKeyBetween(String tableName, String foreignTable) {
		Map<String, List<ForeignKey>> tableFKs = foreignKeys.get(tableName);
		if (tableFKs != null)
			return tableFKs.get(foreignTable);
		return null;
	}
	
	/**
	 * Prints foreign keys belonging to all tables. For debugging purposes only.
	 */
	private void printForeignKeys() {
		System.out.println("Database Type: " + dbType);
		for (String table : tableList) {
			System.out.println("FOREIGN TABLE FOR " + table);
			Map <String, List<ForeignKey>> foreignTables = foreignKeys.get(table);
			if (foreignTables != null) {
				for (String foreignTable : foreignTables.keySet()) {
					System.out.println(" To " + foreignTable + ": ");
					for (ForeignKey zkey : foreignTables.get(foreignTable)) {
						System.out.println("  " + zkey);
					}
				}
			}
			System.out.println("");
		}
	}
	
	/**
	 * Prints the navigationProperties map. For debugging purposes only.
	 */
	private void printNavigationProperties() {
		System.out.println("Database Type: " + dbType);
		for (String table : tableList) {
			System.out.println("NAVIGATION PROPERTIES FOR " + table);
			NavigationTable navTable = navigationProperties.get(table);
			if (navTable != null) {
				for (String foreignTable : navTable.getTables()) {
					System.out.println(" To " + foreignTable + ": ");
					for (NavigationKeys nk : navTable.getNavigationKeys(foreignTable)) {
						System.out.println("  " + nk.getForeignKey() + " -> " + nk.getPrimaryKey());
					}
				}
			}
			System.out.println("");
		}
	}
}
