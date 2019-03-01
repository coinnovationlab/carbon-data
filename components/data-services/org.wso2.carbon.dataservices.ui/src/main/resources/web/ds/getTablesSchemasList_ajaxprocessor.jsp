<!--
 ~ Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 ~
 ~ WSO2 Inc. licenses this file to you under the Apache License,
 ~ Version 2.0 (the "License"); you may not use this file except
 ~ in compliance with the License.
 ~ You may obtain a copy of the License at
 ~
 ~    http://www.apache.org/licenses/LICENSE-2.0
 ~
 ~ Unless required by applicable law or agreed to in writing,
 ~ software distributed under the License is distributed on an
 ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ~ KIND, either express or implied.  See the License for the
 ~ specific language governing permissions and limitations
 ~ under the License.
 -->
<%@page import="java.io.StringWriter"%>
<%@page import="org.apache.commons.io.IOUtils"%>
<%@page import="java.io.InputStreamReader"%>
<%@ page import="java.util.List" %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="java.util.Arrays" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.HashMap" %>
<%@ page import="org.wso2.carbon.ui.CarbonUIUtil" %>
<%@ page import="org.wso2.carbon.CarbonConstants" %>
<%@ page import="org.apache.axis2.context.ConfigurationContext" %>
<%@ page import="org.wso2.carbon.dataservices.ui.DataServiceAdminClient"%>
<%@ page import="org.wso2.carbon.utils.ServerConstants" %>
<%@ page import="org.apache.axis2.AxisFault"%>
<%@ page import="org.wso2.carbon.CarbonError" %>
<%@ page import="java.io.PrintWriter" %>
<%@ page import="org.owasp.encoder.Encode" %>
<%@ page import="org.wso2.carbon.dataservices.core.odata.DataColumn" %>
<%@ page import="org.wso2.carbon.dataservices.ui.beans.Data" %>
<%@ page import="org.apache.axiom.om.util.AXIOMUtil" %>
<%@ page import="org.apache.axiom.om.OMElement" %>
<%@ page import="java.util.Iterator" %>
<%@ page import="org.wso2.carbon.dataservices.ui.beans.Config" %>
<%@ page import="org.wso2.carbon.dataservices.ui.beans.Property" %>
<%@ page import="org.wso2.carbon.dataservices.common.conf.DynamicODataConfig" %>
<%@ page import="org.wso2.carbon.dataservices.common.conf.ODataTableSchemaConfig" %>
<%@ page import="org.wso2.carbon.dataservices.common.DBConstants" %>
<%@ page import="org.wso2.carbon.dataservices.ui.stub.admin.core.xsd.GeneratedListTables" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<fmt:bundle basename="org.wso2.carbon.dataservices.ui.i18n.Resources">
<script type="text/javascript" src="../ajax/js/prototype.js"></script>
<script type="text/javascript" src="../resources/js/resource_util.js"></script>
<jsp:include page="../resources/resources-i18n-ajaxprocessor.jsp"/>
<link rel="stylesheet" type="text/css" href="../resources/css/registry.css"/>
<link rel="stylesheet" type="text/css" href="css/datasource.css" media="all"/>

<carbon:breadcrumb
        label="Add Datasource"
        resourceBundle="org.wso2.carbon.dataservices.ui.i18n.Resources"
        topPage="false"
        request="<%=request%>"/>


<%
String driverClass = request.getParameter("driver");
String jdbcUrl = Encode.forHtmlContent(request.getParameter("jdbcUrl"));
String userName = request.getParameter("userName");
String password = request.getParameter("password");
String passwordAlias = request.getParameter("passwordAlias");
String choosenSchema = request.getParameter("schema");
String tablename = request.getParameter("tablename");
String serviceId = request.getParameter("serviceId");
String configId = request.getParameter("configId");
boolean createView = Boolean.parseBoolean(request.getParameter("createView"));

    boolean schemaSupport = true;
   	String backendServerURL = CarbonUIUtil.getServerURL(config.getServletContext(), session);
   	ConfigurationContext configContext = (ConfigurationContext) config.getServletContext().getAttribute(CarbonConstants.CONFIGURATION_CONTEXT);
   	String cookie = (String) session.getAttribute(ServerConstants.ADMIN_SERVICE_COOKIE);
   	DataServiceAdminClient client = new DataServiceAdminClient(cookie, backendServerURL, configContext);
   	Data dataService = new Data();
   	List<String> dynamicTableList = new ArrayList<String>();
   	DynamicODataConfig dynamicODataConfig;
   	List<ODataTableSchemaConfig> dynamicOdataTableList = new ArrayList<ODataTableSchemaConfig>();
   	if (configId != null && configId.trim().length() > 0 && createView == false) {
	   	String serviceContent = client.getDataServiceContents(serviceId);
	   	OMElement dynContentEl = AXIOMUtil.stringToOM(serviceContent);
	   	dataService.populate(dynContentEl);
	   	Config dsConfig = dataService.getConfig(configId);
	   	ArrayList configProperties = dsConfig.getProperties();
	   	Iterator propertyIterator = configProperties.iterator();
        if (propertyIterator != null) {
            while (propertyIterator.hasNext()) {
                Property property = (Property) propertyIterator.next();
                String propertyName = property.getName();
                Object propertyValue = property.getValue();
                if (property.getValue() instanceof DynamicODataConfig) {
                    if (propertyName.equals(DBConstants.RDBMS.DYNAMIC_ODATA_TABLE_MAPPING)) {
                    	dynamicODataConfig = (DynamicODataConfig)propertyValue;
                    	dynamicOdataTableList = dynamicODataConfig.getTables();
                        for(int count=0;count<dynamicOdataTableList.size();count++){
                        	dynamicTableList.add(dynamicOdataTableList.get(count).getSchemaName()+"."+dynamicOdataTableList.get(count).getTableName());
                        }
                    }
                }
            }
        }
   	}
   	
   	GeneratedListTables [] tableList2 = {};
   	GeneratedListTables [] viewsList2 = {};
   	String [] schemaList = {};
   	if( driverClass != "" && jdbcUrl != "" && userName != "" ){
   		schemaList = client.getdbSchemaListUsingParams(driverClass, jdbcUrl, userName, password, passwordAlias);
   		if(schemaList==null) schemaSupport = false; //schemaList = new String[1];
   		tableList2 = client.generateTableList(driverClass, jdbcUrl, userName, password, passwordAlias,schemaList,"TABLE" );
   		viewsList2 = client.generateTableList(driverClass, jdbcUrl, userName, password, passwordAlias,schemaList,"VIEW" );
   		if(!schemaSupport){ 
   			schemaList = new String[1];
			schemaList[0] = DBConstants.NO_SCHEMA; // support dbs that don't use schema
		} 
   	}
    	
%>
		<input type="hidden" id="schemaSupport" value="<%= schemaSupport%>"/>
	    <table>
		    <tr>
               <td><fmt:message key="odata.choose.schema"/>:</td>
               <td><select id="schema_list" name="schema_list" onchange="reloadOdataObjects()">
                           <% for(int i=0;i<schemaList.length;i++){%>
                           		<option value="<%=schemaList[i] %>"><%=schemaList[i] %></option>
                           <%} %>
                    </select>
               </td>
		    </tr>
		    <tr>
		    	<td colspan="2">
		    		<input class="button" type="button" value="<fmt:message key="odata.select.all"/>" onclick="select_unselect('tablesOdata',false);return false;"/>
                </td>
		    </tr>
	    </table>
	    <% 
	    Map<String,List<String>> tableListAll = new HashMap<String,List<String>>();
	    Map<String,List<String>> viewsListAll = new HashMap<String,List<String>>();
	    for(int j=0;j<schemaList.length;j++){
	    	String [] tableArray = {};
	    	String [] viewsArray = {};
	    	for(int k=0;k<tableList2.length;k++){
	    		GeneratedListTables currSchemaTables = tableList2[k];
	    		GeneratedListTables currSchemaViews = viewsList2[k];    		
	    		if(currSchemaTables.getSchemaName().equals(schemaList[j]) || currSchemaTables.getSchemaName().equals(DBConstants.NO_SCHEMA)){
	    			if(currSchemaTables.getTables() != null){
	    				tableArray = (String [])currSchemaTables.getTables();
	    			}
	    			if(currSchemaViews.getTables() != null){
	    				viewsArray = (String [])currSchemaViews.getTables();
	    			}
	    			break;
	    		}
	    	}
	    	List<String> tableList = new ArrayList<String>(Arrays.asList(tableArray ));
	    	List<String> viewsList = new ArrayList<String>(Arrays.asList(viewsArray ));
	    	tableListAll.put(schemaList[j],tableList);
	    	viewsListAll.put(schemaList[j],viewsList);
	    	%>
	    	
	    <input type="hidden" name="listAllSchemasTables" id="schemaTable__<%=schemaList[j]%>" value="<%= String.join(";", tableList)%>"/>
	    <input type="hidden" name="listAllSchemasViews" id="schemaView__<%=schemaList[j]%>" value="<%= String.join(";", viewsList)%>"/>
	    <div id="<%= schemaList[j] %>" style="display:<%= ((j==0) ? "inline" : "none")%>">
	    <div class="tab">
			  <span class="tablinks" id="Tables<%= j %>Header" onclick="openTabContent(event, 'Tables<%= j %>')">Tables</span>
			  <span class="tablinks" id="Views<%= j %>Header" onclick="openTabContent(event, 'Views<%= j %>')">Views</span>
			</div>
			
			<div id="Tables<%= j %>" class="tabcontent">
			  <h3>List of Tables</h3>
			  <p><table>
	    		<% for(int i=0;i<tableList.size();i=i+5){%>
		    	<tr>
	    			<td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+"_"+schemaList[j] %>" value="<%=tableList.get(i)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+tableList.get(i)) || dynamicTableList.size()==0  ? "checked='checked'" : ""  %> onchange="addTableToList(this,'opttables')"/></td>
	    			<td><label for="tablesOdata<%=i+"_"+schemaList[j] %>" ><%=tableList.get(i) %></label></td>
	    			<td></td><td></td>
                    <% if (i+1<tableList.size() ) { %>
                    <td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+1 +"_"+schemaList[j]%>" value="<%=tableList.get(i+1)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+tableList.get(i+1)) || dynamicTableList.size()==0  ? "checked='checked'" : ""  %> onchange="addTableToList(this,'opttables')"/></td>
	    			<td><label for="tablesOdata<%=i+1 +"_"+schemaList[j]%>"><%=tableList.get(i+1) %></label></td>
	    			<td></td><td></td>
                    <%} if (i+2<tableList.size() ) {%>
                    <td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+2 +"_"+schemaList[j]%>" value="<%=tableList.get(i+2)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+tableList.get(i+2)) || dynamicTableList.size()==0  ? "checked='checked'" : ""  %> onchange="addTableToList(this,'opttables')"/></td>
	    			<td><label for="tablesOdata<%=i+2+"_"+schemaList[j]%>"><%=tableList.get(i+2) %></label></td>
	    			<td></td><td></td>
	    			<% }%>
	    			<% if (i+3<tableList.size() ) { %>
                    <td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+3 +"_"+schemaList[j]%>" value="<%=tableList.get(i+3)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+tableList.get(i+3)) || dynamicTableList.size()==0 ? "checked='checked'" : ""  %> onchange="addTableToList(this,'opttables')"/></td>
	    			<td><label for="tablesOdata<%=i+3 +"_"+schemaList[j]%>"><%=tableList.get(i+3) %></label></td>
	    			<td></td><td></td>
                    <%} if (i+4<tableList.size() ) {%>
                    <td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+4 +"_"+schemaList[j]%>" value="<%=tableList.get(i+4)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+tableList.get(i+4)) || dynamicTableList.size()==0 ? "checked='checked'" : ""  %> onchange="addTableToList(this,'opttables')"/></td>
	    			<td><label for="tablesOdata<%=i+4 +"_"+schemaList[j]%>"><%=tableList.get(i+4) %></label></td>
	    			<td></td><td></td>
	    			<% }%>
		    	</tr>
		    	<% }
		    	%>
	    	</table>
	    	<% if(tableList.size() == 0){%>
	    			<fmt:message key="odata.empty.tables"/>
    		<%
	    		}
	    	%>
	    	</p>
			</div>
			
			<div id="Views<%= j %>" class="tabcontent">
			  <h3>List of Views</h3>
			  <p>
			  <table>
	    		<% for(int i=0;i<viewsList.size();i=i+5){%>
		    	<tr>
	    			<td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+"View_"+schemaList[j] %>" value="<%=viewsList.get(i)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+viewsList.get(i)) || dynamicTableList.size()==0  ? "checked='checked'" : ""  %> onchange="addTableToList(this,'optviews')"/></td>
	    			<td><label for="tablesOdata<%=i +"View_"+schemaList[j]%>" ><%=viewsList.get(i) %></label></td>
	    			<td></td><td></td>
                    <% if (i+1<viewsList.size() ) { %>
                    <td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+1+"View_"+schemaList[j] %>" value="<%=viewsList.get(i+1)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+viewsList.get(i+1)) || dynamicTableList.size()==0 ? "checked='checked'" : ""  %> onchange="addTableToList(this,'optviews')"/></td>
	    			<td><label for="tablesOdata<%=i+1 +"View_"+schemaList[j]%>"><%=viewsList.get(i+1) %></label></td>
	    			<td></td><td></td>
                    <%} if (i+2<viewsList.size() ) {%>
                    <td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+2+"View_"+schemaList[j] %>" value="<%=viewsList.get(i+2)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+viewsList.get(i+2)) || dynamicTableList.size()==0 ? "checked='checked'" : ""  %> onchange="addTableToList(this,'optviews')"/></td>
	    			<td><label for="tablesOdata<%=i+2 +"View_"+schemaList[j] %>"><%=viewsList.get(i+2) %></label></td>
	    			<td></td><td></td>
	    			<% }%>
	    			<% if (i+3<viewsList.size() ) { %>
                    <td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+3+"View_"+schemaList[j] %>" value="<%=viewsList.get(i+3)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+viewsList.get(i+3)) || dynamicTableList.size()==0 ? "checked='checked'" : ""  %> onchange="addTableToList(this,'optviews')"/></td>
	    			<td><label for="tablesOdata<%=i+3 +"View_"+schemaList[j]%>"><%=viewsList.get(i+3) %></label></td>
	    			<td></td><td></td>
                    <%} if (i+4<viewsList.size() ) {%>
                    <td><input type="checkbox" name="tablesOdata" id="tablesOdata<%=i+4+"View_"+schemaList[j] %>" value="<%=viewsList.get(i+4)%>::<%=schemaList[j]%>" <%= dynamicTableList.contains(schemaList[j]+"."+viewsList.get(i+4))  ? "checked='checked'" : ""  %> onchange="addTableToList(this,'optviews')"/></td>
	    			<td><label for="tablesOdata<%=i+4 +"View_"+schemaList[j] %>"><%=viewsList.get(i+4) %></label></td>
	    			<td></td><td></td>
	    			<% }%>
		    	</tr>
		    	<% }
		    	%>
	    	</table>
	    	<% if(viewsList.size() == 0){%>
	    			<fmt:message key="odata.empty.views"/>
    		<%
	    		}
	    	%>
	    	</p> 
			</div>
	    </div>
	    <%} %>
	    <script type="text/javascript">
		    jQuery("#optviews option").remove();
			jQuery("#opttables option").remove();
		    if(<%=createView%>){
		  	  select_unselect("tablesOdata",true); // force selection of tables and the population of tables list inside columns's block on first time enabling OData (by default select all tbls)
		  	} else{
		  	  populateTablesInCombo();
		  	}
		    openTabContent(null, "Tables0");
   	    </script>
</fmt:bundle>
    