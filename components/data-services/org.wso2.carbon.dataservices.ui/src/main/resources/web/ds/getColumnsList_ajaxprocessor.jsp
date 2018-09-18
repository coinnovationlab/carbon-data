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

<%@ page import="org.wso2.carbon.ui.CarbonUIUtil" %>
<%@ page import="org.wso2.carbon.CarbonConstants" %>
<%@ page import="org.apache.axis2.context.ConfigurationContext" %>
<%@ page import="org.wso2.carbon.dataservices.ui.DataServiceAdminClient"%>
<%@ page import="org.wso2.carbon.dataservices.ui.stub.admin.core.xsd.ColumnsList"%>
<%@ page import="org.wso2.carbon.utils.ServerConstants" %>
<%@ page import="org.apache.axis2.AxisFault"%>
<%@ page import="org.wso2.carbon.CarbonError" %>
<%@ page import="java.io.PrintWriter" %>
<%@ page import="org.owasp.encoder.Encode" %>
<%@ page import="org.wso2.carbon.dataservices.core.odata.DataColumn" %>

<%
	String driverClass = request.getParameter("driver");
	String jdbcUrl = Encode.forHtmlContent(request.getParameter("jdbcUrl"));
	String userName = request.getParameter("userName");
	String password = request.getParameter("password");
    String passwordAlias = request.getParameter("passwordAlias");
    String choosenSchema = request.getParameter("schema");
    String tablename = request.getParameter("tablename");
     // read post parameters
//      StringWriter writer = new StringWriter();
//      IOUtils.copy(request.getInputStream(), writer);
//      System.out.println(" post parameters "+writer.toString());
//      Gson gson = new Gson();
//      List<ODataColumnsConfig> dynamicColumnsList = gson.fromJson(writer.toString());
   
	String backendServerURL = CarbonUIUtil
			.getServerURL(config.getServletContext(), session);
	ConfigurationContext configContext = (ConfigurationContext) config.getServletContext()
			.getAttribute(CarbonConstants.CONFIGURATION_CONTEXT);
	String cookie = (String) session.getAttribute(ServerConstants.ADMIN_SERVICE_COOKIE);
	DataServiceAdminClient client = new DataServiceAdminClient(cookie, backendServerURL,configContext);
	ColumnsList [] columnsList2;
	String tableContent = "";
	String key = choosenSchema+"."+tablename;
	key = key.trim();
	try {
		columnsList2 = client.generateTableColumnsList(driverClass, jdbcUrl, userName, password, passwordAlias, choosenSchema, tablename);
   		if(columnsList2 != null){
   			List<ColumnsList> columnsList = new ArrayList<ColumnsList>(Arrays.asList(columnsList2));
   			String options = "";
   			for (DataColumn.ODataDataType type : DataColumn.ODataDataType.values()) { 
   			 	options += "<option value='"+type+"'>"+type+" </option>";
   			}
		
			tableContent +="<table>" ;
			String checked="";
			for(int i=0;i<columnsList.size();i=i+3){
				tableContent += " ";
				if(i==0) {
					tableContent += "<tr class='headerCols'><td><b>Column Name</b></td><td><b>Type</b></td><td></td><td></td>";
					if(i+1<columnsList.size() )
					tableContent += "	<td><b>Column Name</b></td><td><b>Type</b></td><td></td><td></td>";
					if(i+2<columnsList.size() )
					tableContent += "	<td><b>Column Name</b></td><td><b>Type</b></td><td></td><td></td>	</tr>";
				}
				tableContent += "<tr>"+
			    			"<td><input type='checkbox' name='columnsList' id='columnsList_"+columnsList.get(i).getName() +"' value='"+columnsList.get(i).getName() +"::"+tablename+"::"+choosenSchema+"' onchange='saveColState(\""+key+"\")'" +"/>"+
			    			"<label for='columnsList_"+columnsList.get(i).getName() +"' > "+columnsList.get(i).getName()+"</label></td>"+
		   					"<td><select id='typesList_"+columnsList.get(i).getName() +"'  onchange='saveColState(\""+key+"\")'> "
				   					+"<option value='"+columnsList.get(i).getType()+"' selected>"
					    			+columnsList.get(i).getType()
					    			+"</option>"+options
			    			+"</select></td>"+
		   					"<td></td><td></td>";
				
				if(i+1<columnsList.size() ){
		    		tableContent += "";
    						tableContent += ""+
			    			"<td><input type='checkbox' name='columnsList' id='columnsList_"+columnsList.get(i+1).getName() +"' value='"+columnsList.get(i+1).getName() +"::"+tablename+"::"+choosenSchema+"' onchange='saveColState(\""+key+"\")'"  +"/>"+
	    					"<label for='columnsList_"+columnsList.get(i+1).getName() +"' >"+columnsList.get(i+1).getName()+"</label></td>"+
			    			"<td><select id='typesList_"+columnsList.get(i+1).getName() +"'  onchange='saveColState(\""+key+"\")'> "
			    					+"<option value='"+columnsList.get(i+1).getType()+"' selected>"
					    			+columnsList.get(i+1).getType()
					    			+"</option>"+options
	    					+"</select></td>"+
			    			"<td></td><td></td>";
		    	}
		    	if(i+2<columnsList.size() ){
		    		tableContent += "";
    						tableContent += ""+
			    			"<td><input type='checkbox' name='columnsList' id='columnsList_"+columnsList.get(i+2).getName() +"' value='"+columnsList.get(i+2).getName() +"::"+tablename+"::"+choosenSchema+"' onchange='saveColState(\""+key+"\")'"  +"/>"+
	    					"<label for='columnsList_"+columnsList.get(i+2).getName() +"' >"+columnsList.get(i+2).getName()+"</label></td>"+
			    			"<td><select id='typesList_"+columnsList.get(i+2).getName() +"'  onchange='saveColState(\""+key+"\")'> "
			    					+"<option value='"+columnsList.get(i+2).getType()+"' selected>"
					    			+columnsList.get(i+2).getType()
					    			+"</option>"+options
	    					+"</select></td>"+
			    			"<td></td><td></td>";
		    	}
		    	tableContent += "</tr>";
			}
			tableContent += "</table>";
		}
		response.setContentType("text/xml; charset=UTF-8");
		// Set standard HTTP/1.1 no-cache headers.
		response.setHeader("Cache-Control",
		"no-store, max-age=0, no-cache, must-revalidate");
		// Set IE extended HTTP/1.1 no-cache headers.
		response.addHeader("Cache-Control", "post-check=0, pre-check=0");
		// Set standard HTTP/1.0 no-cache header.
		response.setHeader("Pragma", "no-cache");
		PrintWriter pw = response.getWriter();
		pw.write(tableContent);
		pw.flush();
	} catch (AxisFault e) {
		CarbonError carbonError = new CarbonError();
		carbonError.addError("Error occurred while retrieving tables list.");
		request.setAttribute(CarbonError.ID, carbonError);
	    String errorMsg = e.getLocalizedMessage();
%>
<script type="text/javascript">
	location.href = "dsErrorPage.jsp?errorMsg=<%=errorMsg%>";
</script>
<%
    	        	}
        	        	%>