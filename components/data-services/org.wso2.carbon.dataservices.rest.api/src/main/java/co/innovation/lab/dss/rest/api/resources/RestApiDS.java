/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.innovation.lab.dss.rest.api.resources;

import org.apache.commons.httpclient.HttpStatus;
import org.wso2.carbon.dataservices.core.admin.rest.DataServiceManager;
import org.wso2.carbon.dataservices.ui.beans.Data;
import org.wso2.carbon.service.mgt.ServiceMetaData;
import org.wso2.carbon.service.mgt.ServiceMetaDataWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.innovation.lab.dss.rest.api.Utils;
import co.innovation.lab.dss.rest.api.beans.DataService;
import co.innovation.lab.dss.rest.api.resources.AbstractResource.ResponseStatus;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.SwaggerDefinition;
import io.swagger.annotations.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;

@SwaggerDefinition(
        info = @Info(
                description = "REST API for CRUD dataservices",
                version = "1.0.0",
                title = "REST API for manipulating dataservices",
                termsOfService = "http://swagger.io/terms/"
        ),
        consumes = {"application/json"},
        produces = {"application/json"},
        schemes = {SwaggerDefinition.Scheme.HTTP, SwaggerDefinition.Scheme.HTTPS}
)
@Api(value = "/rest/", description = "REST API for CRUD dataservices")
public class RestApiDS extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private DataServiceManager dataServiceManager = Utils.getDataServiceManager(); 
	private Pattern regExPatternDataService = Pattern.compile("/([a-zA-Z.]*)/getDataService");
	private Pattern regExPatternDataServiceList = Pattern.compile("/([a-zA-Z.]*)/listDataService");
	private Pattern regExPatternDataServiceDelete = Pattern.compile("/([a-zA-Z.]*)/dataService/(.*)");
	private Pattern regExPatternDataServiceSave = Pattern.compile("/([a-zA-Z.]*)/saveDataService");
    private int responseStatus;
    private String tenantDomain;
    
    
    @ApiOperation(value = "Paginate through the dataservices of specific tenant, 15 services per page",
	httpMethod = "GET")
    @Path("/{tenantDomain}/listDataService")
		@ApiImplicitParams({
		@ApiImplicitParam(name = "bearerToken", value = "The authorization bearer token", required = true, paramType = "header"),
		@ApiImplicitParam(name = "apikey", value = "The authorization apikey: required if there is no header token provided", required = false, paramType = "query"),
		@ApiImplicitParam(name = "tenantDomain", value = "The tenant containing the dataservice", required = true, paramType = "path"),
		@ApiImplicitParam(name = "search", value = "The term to search", required = false, paramType = "query"),
		@ApiImplicitParam(name = "page", value = "The total number of pages: starting from 0", required = false, paramType = "query"),
		})
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String pathInfo = request.getPathInfo();
		int page = 0;
		Matcher matcherDataService		= regExPatternDataService.matcher(pathInfo);
		Matcher matcherDataServiceList	= regExPatternDataServiceList.matcher(pathInfo);
		if (matcherDataService.find()) {
			tenantDomain = matcherDataService.group(1);
			String serviceId = request.getParameter("serviceid");
			getDataService(response, serviceId);
	    } else if(matcherDataServiceList.find()){
	    	if(request.getParameter("page") != null) {
	    		page = Integer.parseInt(request.getParameter("page"));
	    	}
			String search = request.getParameter("search");
	    	tenantDomain = matcherDataServiceList.group(1);
	    	listDataService(response,search,page);
	    } else {
	    	response.setStatus(HttpStatus.SC_BAD_REQUEST);
	    	return;
	    }
	}
	
	@ApiOperation(value = "Save and Update dataservice providing new parameters",
	    	httpMethod = "POST")
		@ApiResponses(value = {
		    @ApiResponse(code = 200, message = "Method call success"),
		    @ApiResponse(code = 500, message = "Error while saving dataservice ")
		})  
	@Path("/{tenantDomain}/saveDataService")
	@ApiImplicitParams({
        @ApiImplicitParam(name = "bearerToken", value = "The authorization bearer token", required = true, paramType = "header"),
        @ApiImplicitParam(name = "apikey", value = "The authorization apikey: required if there is no header token provided", required = false, paramType = "query"),
        @ApiImplicitParam(name = "tenantDomain", value = "The tenant containing the dataservice", required = true, paramType = "path"),
        @ApiImplicitParam(name = "data", value = "Service Parameters", required = true, paramType = "body"),
        })
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		String pathInfo = request.getPathInfo();
		String line = null;
		try {
			Matcher matcherDataServiceSave	= regExPatternDataServiceSave.matcher(pathInfo);
			if (matcherDataServiceSave.find()) {
				tenantDomain = matcherDataServiceSave.group(1);
				StringBuffer buffer = new StringBuffer();
				BufferedReader reader = request.getReader();
                while ((line = reader.readLine()) != null) {
                    buffer.append(line);
                }
                ObjectMapper mapper = new ObjectMapper();
                DataService data = mapper.readValue(buffer.toString(), DataService.class);

				Data dataService = data.getData();
		        Data toReturn = dataService;
		        try {
		        	toReturn = Utils.elaborateSaveDS(dataService);  
		        	dataServiceManager.saveDataService(dataService.getName(), "", toReturn.buildXML().toString(),tenantDomain);
		        } catch (Exception e) {
		        	System.out.println(e.getMessage());
		            handleResponse("Error while saving dataservice ", response);
		        } 
		    } else {
		    	response.setStatus(HttpStatus.SC_BAD_REQUEST);
		    	return;
		    }
        } catch (Exception e) {
        	handleError("Error while saving dataservice ", response);
        } 
	}
	
	@ApiOperation(value = "Delete specific dataservice",
			httpMethod = "DELETE")
	@Path("/{tenantDomain}/dataService/{servicename:.*}")
	@ApiImplicitParams({
        @ApiImplicitParam(name = "bearerToken", value = "The authorization bearer token", required = true, paramType = "header"),
        @ApiImplicitParam(name = "apikey", value = "The authorization apikey: required if there is no header token provided", required = false, paramType = "query"),
        @ApiImplicitParam(name = "tenantDomain", value = "The tenant containing the dataservice", required = true, paramType = "path"),
        @ApiImplicitParam(name = "servicename", value = "The service name", required = true, paramType = "path"),
        })
	@Override
	public void doDelete(HttpServletRequest request, HttpServletResponse response) {
		String pathInfo = request.getPathInfo();
		try {
			Matcher matcherDataServiceDelete	= regExPatternDataServiceDelete.matcher(pathInfo);
			if (matcherDataServiceDelete.find()) {
				tenantDomain = matcherDataServiceDelete.group(1);
				String servicename = matcherDataServiceDelete.group(2);
				dataServiceManager.deleteDataService(servicename, tenantDomain);
				handleResponse("OK",response);
		    } else {
		    	response.setStatus(HttpStatus.SC_BAD_REQUEST);
		    	return;
		    }
        } catch (Exception e) {
        	handleError("Error while deleting dataservices ", response);
        } 
	}
	
	private void listDataService(HttpServletResponse response, String search, int page) throws IOException {
		int pages = 0;
		try {
			ServiceMetaDataWrapper wrapper = dataServiceManager.listDataServices("ALL", search, page, tenantDomain);
			ServiceMetaData[] services = wrapper.getServices();
	    	pages = wrapper.getNumberOfPages();
	    	String respResult = AbstractResource.handleResponseObject(ResponseStatus.LISTSERVICES, "OK", pages, services);
	    	responseStatus = HttpStatus.SC_OK;
	    	response.setContentType("text/json;charset=UTF-8");
	    	PrintWriter out = response.getWriter();
	        out.println(respResult);
	        response.setStatus(responseStatus);
	    } catch (Exception e) {
	    	handleError("Error while listing dataservices ", response);
	    } 
	}
	
	@ApiOperation(value = "Get the parameters of the specific dataservice",
    		httpMethod = "GET")
	@Path("/{tenantDomain}/getDataService")
	@ApiImplicitParams({
        @ApiImplicitParam(name = "bearerToken", value = "The authorization bearer token", required = true, paramType = "header"),
        @ApiImplicitParam(name = "apikey", value = "The authorization apikey: required if there is no header token provided", required = false, paramType = "query"),
        @ApiImplicitParam(name = "tenantDomain", value = "The tenant containing the dataservice", required = true, paramType = "path"),
        @ApiImplicitParam(name = "serviceid", value = "The service name", required = true, paramType = "query"),
        })
	public void getDataService(HttpServletResponse response, String serviceId) throws IOException {
		String content;
    	Data data = null;
		try {
			content = dataServiceManager.getDataService(serviceId, tenantDomain);
    		data = Utils.elaborateGetDS(content); 
	    	String respResult = AbstractResource.handleResponseData(ResponseStatus.SUCCESS, "OK", data);
	    	responseStatus = HttpStatus.SC_OK;
	    	response.setContentType("text/json;charset=UTF-8");
	    	PrintWriter out = response.getWriter();
	        out.println(respResult);
	        response.setStatus(responseStatus);
	    } catch (Exception e) {
	    	handleError("Error while getting dataservice information ", response);
	    } 
	}
	
	private void handleError(String message, HttpServletResponse response) {
		String respResult = AbstractResource.handleResponse(ResponseStatus.FAILED, message);
    	PrintWriter out;
		try {
			out = response.getWriter();
	        out.println(respResult);
	    	response.setStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR);
	    	response.setContentType("text/json;charset=UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void handleResponse(String message, HttpServletResponse response) {
		String respResult = AbstractResource.handleResponse(ResponseStatus.SUCCESS, message);
    	PrintWriter out;
		try {
			out = response.getWriter();
	        out.println(respResult);
	    	response.setStatus(HttpStatus.SC_OK);
	    	response.setContentType("text/json;charset=UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
