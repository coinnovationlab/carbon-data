package co.innovation.lab.dss.rest.api.resources;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.wso2.carbon.dataservices.ui.beans.Data;
import org.wso2.carbon.service.mgt.ServiceMetaData;
import org.wso2.carbon.service.mgt.ServiceMetaDataWrapper;

import co.innovation.lab.dss.rest.api.Utils;
import co.innovation.lab.dss.rest.api.beans.DataService;

import org.wso2.carbon.dataservices.core.admin.rest.DataServiceManager;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/")
@Api(value = "/", description = "REST API for manipulating dataservices")
@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
@Produces(MediaType.APPLICATION_JSON)
public class DataServicesResource extends AbstractResource {
	
    //private static final Log log = LogFactory.getLog(DataServicesResource.class);
	@Context
    private HttpServletRequest httpServletRequest;
    @Context
    private HttpServletResponse httpServletResponse;
    
    DataServiceManager dataServiceManager;

    public DataServicesResource() throws Exception {
        dataServiceManager = Utils.getDataServiceManager();       
    }
    
    @POST
    @Path("/{tenantDomain}/saveDataService")
    @ApiOperation(value = "Save and Update dataservice providing new parameters",
    	httpMethod = "POST")
	@ApiResponses(value = {
	    @ApiResponse(code = 200, message = "Method call success"),
	    @ApiResponse(code = 500, message = "Error while saving dataservice ")
	})    
    public Response saveDataService(@ApiParam(value = "The tenant containing the dataservice", required = true)@PathParam("tenantDomain") String tenantDomain,
    		@ApiParam(value = "Service Parameters", required = true) DataService data) {
        Data dataService = data.getData();
        Data toReturn = dataService;
        try {
        	toReturn = Utils.elaborateSaveDS(dataService);  
        	dataServiceManager.saveDataService(dataService.getName(), "", toReturn.buildXML().toString(),tenantDomain);
        } catch (Exception e) {
        	System.out.println(e.getMessage());
            return handleResponse(ResponseStatus.FAILED, "Error while saving dataservice ");
        } 
        return Response.ok().build();
    }
    
    @GET
    @Path("/{tenantDomain}/listDataService")
    @ApiOperation(value = "Paginate through the dataservices of specific tenant, 15 services per page",
	httpMethod = "GET")
	@ApiResponses(value = {
	    @ApiResponse(code = 200, message = "Method call success"),
	    @ApiResponse(code = 500, message = "Error while listing dataservices")
	})
    public Response listDataService(@ApiParam(value = "The tenant containing the dataservices", required = true)@PathParam("tenantDomain") String tenantDomain, 
    		@ApiParam(value = "The term to search", required = true)@QueryParam("search") String search,
    		@ApiParam(value = "The total number of pages: starting from 0", required = true)	@QueryParam("page") int page) {
    	
    	ServiceMetaData[] services = null;
    	int pages = 0;
    	try {
    		ServiceMetaDataWrapper wrapper = dataServiceManager.listDataServices("ALL", search, page, tenantDomain);
        	services = wrapper.getServices();
        	pages = wrapper.getNumberOfPages();
        } catch (Exception e) {
        	System.out.println(e.getMessage());
            return handleResponse(ResponseStatus.FAILED, "Error while listing dataservices ");
        } 
        return handleResponseObject(ResponseStatus.LISTSERVICES, "OK", pages, services);
    }
    
	@GET
    @Path("/{tenantDomain}/getDataService")
	@ApiOperation(value = "Get the parameters of the specific dataservice",
		httpMethod = "GET")
	@ApiResponses(value = {
	    @ApiResponse(code = 200, message = "Method call success"),
	    @ApiResponse(code = 500, message = "Error while getting dataservice information")
	})
    public Response getDataService(@ApiParam(value = "The tenant containing the dataservice", required = true)@PathParam("tenantDomain") String tenantDomain, 
    		@ApiParam(value = "The service name", required = true)@QueryParam("serviceid") String serviceid) {
    	
    	String content;
    	Data data = null;
    	try {
    		content = dataServiceManager.getDataService(serviceid, tenantDomain);
    		data = Utils.elaborateGetDS(content);    		   
        } catch (Exception e) {
        	System.out.println(e.getMessage());
            return handleResponse(ResponseStatus.FAILED, "Error while getting dataservice information ");
        } 
        return handleResponseData(ResponseStatus.LISTSERVICES, content, data);
    }
    
    @DELETE
    @Path("/{tenantDomain}/dataService/{servicename:.*}")
    @ApiOperation(value = "Delete specific dataservice",
		httpMethod = "DELETE")
	@ApiResponses(value = {
	    @ApiResponse(code = 200, message = "Method call success"),
	    @ApiResponse(code = 500, message = "Error while deleting dataservices")
	})
    public Response deleteDataService(@ApiParam(value = "The tenant containing the dataservice", required = true)@PathParam("tenantDomain") String tenantDomain, 
    		@ApiParam(value = "The service name", required = true)@PathParam("servicename") String servicename) {
    	try {
    		dataServiceManager.deleteDataService(servicename, tenantDomain);
        } catch (Exception e) {
        	System.out.println(e.getMessage());
            return handleResponse(ResponseStatus.FAILED, "Error while deleting dataservices ");
        } 
        return Response.ok().build();
    }
}
