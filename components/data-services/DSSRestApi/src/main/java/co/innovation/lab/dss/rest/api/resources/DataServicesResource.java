package co.innovation.lab.dss.rest.api.resources;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.wso2.carbon.dataservices.ui.beans.Config;
import org.wso2.carbon.dataservices.ui.beans.Data;
import org.wso2.carbon.dataservices.ui.beans.Property;
import org.wso2.carbon.service.mgt.ServiceMetaData;
import org.wso2.carbon.service.mgt.ServiceMetaDataWrapper;

import co.innovation.lab.dss.rest.api.Utils;
import co.innovation.lab.dss.rest.api.beans.DataService;

import org.wso2.carbon.dataservices.common.conf.DynamicAuthConfiguration;
import org.wso2.carbon.dataservices.common.conf.DynamicODataConfig;
import org.wso2.carbon.dataservices.common.conf.ODataColumnsConfig;
import org.wso2.carbon.dataservices.common.conf.ODataTableSchemaConfig;
import org.wso2.carbon.dataservices.core.admin.rest.DataServiceManager;

import javax.xml.bind.*;
import javax.xml.transform.stream.StreamSource;
import javax.xml.namespace.QName;

import com.fasterxml.jackson.databind.ObjectMapper;

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
    public Response saveDataService(@PathParam("tenantDomain") String tenantDomain,
                                   DataService data) {
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
    public Response listDataService(@PathParam("tenantDomain") String tenantDomain, 
	    		@QueryParam("search") String search,
	    		@QueryParam("page") int page) {
    	
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
    public Response getDataService(@PathParam("tenantDomain") String tenantDomain, 
	    		@QueryParam("serviceid") String serviceid) {
    	
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
    @Path("/{tenantDomain}/dataService/{servicename}")
    public Response deleteDataService(@PathParam("tenantDomain") String tenantDomain, @PathParam("servicename") String servicename) {
    	try {
    		dataServiceManager.deleteDataService(servicename, tenantDomain);
        } catch (Exception e) {
        	System.out.println(e.getMessage());
            return handleResponse(ResponseStatus.FAILED, "Error while deleting dataservices ");
        } 
        return Response.ok().build();
    }
}
