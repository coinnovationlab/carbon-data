package co.innovation.lab.dss.rest.api.resources;

import co.innovation.lab.dss.rest.api.Constants;
import co.innovation.lab.dss.rest.api.beans.*;
import org.wso2.carbon.dataservices.ui.beans.Data;
import javax.ws.rs.core.Response;
import org.wso2.carbon.service.mgt.ServiceMetaData;

public abstract class AbstractResource {

    //private static final Log LOG = LogFactory.getLog(AbstractResource.class);

    public Response handleResponse(ResponseStatus responseStatus, String services) {
        Response response;
        StandardResponse standardResponse = getResponseMessage(responseStatus, services);
        switch (responseStatus) {
            case SUCCESS:
                response = Response.ok().entity(standardResponse).build();
                break;
            case FAILED:
                response = Response.serverError().entity(standardResponse).build();
                break;
            case INVALID:
                response = Response.status(400).entity(standardResponse).build();
                break;
            case FORBIDDEN:
                response = Response.status(403).entity(standardResponse).build();
                break;
            case LISTSERVICES:
            	
                response = Response.ok().entity(standardResponse).build();
                break;
            default:
                response = Response.noContent().build();
                break;
        }
        return response;
    }
    
    public Response handleResponseData(ResponseStatus responseStatus, String data, Data d) {
        Response response;
        StandardResponse standardResponse = getResponseMessageData(responseStatus, data, d);
        switch (responseStatus) {
            case SUCCESS:
                response = Response.ok().entity(standardResponse).build();
                break;
            case FAILED:
                response = Response.serverError().entity(standardResponse).build();
                break;
            case INVALID:
                response = Response.status(400).entity(standardResponse).build();
                break;
            case FORBIDDEN:
                response = Response.status(403).entity(standardResponse).build();
                break;
            case LISTSERVICES:
            	
                response = Response.ok().entity(standardResponse).build();
                break;
            default:
                response = Response.noContent().build();
                break;
        }
        return response;
    }
    
    public Response handleResponseObject(ResponseStatus responseStatus, String message, int pageNr, ServiceMetaData[] metadata) {
        Response response;
        ServiceMetaDataResponse standardResponse = getResponseMetaData(responseStatus, message, pageNr, metadata);
        switch (responseStatus) {
            case SUCCESS:
                response = Response.ok().entity(standardResponse).build();
                break;
            case FAILED:
                response = Response.serverError().entity(standardResponse).build();
                break;
            case INVALID:
                response = Response.status(400).entity(standardResponse).build();
                break;
            case FORBIDDEN:
                response = Response.status(403).entity(standardResponse).build();
                break;
            case LISTSERVICES:
            	
                response = Response.ok().entity(standardResponse).build();
                break;
            default:
                response = Response.noContent().build();
                break;
        }
        return response;
    }

    public Response handleResponse(StandardResponse standardResponse) {
        Response response;
        if (standardResponse.getStatus().equalsIgnoreCase(Constants.SUCCESS)) {
            response = Response.ok().entity(standardResponse).build();

        } else if (standardResponse.getStatus().equalsIgnoreCase(Constants.FAILED)) {
            response = Response.serverError().entity(standardResponse).build();

        } else if (standardResponse.getStatus().equalsIgnoreCase(Constants.INVALID)) {
            response = Response.status(400).entity(standardResponse).build();

        } else if (standardResponse.getStatus().equalsIgnoreCase(Constants.FORBIDDEN)) {
            response = Response.status(403).entity(standardResponse).build();

        } else {
            response = Response.noContent().build();
        }
        return response;
    }

    private StandardResponse getResponseMessage(ResponseStatus status, String message) {
        StandardResponse standardResponse = new StandardResponse(status.toString());
        if (message != null) {
            standardResponse.setMessage(message);
        }
        return standardResponse;
    }
    
    private StandardResponse getResponseMessageData(ResponseStatus status, String message, Data d) {
        StandardResponse standardResponse = new StandardResponse(status.toString());
        standardResponse.setData(d);
        if (message != null) {
            standardResponse.setMessage(message);
        }
        return standardResponse;
    }
    
    private ServiceMetaDataResponse getResponseMetaData(ResponseStatus status, String message, int pageNr, ServiceMetaData[] metadata) {
        ServiceMetaDataResponse standardResponse = new ServiceMetaDataResponse(status.toString());
        standardResponse.setMessage(message);
        if (metadata != null) {
            standardResponse.setMetaData(metadata);
        }
        standardResponse.setPages(pageNr);
        return standardResponse;
    }

    public enum ResponseStatus {
        SUCCESS, FAILED, INVALID, FORBIDDEN, LISTSERVICES
    }

}