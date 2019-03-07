package co.innovation.lab.dss.rest.api.resources;

import co.innovation.lab.dss.rest.api.beans.*;
import org.wso2.carbon.dataservices.ui.beans.Data;
import org.wso2.carbon.service.mgt.ServiceMetaData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AbstractResource {


    public static String handleResponse(ResponseStatus responseStatus, String services) {
        String  response = services;
        ObjectMapper mapper = new ObjectMapper();
        StandardResponse standardResponse = getResponseMessage(responseStatus, services);
        try {
        	response = mapper.writeValueAsString(standardResponse);
        } catch (JsonProcessingException e) {
			e.printStackTrace();
		}
        return response;
    }
    
    public static String handleResponseData(ResponseStatus responseStatus, String data, Data d) {
        String response = data;
        ObjectMapper mapper = new ObjectMapper();
        StandardResponse standardResponse = getResponseMessageData(responseStatus, data, d);
        try {
			response = mapper.writeValueAsString(standardResponse);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
        return response;
    }
    
    public static String handleResponseObject(ResponseStatus responseStatus, String message, int pageNr, ServiceMetaData[] metadata) {
        String response = message;
        ServiceMetaDataResponse standardResponse = getResponseMetaData(responseStatus, message, pageNr, metadata);
        ObjectMapper mapper = new ObjectMapper();
        try {
        	response = mapper.writeValueAsString(standardResponse);
        } catch (JsonProcessingException e) {
			e.printStackTrace();
		}
        return response;
    }


    private static StandardResponse getResponseMessage(ResponseStatus status, String message) {
        StandardResponse standardResponse = new StandardResponse(status.toString());
        if (message != null) {
            standardResponse.setMessage(message);
        }
        return standardResponse;
    }
    
    private static StandardResponse getResponseMessageData(ResponseStatus status, String message, Data d) {
        StandardResponse standardResponse = new StandardResponse(status.toString());
        standardResponse.setData(d);
        if (message != null) {
            standardResponse.setMessage(message);
        }
        return standardResponse;
    }
    
    private static ServiceMetaDataResponse getResponseMetaData(ResponseStatus status, String message, int pageNr, ServiceMetaData[] metadata) {
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