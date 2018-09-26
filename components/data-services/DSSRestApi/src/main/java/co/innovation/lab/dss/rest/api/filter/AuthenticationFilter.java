package co.innovation.lab.dss.rest.api.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.ext.RequestHandler;
import org.apache.cxf.jaxrs.model.ClassResourceInfo;
import org.apache.cxf.message.Message;
import org.apache.cxf.transport.http.AbstractHTTPDestination;
import org.wso2.carbon.dataservices.core.admin.rest.authentication.AuthenticationHandler;

import co.innovation.lab.dss.rest.api.Configuration;
import co.innovation.lab.dss.rest.api.Constants;
import co.innovation.lab.dss.rest.api.beans.StandardResponse;

public class AuthenticationFilter implements RequestHandler {

	//private static Log log = LogFactory.getLog(AuthenticationFilter.class);
    List<AuthenticationHandler> authenticationHandlers;
    public AuthenticationFilter() {
        authenticationHandlers = new ArrayList<AuthenticationHandler>();
        try {
            this.loadAuthenticationHandlers(authenticationHandlers);
        } catch (Exception e) {
        	System.out.println("Error occurred while initializing AuthenticationFilter.");
        }
    }

    public Response handleRequest(Message message, ClassResourceInfo classResourceInfo) {
        boolean authenticated = false;

        try {
        	HttpServletRequest request = (HttpServletRequest) message.get(AbstractHTTPDestination.HTTP_REQUEST);
        	HttpServletResponse response = (HttpServletResponse) message.get(AbstractHTTPDestination.HTTP_RESPONSE);
            for (AuthenticationHandler handler : authenticationHandlers) {
                TreeMap protocolHeaders = (TreeMap) message.get(Message.PROTOCOL_HEADERS);
                if (handler.canHandle(protocolHeaders,request,response)) {
                    authenticated = handler.isAuthenticated(protocolHeaders,request,response);
                }
            }
            if (authenticated) {
                return null;
            } else {
                return handleError();
            }
        } catch(Exception e){
        	return handleError();
        } finally {
        }
    }

    private Response handleError() {
    	System.out.println("Failed authentication");
        return Response.status(401).
                entity(new StandardResponse(Constants.Status.FAILED,
                                            "failed authentication")).build();
    }

    private void loadAuthenticationHandlers(List<AuthenticationHandler> authenticationHandlers)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        List<String> authenticationHandlerClassNames = Configuration
                .getAuthenticationHandlers();
        for (String authenticationHandlerClassName : authenticationHandlerClassNames) {
            AuthenticationHandler authenticationHandler = (AuthenticationHandler) Class.forName
                    (authenticationHandlerClassName)
                    .newInstance();
            authenticationHandlers.add(authenticationHandler);
        }
    }
}
