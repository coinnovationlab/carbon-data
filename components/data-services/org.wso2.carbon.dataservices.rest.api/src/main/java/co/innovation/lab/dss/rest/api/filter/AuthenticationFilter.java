package co.innovation.lab.dss.rest.api.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.wso2.carbon.dataservices.core.admin.rest.authentication.AuthenticationHandler;
import co.innovation.lab.dss.rest.api.Configuration;

public class AuthenticationFilter implements Filter {

	List<AuthenticationHandler> authenticationHandlers;
	
	@Override
	public void destroy() {
		
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
		boolean authenticated = false;
		HttpServletRequest req = (HttpServletRequest) request;
    	HttpServletResponse resp = (HttpServletResponse) response;
        try {
            for (AuthenticationHandler handler : authenticationHandlers) {
            	Map<String, Object> protocolHeaders=new HashMap<String, Object>();
            	Enumeration headerNames = req.getHeaderNames();
	        	while(headerNames.hasMoreElements()) {
	        		String headerName = (String)headerNames.nextElement();
	        		protocolHeaders.put(headerName, req.getHeader(headerName));
	        	}
                if (handler.canHandle(protocolHeaders,req,resp)) {
                    authenticated = handler.isAuthenticated(protocolHeaders,req,resp);
                }
            }
            if (authenticated) {
            	filterChain.doFilter(request, response);
            } else {
                handleError(resp);
            }
        } catch(Exception e){
        	e.printStackTrace();
        	handleError(resp);
        }
	}

	@Override
	public void init(FilterConfig arg0) throws ServletException {
		authenticationHandlers = new ArrayList<AuthenticationHandler>();
        try {
            this.loadAuthenticationHandlers(authenticationHandlers);
        } catch (Exception e) {
        	System.out.println("Error occurred while initializing AuthenticationFilter.");
        }	
	}
    
    private void handleError(HttpServletResponse response) throws IOException {
    	response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
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
