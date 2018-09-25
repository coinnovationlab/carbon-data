package org.wso2.carbon.dataservices.core.admin.rest.authentication;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface AuthenticationHandler {

	/**
     * Returns whether the authenticator can handle the request
     *
     * @param httpHeaders
     * @return Return true if the request can be handled, false otherwise
     */
	public boolean canHandle(Map httpHeaders, HttpServletRequest request, HttpServletResponse response);
	
	/**
     * Process the request and  return the result
     *
     * @param httpHeaders
     * @return true if authentication successful, false otherwise
	 * @throws Exception 
     */
	public boolean isAuthenticated(Map httpHeaders, HttpServletRequest request, HttpServletResponse response) throws Exception;
}
