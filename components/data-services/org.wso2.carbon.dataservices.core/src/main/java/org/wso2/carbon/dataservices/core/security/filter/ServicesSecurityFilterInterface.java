package org.wso2.carbon.dataservices.core.security.filter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface ServicesSecurityFilterInterface {
	
	/**
     * Security control to filter OData/REST requests according to authorization
     * @param request
     * @param tenantDomain
     * @return
     */
    public boolean securityFilter(HttpServletRequest request, HttpServletResponse resp, String tenantDomain);
    
}
