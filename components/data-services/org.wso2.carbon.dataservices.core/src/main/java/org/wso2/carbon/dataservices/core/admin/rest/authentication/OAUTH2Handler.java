package org.wso2.carbon.dataservices.core.admin.rest.authentication;

import java.util.Hashtable;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.CarbonConstants;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.core.services.authentication.CarbonServerAuthenticator;
import org.wso2.carbon.dataservices.core.admin.rest.Utils;
import org.wso2.carbon.dataservices.core.security.filter.ServicesSecurityFilter;
import org.wso2.carbon.identity.authenticator.oauth2.sso.OAUTH2SSOAuthenticator;
import org.wso2.carbon.identity.authenticator.oauth2.sso.common.OAUTH2SSOAuthenticatorConstants;
import org.wso2.carbon.identity.authenticator.oauth2.sso.common.Util;
import org.wso2.carbon.user.api.UserStoreException;
import org.wso2.carbon.user.core.service.RealmService;

public class OAUTH2Handler implements AuthenticationHandler{
	private static final Log log = LogFactory.getLog(OAUTH2Handler.class);
    public static final String AUTHORIZATION_HEADER = "Authorization";
    ServicesSecurityFilter secFilter = new ServicesSecurityFilter();

	public boolean canHandle(Map httpHeaders, HttpServletRequest request, HttpServletResponse response) {
		boolean isAuthEnabled = Util.isAuthenticatorEnabled();
        if(!isAuthEnabled) {
        	return true;
        }
		String authToken = getAuthHeaderToken(request);
    	String apiKey = getApiKey(request);
    	boolean containsAuthToken = authToken != null && !authToken.equals("");
    	boolean containsApiKey = apiKey != null && !apiKey.equals("");  
        if(containsAuthToken || containsApiKey){
            return true;
        }
        return false;
	}

	public boolean isAuthenticated(Map httpHeaders, HttpServletRequest request, HttpServletResponse response) throws Exception {
		String tenantDomain = getTenantDomain(request.getRequestURI());	
		boolean isAuthEnabled = Util.isAuthenticatorEnabled();
        if(!isAuthEnabled) {
        	return true;
        }
        if (tenantDomain != null ) {
            try {
                //get super tenant context and get realm service which is an osgi service
                RealmService realmService = (RealmService)PrivilegedCarbonContext.getThreadLocalCarbonContext().
                                getOSGiService(RealmService.class);
                Hashtable<String,String> param = new Hashtable<String,String>();
                param.put(CarbonConstants.AUTHENTICATOR_TYPE, OAUTH2SSOAuthenticatorConstants.AUTHENTICATOR_NAME);
                OAUTH2SSOAuthenticator oauth2Auth = (OAUTH2SSOAuthenticator)PrivilegedCarbonContext.getThreadLocalCarbonContext().
                        getOSGiService(CarbonServerAuthenticator.class, param);
                if (oauth2Auth != null) {
                    int tenantId = realmService.getTenantManager().getTenantId(tenantDomain);
                    if (tenantId == -1) {
                        log.error("Invalid tenant domain " + tenantDomain);
                        return false;
                    }
                    boolean authorized = secFilter.securityFilter(request, response, tenantDomain);
                    if (authorized) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } catch (UserStoreException e) {
                log.debug("UserStoreException. Return Unauthorized.", e);
                return false;
            }
        } else {
            return false;
        }
	}
	/**
     * 
     * Retrieve the apikey parameter 
     * @param request
     * @return
     */
    private String getApiKey(HttpServletRequest request) {
        String apikey = request.getParameter("apikey");
        return apikey;
    }
    /**
     * 
     * Retrieve the apikey parameter 
     * @param uri
     * @param tenantDomain
     * @return
     */
    private static String getAuthHeaderToken(HttpServletRequest request) {
    	String authToken = "";
        String authorization = request.getHeader("Authorization");
        if(authorization != null) {
        	assert authorization.substring(0, 7).equals("Bearer ");
            authToken = authorization.substring(7);
        }
        return authToken;
    }
    /**
     * Calculate the tenant domain from the complete URL
     *
     * @param url - incoming URL
     * @return - Tenant domain
     */
    public String getTenantDomain(String url) {
        String[] strings = url.split("/");
        boolean foundTenantDelimiter = false;
        String tenant = null;
        for (String str : strings) {
            if (!foundTenantDelimiter && str.equals("rest_services")) {
                foundTenantDelimiter = true;
                continue;
            }
            if (foundTenantDelimiter) {
                tenant = str;
                break;
            }
        }
        return tenant;
    }
}
