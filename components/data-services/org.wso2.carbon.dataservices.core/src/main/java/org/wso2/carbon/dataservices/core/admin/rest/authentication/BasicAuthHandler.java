package org.wso2.carbon.dataservices.core.admin.rest.authentication;

import org.apache.axiom.om.util.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.dataservices.core.security.filter.ServicesSecurityFilter;
import org.wso2.carbon.user.api.UserRealm;
import org.wso2.carbon.user.api.UserStoreException;
import org.wso2.carbon.user.core.service.RealmService;
import org.wso2.carbon.utils.multitenancy.MultitenantUtils;

import java.util.ArrayList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BasicAuthHandler implements AuthenticationHandler {
    private static final Log LOG = LogFactory.getLog(BasicAuthHandler.class);
    public static final String AUTHORIZATION_HEADER = "Authorization";
    public ServicesSecurityFilter secFilter = new ServicesSecurityFilter();

    public boolean canHandle(Map headers, HttpServletRequest request, HttpServletResponse response) {
    	String value = getAuthHeaderValue(request);
    	LOG.trace("BasicAuth -> "+value);
        if (value != "") {
            return true;
        }
        return false;
    }

    public boolean isAuthenticated(Map headers, HttpServletRequest request, HttpServletResponse response) {
    	String tenantDomainAPI = getTenantDomain(request.getRequestURI());	
    	String userName = null;
        try {
        	String authzHeaders = request.getHeader("Authorization");         
            String method = request.getMethod();
            if (authzHeaders != null) {
                //get the authorization header value, if provided
                String authzHeader = (String) authzHeaders;

                //decode it and extract username and password
                byte[] decodedAuthHeader = Base64.decode(authzHeader.split(" ")[1]);
                String authHeader = new String(decodedAuthHeader);
                if(authHeader != null) {
                	String[] val = authHeader.split(":");
                	if(val.length < 2) return false;
	                userName = authHeader.split(":")[0];
	                String password = authHeader.split(":")[1];
	                if (userName != null && password != null) {
	                    String tenantDomain = MultitenantUtils.getTenantDomain(userName);
	                    String tenantAwareUserName = MultitenantUtils.getTenantAwareUsername(userName);
	                    String userNameLessTenant = getUsernameLessTenant(userName);
	                    if( tenantDomain.equals(tenantDomainAPI) || tenantDomain.equals("carbon.super")) {
	                    	
		                    try {
		                        //get super tenant context and get realm service which is an osgi service
		                        RealmService realmService = (RealmService)
		                                PrivilegedCarbonContext.getThreadLocalCarbonContext().
		                                        getOSGiService(RealmService.class);
		                        if (realmService != null) {
		                            int tenantId = realmService.getTenantManager().getTenantId(tenantDomain);
		                            if (tenantId == -1) {
		                                LOG.error("Invalid tenant domain " + tenantDomain);
		                                return false;
		                            }
		                            //get tenant's user realm
		                            UserRealm userRealm = realmService.getTenantUserRealm(tenantId);
		                            boolean authenticated = userRealm.getUserStoreManager().
		                                    authenticate(tenantAwareUserName, password);
		                            boolean checkUserRights = secFilter.checkUserHasRights(userNameLessTenant, tenantDomain, method); 
		                            LOG.info("Inside Basic Auth, authenticated? " + authenticated + " hasRights? " + checkUserRights);
		                            if (authenticated && checkUserRights) {
		                                return true;
		                            } else {
		                                return false;
		                            }
		                        } else {
		                            return false;
		                        }
		                    } catch (UserStoreException e) {
		                        LOG.debug("UserStoreException. Return Unauthorized.", e);
		                        return false;
		                    }
		                }
	                    else{
	                        return false;
	                    }
	                } else {
	                    return false;
	                }
                } else {
                    return false;
                }

            } else {
                return false;
            }
        } catch (Exception e) {
            LOG.error("Error while trying to authenticate user", e);
            return false;
        }
    }
    
    /**
     * 
     * Retrieve the header token 
     * @param uri
     * @param tenantDomain
     * @return
     */
    private static String getAuthHeaderValue(HttpServletRequest request) {
    	String authValue = "";
        String authorization = request.getHeader("Authorization");
        if(authorization != null) {
        	String type = authorization.substring(0,5);
        	if ("basic".equalsIgnoreCase(type)) {
        		authValue = authorization.substring(6);
            }
        }
        return authValue;
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
            if (!foundTenantDelimiter && str.equals("rest")) {
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
    
    public String getUsernameLessTenant(String username) {
    	String userNameLessTenant = username;
    	int lastPos = username.lastIndexOf("@");
    	if(lastPos != -1)
    		userNameLessTenant = username.substring(0, lastPos);
		return userNameLessTenant;
    }
}
