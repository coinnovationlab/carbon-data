package org.wso2.carbon.dataservices.core.security.filter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.wso2.carbon.core.util.AnonymousSessionUtil;
import org.wso2.carbon.dataservices.core.internal.DataServicesDSComponent;
import org.wso2.carbon.dataservices.core.odata.ODataServiceFault;
import org.wso2.carbon.identity.authenticator.oauth2.sso.common.AuthorizationToken;
import org.wso2.carbon.identity.authenticator.oauth2.sso.common.OAUTH2SSOAuthenticatorConstants;
import org.wso2.carbon.registry.core.service.RegistryService;
import org.wso2.carbon.user.core.UserRealm;
import org.wso2.carbon.user.core.UserStoreManager;
import org.wso2.carbon.user.core.service.RealmService;
import edu.emory.mathcs.backport.java.util.Arrays;

public class ServicesSecurityFilter  implements ServicesSecurityFilterInterface{
	
	private static final Log log = LogFactory.getLog(ServicesSecurityFilter.class);
	private static final String SECURITY_FILTER_4SERVICES 	= "SECURITY_FILTER_4SERVICES";
	private static final String SECURITY_FILTER_4MGT 		= "SECURITY_FILTER_4MGT";
	private static String method;
	/**
     * Retrieve the apikey parameter 
     * @param request
     * @return
     * @throws ODataServiceFault
     */
    private static String getApiKey(HttpServletRequest request) {
        String apikey = request.getParameter("apikey");
        return apikey;
    }
    
    /**
     * Retrieve the apikey parameter 
     * @param uri
     * @param tenantDomain
     * @return
     * @throws ODataServiceFault
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
     * Security control to filter OData/REST requests according to authorization token / apikey
     * @param request
     * @param tenantDomain
     * @return
     */
    @Override
    public boolean securityFilter(HttpServletRequest request, HttpServletResponse resp, String tenantDomain) {
    	boolean isAllowed = false;
    	try{
	    	isAllowed = filter(request, resp, tenantDomain, SECURITY_FILTER_4SERVICES);
    	}catch(Exception e) {
    		log.error("Security Filter error: "+e.getMessage());
    		return isAllowed;
    	}
    	return isAllowed;
    }
    
    /**
     * Security control to filter  data services CRUD Rest API requests according to authorization token / apikey
     * @param request
     * @param tenantDomain
     * @return
     */
    @Override
    public boolean managementSecurityFilter(HttpServletRequest request, HttpServletResponse resp, String tenantDomain) {
    	boolean isAllowed = false;
    	try{
    		isAllowed = filter(request, resp, tenantDomain, SECURITY_FILTER_4MGT);
    	}catch(Exception e) {
    		log.error("Security Filter error: "+e.getMessage());
    		return isAllowed;
    	}
    	return isAllowed;
    }
    
    private boolean filter(HttpServletRequest request, HttpServletResponse resp, String tenantDomain, String type) {
    	boolean isAllowed = false;
    	try{
	    	String authToken = getAuthHeaderToken(request);
	    	String apiKey = getApiKey(request);
	    	method = "";
	    	boolean checkIsProvider = false;
	    	boolean containsAuthToken = authToken != null && !authToken.trim().equals("");
	    	boolean containsApiKey = apiKey != null && !apiKey.trim().equals(""); 
	    	if(type.equals(SECURITY_FILTER_4MGT)) {
	    		checkIsProvider = true;
	    		method = request.getMethod();
	    	}
	    	if(containsAuthToken) {
	    		boolean aac_applicationToken =  checkAACUserOfAuthToken(authToken, tenantDomain, request, resp);
	    		if(aac_applicationToken == false) {
		    		boolean isAllowedUserAuthToken = checkValidityOfUser(tenantDomain, authToken, type, checkIsProvider, method);
		    		if (isAllowedUserAuthToken) {
		    			isAllowed = true;
		    		}
	    		} else {
	    			//get roles of the client owner of the token
	    			isAllowed = checkValidityOfClient(tenantDomain, authToken, type, checkIsProvider, method);
	    		}
	    	} else if(containsApiKey) {
	    		boolean isAllowedUserApiKey = checkAACUserOfApiKey(apiKey, tenantDomain, request, resp, checkIsProvider);
	    		if (isAllowedUserApiKey) {
	    			isAllowed = true;
	    		}
	    	} 
    	}catch(Exception e) {
    		log.error("Security Filter error: "+e.getMessage());
    		return isAllowed;
    	}
    	return isAllowed;
    }
    /**
     *  Refer to AAC to GET ApiKey Information - Retrieves username@tenant AND its relates list of Roles
     *  Check if user's context matches context of custom auth
     * 		control toward AAC
     *      	a. check list of roles of AAC having context according to authenticator configuration
     * @param apiKey
     * @param tenantDomain
     * @return
     */
    private static boolean checkAACUserOfApiKey(String apiKey, String tenant, HttpServletRequest request, HttpServletResponse resp, boolean checkIsProvider) {
    	boolean isAllowedUserApiKey = false;
    	String userApiKey = "";
    	try {
	    	String urlApiKeyCheck = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.APIKEY_CHECK_URL)+ "/"+apiKey; 
	    	Map <String,Object> response = handleGetRequest(urlApiKeyCheck);
	    	if(response != null) {
	    		String username = (String) response.get(ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.USER_NAME_FIELD));
	    		@SuppressWarnings("unchecked")
				List<String> rolesListResp =  (List<String>) response.get("roles");	
	    		if(username != null && rolesListResp != null) {
					String [] usernameArray = username.split("@");
					int length = usernameArray.length;
					userApiKey = (length == 3 || length == 2 ? usernameArray[0]+"@"+usernameArray[1] : usernameArray[0]);
					log.info("(apikey)user trying to request data: "+userApiKey+" odataTenant: "+tenant);
	    			boolean roleAccordingContext = elaborateRolesListApiKey(rolesListResp,tenant,checkIsProvider,method);
	    			return roleAccordingContext;
	    		} else {
					throw new Exception("Username/Roles are empty. Check ApiKey scopes.");
				}
			}
    	}catch(Exception e) {
    		log.error("checkAACUserOfApiKey error: "+e.getMessage());
    		return isAllowedUserApiKey;
    	}
    	return isAllowedUserApiKey;
    }
    
    /**
     * Refer to AAC to get Token Information - Retrieves username@tenant if token isValid 
     * Check if user's context matches context of custom auth
     * 		control toward AAC
     *			a.call GET /userroles/token/{token}
     *          b.check list of roles of AAC having context according to authenticator configuration
     *          
     * @param authToken
     * @param tenantDomain
     * @return
     */
    private static boolean checkAACUserOfAuthToken(String authToken, String oDataTenant, HttpServletRequest request, HttpServletResponse resp) {
    	String userAuthToken = "";
    	boolean aac_applicationToken = false;
    	try {
	    	String urlTokenApi = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CHECK_TOKEN_ENDPOINT_URL); 
	    	Map <String,Object> response = handlePostRequest(urlTokenApi,authToken);
			if(response != null) {
				String username = (String) response.get(ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.USER_NAME_FIELD));
				if(response.get("aac_applicationToken") != null)
					aac_applicationToken = (boolean) response.get("aac_applicationToken");
				boolean active = (boolean) response.get("active");
				if(active && username != null) {
					String [] usernameArray = username.split("@");
					int length = usernameArray.length;
					userAuthToken = (length == 3 || length == 2 ? usernameArray[0]+"@"+usernameArray[1] : usernameArray[0]);
					log.trace("(authtoken)user trying to request data: "+userAuthToken +" odataTenant: "+oDataTenant);				
				} else {
					throw new Exception("User Token not valid");
				}
			}
		}
    	catch(Exception e) {
    		log.error("checkAACUserOfAuthToken error: "+e.toString());
    		return aac_applicationToken;
    	}
    	return aac_applicationToken;
    }
    
    /**
     * Check if user has proper roles in the token/apikey in order to allow him to access the ODATA/REST services of the data service being exposed.(4Services reason)
     * Check if user is provider of the domain in order to be able to manage new or existing data services
     * by accessing the data services CRUD Rest API (4Mgt reason)
     * @param username
     * @param tenantDomain
     * @param authToken
     * @param request
     * @param resp
     * @param reason  '4Services' in case of REST/ODATA service access, '4Managment' in case of data services CRUD Rest API
     * @return
     */
    private static boolean checkValidityOfUser(String tenantDomain, String authToken, String reason, boolean checkIsProvider, String method) {
    	boolean roleAccordingContext = false;
    	String urlRolesApi =  ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.API_ROLE_INFO_URL);
    	roleAccordingContext = checkIsRoleAccordingly(urlRolesApi,tenantDomain,authToken,checkIsProvider,method);
		return roleAccordingContext;
    }
    
    private static boolean checkValidityOfClient(String tenantDomain, String authToken, String reason, boolean checkIsProvider, String method) {
    	boolean roleAccordingContext = false;
    	String urlRolesApi =  ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.API_CLIENT_ROLE_INFO_URL);
    	roleAccordingContext = checkIsRoleAccordingly(urlRolesApi,tenantDomain,authToken,checkIsProvider,method);
		return roleAccordingContext;
    }
    
    private static boolean checkIsRoleAccordingly(String urlRolesApi, String tenantDomain, String authToken, boolean checkIsProvider, String method) {
    	List<Map<String,Object>> rolesListResp =  handleGetRoles(urlRolesApi,authToken);
		boolean roleAccordingContext = elaborateRolesList(rolesListResp,tenantDomain,checkIsProvider,method);
		return roleAccordingContext;
    }

    public static boolean checkUserHasRights(String username, String tenantDomain, String method) {
    	boolean hasRights = false;
    	RegistryService registryService = DataServicesDSComponent.getRegistryService();
        RealmService realmService = DataServicesDSComponent.getRealmService();
        try {
			UserRealm realm = AnonymousSessionUtil.getRealmByTenantDomain(registryService,realmService, tenantDomain);
			if(realm != null) {
				UserStoreManager userstore = realm.getUserStoreManager();
				if (userstore.isExistingUser(username)) {
					String [] roleList = userstore.getRoleListOfUser(username);
					List<String> rolesList = Arrays.asList(roleList);
					boolean isAdmin = rolesList.contains("admin");
					if(isAdmin || method.equals("GET")) {
						hasRights = true;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return hasRights;
		}
        log.info("User "+username+" has proper rights in DSS? "+hasRights+" Tenant: "+tenantDomain);
    	return hasRights;
    }
    
    private static List<Map<String,Object>> handleGetRoles(String urlApi, String tokenToBeChecked){
    	try {
	    	HttpEntity<String> httpEntity;
	    	HttpHeaders headers = new HttpHeaders();
        	headers.add("Authorization","Bearer "+tokenToBeChecked);
        	httpEntity = new HttpEntity<String>(headers);
	    	RestTemplate restTemplate = new RestTemplate();
	    	ResponseEntity<List> response = restTemplate.exchange(urlApi, HttpMethod.GET, httpEntity, List.class);
	    	
			log.trace("response of GET API "+urlApi+" :  "+ response.toString()); 
			return response.getBody();
    	}catch(Exception e) {
    		log.error("handleGetRoles error GET API "+urlApi+ " : "+e.getMessage());
    	}
    	return null;
    }
    
	@SuppressWarnings("unchecked")
	private static Map<String,Object> handleGetRequest(String urlApi){
    	try {
	    	HttpEntity<String> httpEntity;
    		HttpHeaders headers = setHeaderBasicAuth();
        	httpEntity = new HttpEntity<String>(headers);
	    	RestTemplate restTemplate = new RestTemplate();
	    	ResponseEntity<Map> response = restTemplate.exchange(urlApi, HttpMethod.GET, httpEntity, Map.class);
	    	
			log.info("response of GET API "+urlApi+" :  "+ response.getBody().toString()); 
			return response.getBody();
    	}catch(Exception e) {
    		log.error("handleGetRequest error GET API "+urlApi+ " : "+e.getMessage());
    	}
    	return null;
    }
	
	@SuppressWarnings("unchecked")
	private static Map<String,Object> handlePostRequest(String urlApi, String authToken) throws Exception{
		urlApi += "?token=" + authToken;
    	try {
	    	HttpEntity<String> httpEntity;
    		HttpHeaders headers = setHeaderBasicAuth();
        	httpEntity = new HttpEntity<String>(headers);
	    	RestTemplate restTemplate = new RestTemplate();
	    	ResponseEntity<Map> response = restTemplate.exchange(urlApi, HttpMethod.POST, httpEntity, Map.class);
	    	
			log.info("response of POST API "+urlApi+" :  "+ response.getBody().toString()); 
			return response.getBody();
    	}catch(Exception e) {
    		log.error("handlePostRequest error POST API "+urlApi+ " : "+e.getMessage());
    		throw new Exception("handlePostRequest error POST API "+urlApi+ " : "+e.getMessage());
    	}
    }
    
   
    private static boolean elaborateRolesList(List<Map<String,Object>> rolesListResp, String serviceTenant, boolean checkIsProvider, String method) {
    	boolean containsProperRole = false;
    	boolean roleAccordingly = false;
    	String definedContext = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.ROLE_CONTEXT);
    	Map<String,Object> record;
    	String context,role,space;
    	boolean isProvider = false;
    	log.info("elaborating roles: "+rolesListResp.toString());
    	if(rolesListResp != null) {
	    	for(int i = 0;i<rolesListResp.size();i++) {
	    		record = rolesListResp.get(i);
	    		context = (String) record.get("context");
	    		role = (String) record.get("role");
	    		space = (String) record.get("space");
	    		log.info("oDataTenant: "+serviceTenant+" roleName: " +role+" context: " +context+" spaceName: " +space);
	    		if(context!= null && role!= null && context.equals(definedContext) && space.equals(serviceTenant)) {
	    			if(!isProvider)
	    				isProvider = isProvider(role, context, definedContext);
	    			containsProperRole = true;
	    		}
	    	}
	    	roleAccordingly = containsProperRole;
	    	if(checkIsProvider) {
	    		roleAccordingly = (containsProperRole && (isProvider || method.equals("GET")) );
	    	} 			
	    }
    	log.info("containsProperRole? " + containsProperRole + " check4Provider? " + checkIsProvider + " roleAccordingly? " + roleAccordingly);
    	return roleAccordingly;
    }
    
    private static boolean elaborateRolesListApiKey(List<String> rolesListResp, String serviceTenant, boolean checkIsProvider, String method) {
    	boolean containsProperRole = false;
    	boolean roleAccordingly = false;
    	String definedContext = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.ROLE_CONTEXT);
    	String record;
		String[] temp;
    	String role, space;
    	boolean isProvider = false;
    	log.info("elaborating roles of ApiKey: "+rolesListResp.toString());
    	if(rolesListResp != null) {
	    	for(int i = 0;i<rolesListResp.size();i++) {
	    		if(rolesListResp.get(i).contains(":")) {
	    			record = rolesListResp.get(i);
	    			temp = record.split(":");
	    			space = temp[0];
		    		role = temp[1];
		    		log.info("oDataTenant: "+serviceTenant+" roleName: " +role+" context_space " +space);
		    		if(space!= null && role!= null && space.contains(serviceTenant) && space.contains(definedContext)) {
		    			if(!isProvider)
		    				isProvider = isProvider(role, space, definedContext);
		    			containsProperRole = true;
		    			break;
		    		}
	    		}
	    	}
	    	roleAccordingly = containsProperRole;
	    	if(checkIsProvider) {
	    		roleAccordingly = (containsProperRole && (isProvider || method.equals("GET")) );
	    	} 			
	    }
    	log.info("containsProperRole? " + containsProperRole + " check4Provider? " + checkIsProvider + " roleAccordingly? " + roleAccordingly);
    	return roleAccordingly;
    }
    
    private static boolean isProvider(String roleName, String context, String definedContext) {
    	boolean isProvider = false;
    	String roleProvider	= ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.ROLE_PROVIDER);
    	if(context != null && roleName!= null && context.contains(definedContext) && roleName.equals(roleProvider)) {
    		isProvider = true;
    	}
    	return isProvider;
    }
 
    public static HttpHeaders setHeaderBasicAuth() {
    	String clientId 	= ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CLIENT_ID);
    	String cleintSecret = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CLIENT_SECRET);
		String val = clientId + ":" + cleintSecret;
		String encodedStr = new String(Base64.getEncoder().encode(val.getBytes()));
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.add("Authorization", "Basic " + encodedStr);
		return headers;
	}
}
