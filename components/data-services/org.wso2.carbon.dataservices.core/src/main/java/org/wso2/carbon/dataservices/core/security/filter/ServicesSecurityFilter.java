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
	private static final String SECURITY_FILTER_TOKEN_CLIENT_ID 				= "clientToken";
	private static final String SECURITY_FILTER_TOKEN_CLIENT_EXPIRE 			= "clientTokenExpiresIn";
	private static final String SECURITY_FILTER_TOKEN_CLIENT_GENERATION_TIME 	= "clientTokenGenerationTime";
	private static final String SECURITY_FILTER_TOKEN_CLIENT_ELEMENTS		 	= "clientTokenElements";
	private static final String SECURITY_FILTER_4SERVICES	 			= "SECURITY_FILTER_4SERVICES";
	private static final String SECURITY_FILTER_4MGT 					= "SECURITY_FILTER_4MGT";
	private static String method;
	private static Map<String,Object> accessTokens = new HashMap<>(); 
	//{"clientToken" : "1234", "expires_in" : "1234", "tokenElements" : [TokenInfo, TokenInfo....] }
	
	
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
	    	String authToken = getAuthHeaderToken(request);
	    	String apiKey = getApiKey(request);
	    	boolean containsAuthToken = authToken != null && !authToken.equals("");
	    	boolean containsApiKey = apiKey != null && !apiKey.equals("");  
	    	if(containsAuthToken) {
	    		String userAuthToken = checkAACUserOfAuthToken(authToken, tenantDomain, request, resp);
	    		if(userAuthToken != null && !userAuthToken.equals("")) {
		    		boolean isAllowedUserAuthToken = checkValidityOfUser(userAuthToken, tenantDomain, authToken, request, resp, SECURITY_FILTER_4SERVICES, "");
		    		if (isAllowedUserAuthToken) {
		    			isAllowed = true;
		    		}
	    		}
	    	} else if(containsApiKey) {
	    		boolean isAllowedUserApiKey = checkAACUserOfApiKey(apiKey, tenantDomain, request, resp, false);
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
     * Security control to filter  data services CRUD Rest API requests according to authorization token / apikey
     * @param request
     * @param tenantDomain
     * @return
     */
    @Override
    public boolean managementSecurityFilter(HttpServletRequest request, HttpServletResponse resp, String tenantDomain) {
    	boolean isAllowed = false;
    	try{
	    	String authToken = getAuthHeaderToken(request);
	    	String apiKey = getApiKey(request);
	    	method = request.getMethod();
	    	boolean containsAuthToken = authToken != null && !authToken.trim().equals("");
	    	boolean containsApiKey = apiKey != null && !apiKey.trim().equals("");  
	    	if(containsAuthToken) {
	    		String userAuthToken = checkAACUserOfAuthToken(authToken, tenantDomain, request, resp);
	    		if(userAuthToken != null && !userAuthToken.equals("")) {
		    		boolean isAllowedUserAuthToken = checkValidityOfUser(userAuthToken, tenantDomain, authToken, request, resp, SECURITY_FILTER_4MGT, method);
		    		if (isAllowedUserAuthToken) {
		    			isAllowed = true;
		    		}
	    		}
	    	} else if(containsApiKey) {
	    		boolean isAllowedUserApiKey = checkAACUserOfApiKey(apiKey, tenantDomain, request, resp, true);
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
     *  Refer to AAC to  GET ApiKey Information - Retrieves username@tenant AND its relates list of Roles
     *  Check if user's context matches context of custom auth
     * 		1. control inside DSS
     * 			a. check if user exists inside DSS
     *   	else
     *      2. control toward AAC
     *      	a. check list of roles of AAC having context and prefix according to authenticator configuration
     * @param apiKey
     * @param tenantDomain
     * @return
     */
    private static boolean checkAACUserOfApiKey(String apiKey, String tenant, HttpServletRequest request, HttpServletResponse resp, boolean checkIsProvider) {
    	boolean isAllowedUserApiKey = false;
    	String userApiKey = "";
    	try {
	    	String urlApiKeyCheck = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.APIKEY_CHECK_URL)+ "/"+apiKey; 
	    	String clientToken = retrieveClientToken(request, resp);
	    	Map <String,Object> response = handleGetRequest(urlApiKeyCheck,clientToken);
	    	if(response != null) {
	    		String username = (String) response.get(ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.USER_NAME_FIELD));
	    		@SuppressWarnings("unchecked")
				List<Map<String,Object>> rolesListResp =  (List<Map<String, Object>>) response.get("roles");	
	    		if(username != null ) {
					String [] usernameArray = username.split("@");
					int length = usernameArray.length;
					userApiKey = (length == 3 ? usernameArray[0]+"@"+usernameArray[1] : usernameArray[0]);
					String userDomain = usernameArray[length-1];
					log.info("(apikey)user trying to request data: "+userApiKey+" userDoamin: "+userDomain+" odataTenant: "+tenant);
					boolean existsInDSS = checkUserExistsInDSS(userApiKey, tenant);
		    		if(existsInDSS) {
		    			return true;
		    		} else {
		    			boolean roleAccordingContext = elaborateRolesList(rolesListResp,tenant,checkIsProvider,method);
		    			return roleAccordingContext;
		    		}
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
     * 		1. control inside DSS
     * 			a. check if user exists inside DSS
     * 		else
     *		2. control toward AAC
     *			a.call GET /userroles/token/{token}
     *          b.check list of roles of AAC having context and prefix according to authenticator configuration
     *          
     * @param authToken
     * @param tenantDomain
     * @return
     */
    private static String checkAACUserOfAuthToken(String authToken, String oDataTenant, HttpServletRequest request, HttpServletResponse resp) {
    	String userAuthToken = "";
    	try {
	    	String urlTokenApi = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CHECK_TOKEN_ENDPOINT_URL); 
	    	Map <String,Object> response = handlePostRequest(urlTokenApi,authToken);
			if(response != null) {
				String username = (String) response.get(ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.USER_NAME_FIELD));
				boolean active = (boolean) response.get("active");
				if(active && username != null) {
					String [] usernameArray = username.split("@");
					int length = usernameArray.length;
					userAuthToken = (length == 3 || length == 2 ? usernameArray[0]+"@"+usernameArray[1] : usernameArray[0]);
					String userDomain = usernameArray[length-1];
					log.trace("(authtoken)user trying to request data: "+userAuthToken+" context: "+userDomain+" odataTenant: "+oDataTenant);				
				} else {
					throw new Exception("User Token not valid");
				}
			}
		}
    	catch(Exception e) {
    		log.error("checkAACUserOfAuthToken error: "+e.toString());
    		return userAuthToken;
    	}
    	return userAuthToken;
    }
    
    private static boolean compareContext(String userContext) {
    	boolean theyMatch = false;
    	String definedContext = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.ROLE_CONTEXT);
    	theyMatch = definedContext.equalsIgnoreCase(userContext);
    	log.info("comparing context: "+ " defined:"+definedContext+" serviceTenantContext: "+userContext+" " +theyMatch);
    	return theyMatch;
    }
    
    /**
     * Check if user exists in DSS in order to allow him to access the ODATA/REST services of the data service being exposed.(4Services reason)
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
    private static boolean checkValidityOfUser(String username, String tenantDomain, String authToken, HttpServletRequest request, HttpServletResponse resp, String reason, String method) {
    	if(reason.equals(SECURITY_FILTER_4SERVICES)) {
    		boolean existsInDSS = checkUserExistsInDSS(username, tenantDomain);
    		if(existsInDSS) {
    			return true;
    		} else {
    			boolean roleAccordingContext = checkIsRoleAccordingly(tenantDomain,authToken,request,resp,false, "");
    			return roleAccordingContext;
    		}
    	} else if(reason.equals(SECURITY_FILTER_4MGT)) {
    		boolean existsInDSS = checkUserExistsInDSS(username, tenantDomain);
    		boolean userHasRights = checkUserHasRights(username, tenantDomain, method);
    		if(existsInDSS && userHasRights) {
    			return true;
    		} else {
    			boolean roleAccordingContext = checkIsRoleAccordingly(tenantDomain,authToken,request,resp,true,method);
    			return roleAccordingContext;
    		}
		}
    	return false;
    }
    
    private static boolean checkIsRoleAccordingly(String tenantDomain, String authToken, HttpServletRequest request, HttpServletResponse resp, boolean checkIsProvider, String method) {
    	String urlRolesApi =  ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.API_ROLE_INFO_URL);
    	List<Map<String,Object>> rolesListResp =  handleGetRoles(urlRolesApi,authToken,request,resp);
		boolean roleAccordingContext = elaborateRolesList(rolesListResp,tenantDomain,checkIsProvider,method);
		return roleAccordingContext;
    }
    
    private static boolean checkUserExistsInDSS(String username, String tenantDomain) {
    	boolean exists = false;
    	RegistryService registryService = DataServicesDSComponent.getRegistryService();
        RealmService realmService = DataServicesDSComponent.getRealmService();
        if(username != null && !username.equals("")) {
	        try {
				UserRealm realm = AnonymousSessionUtil.getRealmByTenantDomain(registryService,realmService, tenantDomain);
				if(realm != null) {
					UserStoreManager userstore = realm.getUserStoreManager();
					if( !tenantDomain.equals("carbon.super") && username.equals("admin")) {
						exists = false;
					}else if (userstore.isExistingUser(username)) {
						exists = true;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				return exists;
			}
        }
        log.info("User "+username+" exists in DSS? "+exists+" Tenant: "+tenantDomain);
    	return exists;
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
    
    private static List<Map<String,Object>> handleGetRoles(String urlApi, String tokenToBeChecked, HttpServletRequest request, HttpServletResponse resp){
    	try {
	    	HttpEntity<String> httpEntity;
//	    	if(tokenToBeChecked != null) {
//	    		urlApi += "/" + tokenToBeChecked;
//	    	}
//	    	String clientToken = retrieveClientToken(request, resp);
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
	private static Map<String,Object> handleGetRequest(String urlApi, String authToken){
    	try {
	    	HttpEntity<String> httpEntity;
	    	if(authToken != null) {
	    		HttpHeaders headers = new HttpHeaders();
	        	headers.add("Authorization","Bearer "+authToken);
	        	httpEntity = new HttpEntity<String>(headers);
	    	}else {
	    		httpEntity = HttpEntity.EMPTY;
	    	}
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
    
    private static boolean isProvider(String roleName, String context, String definedContext) {
    	boolean isProvider = false;
    	if(context != null && roleName!= null && context.equals(definedContext) 
    			&& roleName.equals(OAUTH2SSOAuthenticatorConstants.ROLE_PROVIDER)) {
    		isProvider = true;
    	}
    	return isProvider;
    }
    
    private static String retrieveClientToken(HttpServletRequest request, HttpServletResponse resp) {

    	String securityTokenClient = getSSOTokenClient();
    	int expiresIn 			= accessTokens.get(SECURITY_FILTER_TOKEN_CLIENT_EXPIRE) != null ?Integer.parseInt((String) accessTokens.get(SECURITY_FILTER_TOKEN_CLIENT_EXPIRE)) : 0;
        String generationTime 	= (String) accessTokens.get(SECURITY_FILTER_TOKEN_CLIENT_GENERATION_TIME);
    	String clientToken = "";
    	log.info("expiresIn: " + expiresIn);
    	log.info("generationTime: " + generationTime);
    	boolean hasExpired = hasExpiredClient(expiresIn,generationTime);
    	if (securityTokenClient == null || hasExpired) {
	    	String url_token = ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.TOKEN_URL);
	    	MultiValueMap<String,String> dataToBeSent = new LinkedMultiValueMap<String,String>();
	    	dataToBeSent.add("client_id", ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CLIENT_ID));
	    	dataToBeSent.add("client_secret", ServicesSecurityFilterUtils.authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CLIENT_SECRET));
	    	dataToBeSent.add("grant_type", "client_credentials");
	    	
	    	HttpHeaders headers = new HttpHeaders();
	    	MediaType contType = MediaType.APPLICATION_FORM_URLENCODED;
	        headers.setContentType(contType);
	        HttpEntity<MultiValueMap<String,String>> requestBody = new HttpEntity<MultiValueMap<String,String>>(dataToBeSent, headers);
	        RestTemplate restTemplate = new RestTemplate();
	        HttpMessageConverter formHttpMessageConverter = new FormHttpMessageConverter();
	        HttpMessageConverter jsonHttpMessageConverternew = new  MappingJackson2HttpMessageConverter();
			List<HttpMessageConverter<?>> list = new ArrayList<HttpMessageConverter<?>>();
			list.add(formHttpMessageConverter);
			list.add(jsonHttpMessageConverternew);
			restTemplate.setMessageConverters(list);
			AuthorizationToken response = restTemplate.postForObject(url_token, requestBody, AuthorizationToken.class);
			log.info("NEW: obtain client_credentials token : "+response.getAccess_token());
	        clientToken = response.getAccess_token();
	        String clientTokenExp = Integer.toString(response.getExpires_in());
	        storeSSOClientToken(clientToken,clientTokenExp);
    	}else {
    		clientToken = securityTokenClient;
    	}
    	return clientToken;
    }
    
    @SuppressWarnings("unchecked")
	private static void storeSSOClientToken(String securityTokenID, String securityTokenExpire) {
    	accessTokens.put(SECURITY_FILTER_TOKEN_CLIENT_ID, securityTokenID);
    	accessTokens.put(SECURITY_FILTER_TOKEN_CLIENT_EXPIRE, 	securityTokenExpire);
    	accessTokens.put(SECURITY_FILTER_TOKEN_CLIENT_GENERATION_TIME, 	LocalDateTime.now().toString());
    	log.trace("setting tokenClient: " 					+SECURITY_FILTER_TOKEN_CLIENT_ID+": " + securityTokenID);
		log.trace("setting tokenClient Generation Time: "	+SECURITY_FILTER_TOKEN_CLIENT_GENERATION_TIME+": " + LocalDateTime.now().toString());
		log.trace("setting tokenClient Expire: "			+SECURITY_FILTER_TOKEN_CLIENT_EXPIRE+": " + securityTokenExpire);
	}
    
    private static void storeSSOUserTokens(String userToken, String userApiKey, int userKeyExpiresIn, int issuedTime) {
    	TokenInfo info = new TokenInfo();
    	if(userToken != null)
    		info.setToken(userToken);
    	else if(userApiKey != null)
    		info.setApikey(userApiKey);
    	info.setExpiresin(userKeyExpiresIn);
    	info.setIssuedTime(issuedTime);
    	List<TokenInfo> tokenList = (List<TokenInfo>) accessTokens.get(SECURITY_FILTER_TOKEN_CLIENT_ELEMENTS);
    	if(tokenList != null)    		
    		accessTokens.put(SECURITY_FILTER_TOKEN_CLIENT_ELEMENTS, tokenList.add(info));
    	else {
    		tokenList = new ArrayList<TokenInfo>();
    		accessTokens.put(SECURITY_FILTER_TOKEN_CLIENT_ELEMENTS, tokenList.add(info));
    	}
    	tokenList = (List<TokenInfo>) accessTokens.get(SECURITY_FILTER_TOKEN_CLIENT_ELEMENTS);
    	log.info("Number of user keys for the current cleintToken: " + tokenList.size());
	}

    private static String getSSOTokenClient() {
        String clientToken  	= (String) accessTokens.get(SECURITY_FILTER_TOKEN_CLIENT_ID);
        log.trace("getting Token Client: " + clientToken);
    	return clientToken;
    }
    
    @SuppressWarnings("unchecked")
	private static TokenInfo getSSOTokenClientElements(String userToken, String userApiKey) {
        List<TokenInfo> clientTokenElements = (List<TokenInfo>) accessTokens.get(SECURITY_FILTER_TOKEN_CLIENT_ELEMENTS);
        TokenInfo tokenInfo = null;
        if(clientTokenElements != null && clientTokenElements.size() > 0) {
        	for(int i = 0; i<clientTokenElements.size(); i++) {
        		if(userApiKey != null && clientTokenElements.get(i).getApikey().equals(userApiKey)) {
        			tokenInfo = new TokenInfo();
        			tokenInfo.setApikey(userApiKey);
        			tokenInfo.setExpiresin(clientTokenElements.get(i).getExpiresin());
        			break;
        		} else if(userToken != null && clientTokenElements.get(i).getToken().equals(userToken)) {
        			tokenInfo = new TokenInfo();
        			tokenInfo.setToken(userToken);
        			tokenInfo.setExpiresin(clientTokenElements.get(i).getExpiresin());
        			break;
        		}
        	}
        	return tokenInfo;
        }
        return tokenInfo;
    }
    
    private static boolean hasExpiredClient(int tokenExpiration, String tokenGenerationTime) {
    	boolean hasExp = false;
    	log.info("CLIENT TOKEN: check if client token has expired ");
    	try {
	    	if(tokenExpiration != 0 && tokenGenerationTime != null) {
		    	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		    	LocalDateTime now = LocalDateTime.now();
		    	LocalDateTime generationTime = LocalDateTime.parse(tokenGenerationTime);
		    	Long expIn = new Long(tokenExpiration);
		    	Long seconds = generationTime.until(now, ChronoUnit.SECONDS);
		    	if(expIn > 0) {
		    		hasExp = seconds > expIn;
		    	} else {
		    		throw new Exception();
		    	}
		    	log.info("CLIENT TOKEN: diff in seconds: "+seconds+" "+hasExp);
		    	log.info("CLIENT TOKEN: now time format: "+dtf.format(now)); 
		    	log.info("CLIENT TOKEN: generation time format: "+dtf.format(generationTime)); 
		    }
    	} catch(Exception e) {
    		e.printStackTrace();
    		log.error("AAC ERROR: Missing expiration time of client token");
    		return true;
    	}
    	return hasExp;
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
