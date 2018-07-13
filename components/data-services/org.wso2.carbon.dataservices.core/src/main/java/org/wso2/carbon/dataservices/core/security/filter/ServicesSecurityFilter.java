package org.wso2.carbon.dataservices.core.security.filter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.Cookie;
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
import org.wso2.carbon.core.security.AuthenticatorsConfiguration;
import org.wso2.carbon.core.util.AnonymousSessionUtil;
import org.wso2.carbon.dataservices.core.internal.DataServicesDSComponent;
import org.wso2.carbon.dataservices.core.odata.ODataServiceFault;
import org.wso2.carbon.identity.authenticator.oauth2.sso.common.AuthorizationToken;
import org.wso2.carbon.identity.authenticator.oauth2.sso.common.OAUTH2SSOAuthenticatorConstants;
import org.wso2.carbon.registry.core.service.RegistryService;
import org.wso2.carbon.user.core.UserRealm;
import org.wso2.carbon.user.core.UserStoreManager;
import org.wso2.carbon.user.core.service.RealmService;

public class ServicesSecurityFilter {
	
	private static final Log log = LogFactory.getLog(ServicesSecurityFilter.class);
	private static String SECURITY_FILTER_TOKEN_ID = "securityFilterTokenId";
	private static String SECURITY_FILTER_TOKEN_EXPIRE = "securityFilterTokenExpire";
	private static String SECURITY_FILTER_TOKEN_GENERATION_TIME = "securityFilterTokenGenerationTime";
	private static final int MAX_EXPIRE = Integer.parseInt(authenticatorConfig(OAUTH2SSOAuthenticatorConstants.MAX_EXPIRE_SEC_TOKEN));
	/**
     * 
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
     * 
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
    public static boolean securityFilter(HttpServletRequest request, HttpServletResponse resp, String tenantDomain) {
    	boolean isAllowed = false;
    	try{
	    	String authToken = getAuthHeaderToken(request);
	    	String apiKey = getApiKey(request);
	    	boolean containsAuthToken = authToken != null && !authToken.equals("");
	    	boolean containsApiKey = apiKey != null && !apiKey.equals("");  
	    	if(containsAuthToken) {
	    		boolean isAllowedUserAuthToken = checkAACUserOfAuthToken(authToken, tenantDomain, request, resp);
	    		if (isAllowedUserAuthToken) {
	    			isAllowed = true;
	    		}
	    	} else if(containsApiKey) {
	    		boolean isAllowedUserApiKey = checkAACUserOfApiKey(apiKey, tenantDomain, request, resp);
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
    private static boolean checkAACUserOfApiKey(String apiKey, String oDataTenant, HttpServletRequest request, HttpServletResponse resp) {
    	boolean checkApiKey = false;
    	try {
    	String urlApiKeyCheck = authenticatorConfig(OAUTH2SSOAuthenticatorConstants.APIKEY_CHECK_URL)+ "/"+apiKey; 
    	Map <String,Object> response = handleGetRequest(urlApiKeyCheck,null);
    	if(response != null) {
    		String username = (String) response.get(authenticatorConfig(OAUTH2SSOAuthenticatorConstants.USER_NAME_FIELD));
    		if(username != null ) {
				String [] usernameArray = username.split("@");
				int length = usernameArray.length;
				String user = (length == 3 ? usernameArray[0]+"@"+usernameArray[1] : usernameArray[0]);
				String userDomain = usernameArray[length-1];
				log.info("(apikey)user trying to request data: "+user+" context: "+userDomain);
				
				boolean existsInDSS = checkUserExistsInDSS(user, oDataTenant);
				if(existsInDSS) {
					return true;
				}else {
					@SuppressWarnings("unchecked")
					List<Map<String,Object>> rolesListResp =  (List<Map<String, Object>>) response.get("roles");
					boolean roleAccordingContext = elaborateRolesList(rolesListResp, oDataTenant);
					if(roleAccordingContext) {
						return true;
					}
				}
	    	}
		}
    	}catch(Exception e) {
    		log.error("checkAACUserOfApiKey error: "+e.getMessage());
    		return checkApiKey;
    	}
    	return checkApiKey;
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
    private static boolean checkAACUserOfAuthToken(String authToken, String oDataTenant, HttpServletRequest request, HttpServletResponse resp) {
    	boolean checkAuthToken = false;
    	try {
	    	String urlTokenApi = authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CHECK_TOKEN_ENDPOINT_URL); 
	    	Map <String,Object> response = handleGetRequest(urlTokenApi,authToken);
			if(response != null) {
				String username = (String) response.get(authenticatorConfig(OAUTH2SSOAuthenticatorConstants.USER_NAME_FIELD));
				if(username != null) {
					String [] usernameArray = username.split("@");
					int length = usernameArray.length;
					String user = (length == 3 ? usernameArray[0]+"@"+usernameArray[1] : usernameArray[0]);
					String userDomain = usernameArray[length-1];
					log.info("(authtoken)user trying to request data: "+user+" context: "+userDomain);
					
					boolean existsInDSS = checkUserExistsInDSS(user, oDataTenant);
					if(existsInDSS) {
						return true;
					}else {
						String urlRolesApi =  authenticatorConfig(OAUTH2SSOAuthenticatorConstants.ROLES_OF_TOKEN_URL);
						List<Map<String,Object>> rolesListResp =  handleGetRoles(urlRolesApi,authToken,request,resp);
						boolean roleAccordingContext = elaborateRolesList(rolesListResp,oDataTenant);
						if(roleAccordingContext) {
							return true;
						}
					}
				}
			}
		}
    	catch(Exception e) {
    		log.error("checkAACUserOfAuthToken error: "+e.toString());
    		return checkAuthToken;
    	}
    	return checkAuthToken;
    }
    
    private static boolean compareContext(String userContext) {
    	boolean theyMatch = false;
    	String definedContext = authenticatorConfig(OAUTH2SSOAuthenticatorConstants.ROLE_CONTEXT);
    	theyMatch = definedContext.equalsIgnoreCase(userContext);
    	log.info("comparing context: "+ " defined:"+definedContext+" serviceTenantContext: "+userContext+" " +theyMatch);
    	return theyMatch;
    }
    
    private static boolean checkUserExistsInDSS(String username, String tenantDomain) {
    	boolean exists = false;
    	RegistryService registryService = DataServicesDSComponent.getRegistryService();
        RealmService realmService = DataServicesDSComponent.getRealmService();
        try {
			UserRealm realm = AnonymousSessionUtil.getRealmByTenantDomain(registryService,realmService, tenantDomain);
			if(realm != null) {
				UserStoreManager userstore = realm.getUserStoreManager();
				if (userstore.isExistingUser(username)) {
					exists = true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return exists;
		}
    	return exists;
    }
    
    private static List<Map<String,Object>> handleGetRoles(String urlApi, String tokenToBeChecked, HttpServletRequest request, HttpServletResponse resp){
    	try {
	    	HttpEntity<String> httpEntity;
	    	if(tokenToBeChecked != null) {
	    		urlApi += "/" + tokenToBeChecked;
	    	}
	    	String clientToken = retrieveClientToken(request, resp);
	    	HttpHeaders headers = new HttpHeaders();
        	headers.add("Authorization","Bearer "+clientToken);
        	httpEntity = new HttpEntity<String>(headers);
	    	RestTemplate restTemplate = new RestTemplate();
	    	ResponseEntity<List> response = restTemplate.exchange(urlApi, HttpMethod.GET, httpEntity, List.class);
	    	
			log.info("response of GET API "+urlApi+" :  "+ response.toString()); 
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
    
    private static String authenticatorConfig(String paramName) {
    	AuthenticatorsConfiguration authenticatorsConfiguration = AuthenticatorsConfiguration.getInstance();
        AuthenticatorsConfiguration.AuthenticatorConfig authenticatorConfig =
                authenticatorsConfiguration.getAuthenticatorConfig(OAUTH2SSOAuthenticatorConstants.AUTHENTICATOR_NAME);
        String paramValue = "";

        if (authenticatorConfig != null) {
            Map<String, String> configParameters = authenticatorConfig.getParameters();
            paramValue = configParameters.get(paramName);
        }
        return paramValue;
    }
    
    private static boolean elaborateRolesList(List<Map<String,Object>> rolesListResp, String serviceTenant) {
    	boolean containsProperRole = false;
    	String definedContext = authenticatorConfig(OAUTH2SSOAuthenticatorConstants.ROLE_CONTEXT);
    	String definedPrefix = authenticatorConfig(OAUTH2SSOAuthenticatorConstants.ROLE_PREFIX);
    	int lngth = definedPrefix.length();
    	Map<String,Object> record;
    	String context,role,no_prefix;
    	log.info("elaborating roles: "+rolesListResp.toString());
    	if(rolesListResp != null) {
	    	for(int i = 0;i<rolesListResp.size();i++) {
	    		record = rolesListResp.get(i);
	    		context = (String) record.get("context");
	    		role = (String) record.get("role");
	    		no_prefix = role.substring(lngth);
	    		if(role.startsWith(definedPrefix) && context.equals(definedContext) && no_prefix.equals(serviceTenant)) {
	    			containsProperRole = true;
	    			break;
	    		}
	    	}
	    }
    	return containsProperRole;
    }
    
    private static String retrieveClientToken(HttpServletRequest request, HttpServletResponse resp) {

    	Cookie securityTokenCookie = getSSOTokenCookie(request,SECURITY_FILTER_TOKEN_ID);
    	Cookie securityTokenCookieExpiration = getSSOTokenCookie(request,SECURITY_FILTER_TOKEN_EXPIRE);
    	Cookie securityTokenCookieGenerationTime = getSSOTokenCookie(request,SECURITY_FILTER_TOKEN_GENERATION_TIME);
    	String clientToken = "";
    	boolean hasExpired = hasExpired(securityTokenCookieExpiration,securityTokenCookieGenerationTime);
    	if (securityTokenCookie == null || hasExpired) {
	    	String url_token = authenticatorConfig(OAUTH2SSOAuthenticatorConstants.TOKEN_URL);
	    	MultiValueMap<String,String> dataToBeSent = new LinkedMultiValueMap<String,String>();
	    	dataToBeSent.add("client_id", authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CLIENT_ID));
	    	dataToBeSent.add("client_secret", authenticatorConfig(OAUTH2SSOAuthenticatorConstants.CLIENT_SECRET));
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
	        storeSSOTokenCookie(clientToken,clientTokenExp,request,resp);
    	}else {
    		clientToken = securityTokenCookie.getValue();
    	}
    	return clientToken;
    }
    
    private static void storeSSOTokenCookie(String securityTokenID, String securityTokenExpire, HttpServletRequest req, HttpServletResponse resp) {
		Cookie securityTokenCookie = new Cookie(SECURITY_FILTER_TOKEN_ID, securityTokenID);
		securityTokenCookie.setSecure(true);
		Cookie securityTokenCookieExpiration = new Cookie(SECURITY_FILTER_TOKEN_EXPIRE, securityTokenExpire);
		securityTokenCookieExpiration.setSecure(true);
		Cookie securityTokenCookieGenerationTime = new Cookie(SECURITY_FILTER_TOKEN_GENERATION_TIME, LocalDateTime.now().toString());
		securityTokenCookieGenerationTime.setSecure(true);
		log.info("setting cookies: "+SECURITY_FILTER_TOKEN_ID+": "+securityTokenCookie.getValue());
		log.info("setting cookies: "+SECURITY_FILTER_TOKEN_GENERATION_TIME+": "+securityTokenCookieGenerationTime.getValue());
		log.info("setting cookies: "+SECURITY_FILTER_TOKEN_EXPIRE+": "+securityTokenCookieExpiration.getValue());
		resp.addCookie(securityTokenCookie);
		resp.addCookie(securityTokenCookieExpiration);
		resp.addCookie(securityTokenCookieGenerationTime);
	}
    
    private static Cookie getSSOTokenCookie(HttpServletRequest req, String paramName) {
        Cookie[] cookies = req.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (paramName.equals(cookie.getName())) {
                	log.info("getting cookies: : "+ paramName+" " +cookie.getValue());
                    return cookie;
                }
            }
        }
        return null;
    }
    
    private static boolean hasExpired(Cookie securityTokenCookieExpiration, Cookie securityTokenCookieGenerationTime) {
    	boolean hasExp = false;
    	if(securityTokenCookieExpiration != null && securityTokenCookieGenerationTime != null) {
	    	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	    	LocalDateTime now = LocalDateTime.now();
	    	LocalDateTime generationTime = LocalDateTime.parse(securityTokenCookieGenerationTime.getValue());
	    	Long expIn = Long.parseLong(securityTokenCookieExpiration.getValue());
	    	Long seconds = generationTime.until(now, ChronoUnit.SECONDS);
	    	if(expIn > 0) {
	    		hasExp = seconds > expIn;
	    	}else {
	    		hasExp = seconds > MAX_EXPIRE;
	    	}
	    	log.info("diff in seconds: "+seconds+" "+hasExp);
	    	log.info("now time format: "+dtf.format(now)); 
	    	log.info("generation time format: "+dtf.format(generationTime)); 
	    }
    	return hasExp;
    }

}
