package org.wso2.carbon.dataservices.core.security.filter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.core.security.AuthenticatorsConfiguration;
import org.wso2.carbon.identity.authenticator.oauth2.sso.common.OAUTH2SSOAuthenticatorConstants;

public class ServicesSecurityFilterUtils  {
	
	private static final Log log = LogFactory.getLog(ServicesSecurityFilterUtils.class);
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ServicesSecurityFilterInterface initializeSecurityFilter(){
		ServicesSecurityFilterInterface secureFilter = null;
		String className = authenticatorConfig("SecurityFilterClass");
		Class clazz;
		Constructor constructor;
		try {
			clazz = Class.forName(className);
			constructor = clazz.getConstructor();
			Object newObject = constructor.newInstance();
			secureFilter = (ServicesSecurityFilterInterface) newObject;
		} catch (ClassNotFoundException e) {
			log.info("Error in getting SecurityFilter Class: "+e.getMessage());
			e.printStackTrace();
		} catch (NoSuchMethodException | SecurityException e) {
			log.info("Error in getting SecurityFilter Class: "+e.getMessage());
			e.printStackTrace();
		} catch (InstantiationException e) {
			log.info("Error in getting SecurityFilter Class: "+e.getMessage());
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			log.info("Error in getting SecurityFilter Class: "+e.getMessage());
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			log.info("Error in getting SecurityFilter Class: "+e.getMessage());
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			log.info("Error in getting SecurityFilter Class: "+e.getMessage());
			e.printStackTrace();
		}	
		return secureFilter;
	}
	
	public static String authenticatorConfig(String paramName) {
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
}
