package org.wso2.carbon.dataservices.core.security.filter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.wso2.carbon.core.security.AuthenticatorsConfiguration;
import org.wso2.carbon.identity.authenticator.oauth2.sso.common.OAUTH2SSOAuthenticatorConstants;

public class ServicesSecurityFilterUtils  {
	
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
			e.printStackTrace();
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
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
