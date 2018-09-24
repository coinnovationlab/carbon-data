package org.wso2.carbon.dataservices.core.admin.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.dataservices.core.internal.DataServicesDSComponent;
import org.wso2.carbon.user.api.UserRealm;
import org.wso2.carbon.user.api.UserStoreException;
import org.wso2.carbon.user.core.service.RealmService;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    private static final Log LOG = LogFactory.getLog(Utils.class);

    private Utils() {
    }

    public static RealmService getRealmService() {
        return DataServicesDSComponent.getRealmService();
    }

    public static <T> List<T> intersection(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<T>();
        for (T t : list1) {
            if (list2.contains(t)) {
                list.add(t);
            }
        }
        return list;
    }

    public static UserRealm getUserRealm(String tenantDomain) throws UserStoreException {
        RealmService realmService = Utils.getRealmService();
        int tenantId = realmService.getTenantManager().getTenantId(tenantDomain);
        return realmService.getTenantUserRealm(tenantId);
    }

    public static int getTenantId(String tenantDomain) throws UserStoreException {
        return Utils.getRealmService().getTenantManager().getTenantId(tenantDomain);
    }

    public static String getTenantDomain(Integer tenantId) throws UserStoreException {
        return Utils.getRealmService().getTenantManager().getDomain(tenantId.intValue());
    }

	public static void startTenantFlow(String tenantDomain) throws Exception {
    	int tenantId = Utils.getTenantId(tenantDomain);
        if (tenantId == -1) {
            throw new Exception("Invalid Tenant Domain: " + tenantDomain);
        }
        if ( !tenantDomain.equals("carbon.super")) {
        	PrivilegedCarbonContext.startTenantFlow();
        	PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantDomain(tenantDomain);
            PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(tenantId);
        } 
    }

    public static void endTenantFlow() throws Exception  {
    	PrivilegedCarbonContext.endTenantFlow();
    }

}
