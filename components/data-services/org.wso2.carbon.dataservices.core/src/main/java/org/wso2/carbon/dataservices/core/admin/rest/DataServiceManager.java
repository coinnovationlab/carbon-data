package org.wso2.carbon.dataservices.core.admin.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.dataservices.core.admin.DataServiceAdmin;
import org.wso2.carbon.dataservices.core.admin.ServiceAdminDS;
import org.wso2.carbon.service.mgt.ServiceMetaDataWrapper;
import org.wso2.carbon.user.api.UserStoreException;

public class DataServiceManager {

	private static final Log log = LogFactory.getLog(DataServiceManager.class);
    private static DataServiceManager dataServiceManager = new DataServiceManager();

    public static DataServiceManager getInstance() {
        return dataServiceManager;
    }

    public void saveDataService(String serviceName, String serviceHierarchy, String serviceContents, String tenantDomain) throws Exception {
    	try {
    		Utils.startTenantFlow(tenantDomain);
    		DataServiceAdmin dataServiceAdmin = new DataServiceAdmin();
            dataServiceAdmin.saveDataService(serviceName, serviceHierarchy, serviceContents);
        } catch (Exception e) {
			e.printStackTrace();
		} finally {
			Utils.endTenantFlow();
        }
    }
    
    public ServiceMetaDataWrapper listDataServices(String serviceTypeFilter, String serviceSearchString, int pageNumber, String tenantDomain) throws Exception {
    	ServiceMetaDataWrapper listDS = null;
    	try {
            Utils.startTenantFlow(tenantDomain);
            ServiceAdminDS serviceAdmin = new ServiceAdminDS();
        	listDS = serviceAdmin.listServices("ALL", serviceSearchString, pageNumber);
        } catch (UserStoreException e) {
			e.printStackTrace();
		} finally {
			Utils.endTenantFlow();
        }
		return listDS;
    }
    
    public String getDataService(String serviceName, String tenantDomain) throws Exception {
    	String data = null;
    	try {
            Utils.startTenantFlow(tenantDomain);
            DataServiceAdmin dataServiceAdmin = new DataServiceAdmin();
        	data = dataServiceAdmin.getDataServiceContentAsString(serviceName);
        } catch (UserStoreException e) {
			e.printStackTrace();
		} finally {
			Utils.endTenantFlow();
        }
		return data;
    }
    
    public void deleteDataService(String serviceName, String tenantDomain) throws Exception {
    	try {
            Utils.startTenantFlow(tenantDomain);
            ServiceAdminDS serviceAdmin = new ServiceAdminDS();
        	String [] serviceGroups = new String[1];
        	serviceGroups[0] = serviceName;
        	serviceAdmin.deleteServiceGroups(serviceGroups);
        } catch (UserStoreException e) {
			e.printStackTrace();
		} finally {
			Utils.endTenantFlow();
        }
    }
    
}
