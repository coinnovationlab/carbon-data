package co.innovation.lab.dss.rest.api;

import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.wso2.carbon.dataservices.common.conf.DynamicAuthConfiguration;
import org.wso2.carbon.dataservices.common.conf.DynamicODataConfig;
import org.wso2.carbon.dataservices.core.admin.rest.DataServiceManager;
import org.wso2.carbon.dataservices.ui.beans.Data;
import org.wso2.carbon.context.PrivilegedCarbonContext;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Utils {

	private Utils() {
    }

    public static DataServiceManager getDataServiceManager() {
    	return (DataServiceManager) PrivilegedCarbonContext.getThreadLocalCarbonContext()
    			.getOSGiService(DataServiceManager.class);
    }

    public static String getTenantDomain(int tenantId) throws  DataServicesCoreRestException{
        try {
            return org.wso2.carbon.dataservices.core.admin.rest.Utils.getTenantDomain(tenantId);
        }  catch (Exception e) {
            throw new DataServicesCoreRestException("Error while getting tenant domain of tenant id :"+tenantId, e);
        }
    }
    
    public static Data elaborateGetDS(String content) throws XMLStreamException{
    	Data data = new Data();
    	OMElement dynContentEl = AXIOMUtil.stringToOM(content);
    	data.populate(dynContentEl);
        return data;
    }
    
    public static Data elaborateSaveDS(Data dataService){
    	ObjectMapper objectMapper = new ObjectMapper();
    	Object dataConf;
    	String type;
    	for(int i=0;i<dataService.getConfigs().size();i++){
    		for(int j=0;j<dataService.getConfigs().get(i).getProperties().size();j++){
    			dataConf = dataService.getConfigs().get(0).getProperties().get(j).getValue();
    			type = dataService.getConfigs().get(0).getProperties().get(j).getName();
    			if(type.equals("dynamicOdataConfig")){
    				DynamicODataConfig obj = objectMapper.convertValue(dataConf, DynamicODataConfig.class);
    	        	dataService.getConfigs().get(i).getProperties().get(j).setValue(obj);
    			} else if(type.equals("dynamicUserAuthMapping")){
    				DynamicAuthConfiguration obj = objectMapper.convertValue(dataConf, DynamicAuthConfiguration.class);
    	        	dataService.getConfigs().get(i).getProperties().get(j).setValue(obj);
    			}
        	}
    	}
    	return dataService;
    }
}
