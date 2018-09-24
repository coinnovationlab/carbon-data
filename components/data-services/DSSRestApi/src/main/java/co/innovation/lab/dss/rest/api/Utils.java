package co.innovation.lab.dss.rest.api;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.wso2.carbon.dataservices.common.conf.DynamicAuthConfiguration;
import org.wso2.carbon.dataservices.common.conf.DynamicODataConfig;
import org.wso2.carbon.dataservices.common.conf.ODataColumnsConfig;
import org.wso2.carbon.dataservices.common.conf.ODataTableSchemaConfig;
import org.wso2.carbon.dataservices.core.admin.rest.DataServiceManager;
import org.wso2.carbon.dataservices.ui.beans.Config;
import org.wso2.carbon.dataservices.ui.beans.Data;
import org.wso2.carbon.dataservices.ui.beans.Property;
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
		String name = dynContentEl.getAttributeValue(new QName("name"));
		String description = dynContentEl.getFirstChildWithName(new QName("description")).getText();
		data.setName(name);
		data.setDescription(description);
		Iterator<OMElement> dynamicConfigs = dynContentEl.getChildrenWithName(new QName("config"));
        while (dynamicConfigs.hasNext()) {
        	Config conf = new Config();
        	OMElement dynConf = dynamicConfigs.next();
        	boolean enableOData = Boolean.parseBoolean(dynConf.getAttributeValue(new QName("enableOData")));
        	boolean isPublic = Boolean.parseBoolean(dynConf.getAttributeValue(new QName("isPublic")) );
        	conf.setExposeAsOData(enableOData);
        	conf.setPublicOData(isPublic);
        	Iterator<OMElement> properties = dynConf.getChildrenWithName(new QName("property"));
        	while (properties.hasNext()) {
        		Property prop = new Property();
            	OMElement dynProperty = properties.next();
                String propName = dynProperty.getAttributeValue(new QName("name"));
                Object propValue = null;
                if(propName.equals("dynamicOdataConfig")){
                	ArrayList<String> dynamicTableList = new ArrayList<String>();
                	DynamicODataConfig dynamicODataTableConfiguration = new DynamicODataConfig();
    	            Iterator<OMElement> dynamicODataTablesConfigs = dynProperty.getChildrenWithName(new QName("tblname"));
    	            String maxLimit = dynProperty.getAttributeValue(new QName("maxLimit"));
    	            ODataColumnsConfig columnsConf = new ODataColumnsConfig();
    	            List<ODataColumnsConfig> columnsConfAll = new ArrayList<ODataColumnsConfig>();
    	            dynamicODataTableConfiguration.setMaxLimit(maxLimit);
    	            while (dynamicODataTablesConfigs.hasNext()) {
    	                OMElement dynamicOdataConfig = dynamicODataTablesConfigs.next();
    	                String tblname = dynamicOdataConfig.getAttributeValue(new QName("name"));
    	                String schemaname = dynamicOdataConfig.getAttributeValue(new QName("schema"));
    	                ODataTableSchemaConfig odataTbl = new ODataTableSchemaConfig();
    	                odataTbl.setTableName(tblname);
    	                odataTbl.setSchemaName(schemaname);
    	                Iterator<OMElement> dynamicColConfigs = dynamicOdataConfig.getChildrenWithName(new QName("column"));
    	                columnsConfAll = new ArrayList<ODataColumnsConfig>();
    	                while (dynamicColConfigs.hasNext()) {
    		                OMElement dynamicColConfig = dynamicColConfigs.next();
    		                String type = dynamicColConfig.getAttributeValue(new QName("type"));
    		                String colName = dynamicColConfig.getText();
    		                columnsConf = new ODataColumnsConfig();
    		                columnsConf.setColumnName(colName);
    		                columnsConf.setType(type);
    		                columnsConfAll.add(columnsConf);
    		                odataTbl.setColumns(columnsConfAll);
    		            }
    	                propValue = odataTbl;
    	            }
    	            
                }else{
                	propValue = dynProperty.getText();
                }
                prop.setName(propName);
                prop.setValue(propValue);
                conf.addProperty(prop);
            }
        	data.setConfig(conf);
        }
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
