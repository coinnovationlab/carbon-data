/*
 *  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.carbon.dataservices.core.admin;

import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.core.multitenancy.utils.TenantAxisUtils;
import org.wso2.carbon.dataservices.core.internal.DataServicesDSComponent;
import org.wso2.carbon.service.mgt.ServiceAdmin;
import org.wso2.carbon.utils.ConfigurationContextService;

/**
 * Data Services admin service class, for the basic functions.
 */
public class ServiceAdminDS extends ServiceAdmin {

	public ServiceAdminDS() throws Exception {
		super();
	}
		
	@Override
	public AxisConfiguration getAxisConfig () {
		String tenantDomain = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantDomain();
		ConfigurationContextService contextService = DataServicesDSComponent.getContextService();
        ConfigurationContext configContext = contextService.getServerConfigContext();
        AxisConfiguration axis = configContext.getAxisConfiguration();
		if(!tenantDomain.equals("carbon.super")) {
	        ConfigurationContext tenantConfigContx = TenantAxisUtils.getTenantConfigurationContext(tenantDomain, configContext);           
	        axis = tenantConfigContx.getAxisConfiguration();
		}
		return axis;
	}
	
	@Override
	public ConfigurationContext getConfigContext() {
		String tenantDomain = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantDomain();
		ConfigurationContextService contextService = DataServicesDSComponent.getContextService();
        ConfigurationContext configContext = contextService.getServerConfigContext();
		if(!tenantDomain.equals("carbon.super")) {
	        configContext = TenantAxisUtils.getTenantConfigurationContext(tenantDomain, configContext);
		}
		return configContext;
	}
}
