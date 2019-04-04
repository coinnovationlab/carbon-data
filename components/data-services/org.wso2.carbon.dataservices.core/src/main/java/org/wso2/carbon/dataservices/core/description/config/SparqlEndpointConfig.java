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
package org.wso2.carbon.dataservices.core.description.config;

import org.wso2.carbon.dataservices.common.DBConstants;
import org.wso2.carbon.dataservices.common.DBConstants.DataSourceTypes;
import org.wso2.carbon.dataservices.core.DataServiceFault;
import org.wso2.carbon.dataservices.core.engine.DataService;
import org.wso2.carbon.dataservices.core.odata.ODataDataHandler;
import org.wso2.carbon.dataservices.core.odata.ODataServiceFault;

import java.util.Map;

/**
 * This class represents a RDF based data source configuration.
 */
public class SparqlEndpointConfig extends Config {
	
	private String sparqlEndpointUrl;

	public SparqlEndpointConfig(DataService dataService, String configId, Map<String, String> properties,
	                            boolean odataEnable, boolean isPublicOData, String creator) {
		super(dataService, configId, DataSourceTypes.SPARQL, properties, odataEnable, isPublicOData, creator);
		this.sparqlEndpointUrl = this.getProperty(DBConstants.SPARQL.DATASOURCE).trim();
	}
	
	public String getSparqlEndpoint() {
		return this.sparqlEndpointUrl;
	}
	
	@Override
	public boolean isActive() {
		return true;
	}
	
	public void close() {
		/* nothing to close */
	}

	@Override
	public ODataDataHandler createODataHandler() throws ODataServiceFault {
		throw new ODataServiceFault("Expose as OData Service feature doesn't support for the " + getConfigId() +
		                           " Datasource.");
	}

	@Override
	public boolean isResultSetFieldsCaseSensitive() {
		return false;
	}
}