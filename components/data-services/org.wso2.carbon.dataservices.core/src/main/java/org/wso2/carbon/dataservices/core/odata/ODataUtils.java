/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.dataservices.core.odata;

import org.apache.commons.codec.binary.Base64;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.core.Encoder;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.wso2.carbon.dataservices.common.DBConstants;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Utility class for OData.
 */
public class ODataUtils {
	
	/**
	* This method generates an unique ETag for each data row entry.
	*
	* @param tableName Name of the table
	* @param entry     Data row entry
	* @return E Tag
	*/
	public static String generateETag(String configID, String tableName, ODataEntry entry) {
		StringBuilder uniqueString = new StringBuilder();
		uniqueString.append(configID).append(tableName);
		for (String columnName : entry.getNames()) {
			uniqueString.append(columnName).append(entry.getValue(columnName));
		}
		return UUID.nameUUIDFromBytes((uniqueString.toString()).getBytes()).toString();
	}
	
	/**
	* Taken from version 4.5.0-SNAPSHOT of org.apache.olingo.server.core.responses.EntityResponse,
	* available at: https://github.com/apache/olingo-odata4/blob/master/lib/server-core-ext/src/main/java/org/apache/olingo/server/core/responses/EntityResponse.java
	*
	* The static method EntityResponse.buildLocation imported from groupId org.wso2.orbit.org.apache.olingo and artifactId odata-server with version 4.3.0wso2v1
	* is bugged, and as of 2018/07/24, even more recent versions of such dependency do not fix this bug, so I simply copied the method from the aforementioned Git repository.
	*
	* @param baseURL
	* @param entity
	* @param entitySetName
	* @param type
	* @return
	* @throws EdmPrimitiveTypeException
	*/
	public static String buildLocation(String baseURL, Entity entity, String entitySetName, EdmEntityType type) 
			throws EdmPrimitiveTypeException {
		StringBuilder location = new StringBuilder();
		
		location.append(baseURL).append("/").append(entitySetName);
		
		int i = 0;
		boolean usename = type.getKeyPredicateNames().size() > 1;
		location.append("(");
		for (String key : type.getKeyPredicateNames()) {
			if (i > 0) {
				location.append(",");
			}
			i++;
			if (usename) {
				location.append(key).append("=");
			}
			
			EdmProperty property = (EdmProperty)type.getProperty(key);
			String propertyType = entity.getProperty(key).getType();
			Object propertyValue = entity.getProperty(key).getValue();
			
			if (propertyValue == null) {
				throw new EdmPrimitiveTypeException("The key value for property "+key+" is invalid; Key value cannot be null");
			}
			
			if(propertyType.startsWith("Edm.")) {
				propertyType = propertyType.substring(4);
			}
			EdmPrimitiveTypeKind kind = EdmPrimitiveTypeKind.valueOf(propertyType);
			String value =  EdmPrimitiveTypeFactory.getInstance(kind).valueToString(
				propertyValue, true, property.getMaxLength(), property.getPrecision(), property.getScale(), true);
			if (kind == EdmPrimitiveTypeKind.String) {
				value = EdmString.getInstance().toUriLiteral(Encoder.encode(value));
			}
			location.append(value);
		}
		location.append(")");
		return location.toString();
	}
}