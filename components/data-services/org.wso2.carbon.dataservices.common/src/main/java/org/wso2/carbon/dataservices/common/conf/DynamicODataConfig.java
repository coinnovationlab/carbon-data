package org.wso2.carbon.dataservices.common.conf;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;


public class DynamicODataConfig {

		private List<ODataTableSchemaConfig> tables = new ArrayList<>();
		private String maxLimit = "100"; 
		
		public List<ODataTableSchemaConfig> getTables() {
			return tables;
		}

		public void setTables(List<ODataTableSchemaConfig> tables) {
			this.tables = tables;
		}
		
		@XmlAttribute(name = "maxLimit", required = true)
		public String getMaxLimit() {
			return maxLimit;
		}
		
		public void setMaxLimit(String limit) {
			this.maxLimit = limit;
		}
}
