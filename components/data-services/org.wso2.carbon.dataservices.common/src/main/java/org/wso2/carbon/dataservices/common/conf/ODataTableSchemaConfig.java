package org.wso2.carbon.dataservices.common.conf;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;


public class ODataTableSchemaConfig {

		private String tblName; 
		private String schemaName; 
		
		@XmlElement(name = "tblname")
		public String getTableName() {
			return tblName;
		}

		public void setTableName(String table) {
			this.tblName = table;
		}
		
		@XmlAttribute(name = "schema", required = true)
		public String getSchemaName() {
			return schemaName;
		}
		
		public void setSchemaName(String schemaName) {
			this.schemaName = schemaName;
		}
}
