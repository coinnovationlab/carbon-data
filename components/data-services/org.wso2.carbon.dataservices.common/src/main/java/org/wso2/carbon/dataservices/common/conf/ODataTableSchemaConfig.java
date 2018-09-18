package org.wso2.carbon.dataservices.common.conf;

import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;


public class ODataTableSchemaConfig {

		private String tblName; 
		private String schemaName; 
		private List<ODataColumnsConfig> columnsList;
		
		@XmlAttribute(name = "name", required = true)
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
		
		public List<ODataColumnsConfig> getColumns(){
			return columnsList;
		}
		
		public void setColumns(List<ODataColumnsConfig> columnsList) {
			this.columnsList = columnsList;
		}
}
