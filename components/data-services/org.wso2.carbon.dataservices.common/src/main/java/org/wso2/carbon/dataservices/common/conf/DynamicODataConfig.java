package org.wso2.carbon.dataservices.common.conf;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;


public class DynamicODataConfig {

		private List<String> tables = new ArrayList<>();
		
		@XmlElement(name = "tblname")
		public List<String> getTables() {
			return tables;
		}

		public void setTables(List<String> tables) {
			this.tables = tables;
		}
}
