package org.wso2.carbon.dataservices.core.script;

public class GeneratedListTables {

	private String [] tablesOfSchemas;
	private String schemaName;
	
	public GeneratedListTables() {
	}
	
	public void setTables(String [] tablesOfSchemas) {
		this.tablesOfSchemas = tablesOfSchemas;
	}
	
	public  String [] getTables(){
		return this.tablesOfSchemas;
	}
	
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	
	public  String getSchemaName(){
		return this.schemaName;
	}
}
