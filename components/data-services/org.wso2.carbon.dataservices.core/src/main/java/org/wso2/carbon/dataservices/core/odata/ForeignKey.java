package org.wso2.carbon.dataservices.core.odata;

public class ForeignKey {
	public static enum FKType {IMPORTED, EXPORTED}
	
	private FKType type;
	
	private String database; // database of the table containing this FK
	private String table; // table containing this FK
	private String name; // name of the FK
	
	private String foreignDatabase; // database of the table that the FK links to
	private String foreignTable; // table that the FK links to
	private String foreignName; // name of the column referenced by the FK
	
	public ForeignKey(FKType type, String database, String table, String name, String foreignDatabase, String foreignTable, String foreignName) {
		this.type = type;
		this.database = database;
		this.table = table;
		this.name = name;
		this.foreignDatabase = foreignDatabase;
		this.foreignTable = foreignTable;
		this.foreignName = foreignName;
	}
	
	public FKType getType() {
		return type;
	}
	
	public String getDatabase() {
		return database;
	}
	
	public String getTable() {
		return table;
	}
	
	public String getName() {
		return name;
	}
	
	public String getFullName() {
		return table + "." + name;
	}
	
	public String getForeignDatabase() {
		return foreignDatabase;
	}
	
	public String getForeignTable() {
		return foreignTable;
	}
	
	public String getForeignName() {
		return foreignName;
	}
	
	public String getFullForeignName() {
		return foreignTable + "." + foreignName;
	}
	
	@Override
	public String toString() {
		String str = "";
		switch(type) {
			case IMPORTED:
				str += type + ": " + getFullName() + " -> " + getFullForeignName();
				break;
			case EXPORTED:
				str += type + ": " + getFullForeignName() + " -> " + getFullName();
				break;
			default:
		}
		return str;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof ForeignKey))
			return false;
		ForeignKey fk = (ForeignKey) o;
		if (fk.type == this.type && fk.database == this.database && fk.table == this.table && fk.name == this.name &&
				fk.foreignDatabase == this.foreignDatabase && fk.foreignTable == this.foreignTable && fk.foreignName == this.foreignName)
			return true;
		return false;
	}
}
