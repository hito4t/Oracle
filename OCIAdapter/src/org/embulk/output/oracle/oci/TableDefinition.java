package org.embulk.output.oracle.oci;

public class TableDefinition {

	public final String tableName;
	public final ColumnDefinition[] columns;


	public TableDefinition(String tableName, ColumnDefinition... columns) {
		this.tableName = tableName;
		this.columns = columns;
	}

}
