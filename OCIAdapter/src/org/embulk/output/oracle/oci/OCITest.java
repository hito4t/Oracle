package org.embulk.output.oracle.oci;



public class OCITest {

	public static void main(String[] args) throws Exception {
		try (OCIWrapper oci = new OCIWrapper()) {
			oci.open("TESTDB", "TEST_USER", "test_pw");

			TableDefinition tableDefinition = new TableDefinition(
					"EXAMPLE",
					new ColumnDefinition("ID", ColumnDefinition.SQLT_INT, 4),
					new ColumnDefinition("NUM", ColumnDefinition.SQLT_INT, 4),
					new ColumnDefinition("VALUE1", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE2", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE3", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE4", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE5", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE6", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE7", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE8", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE9", ColumnDefinition.SQLT_CHR, 60),
					new ColumnDefinition("VALUE10", ColumnDefinition.SQLT_CHR, 60)
					);
			oci.prepareLoad(tableDefinition);

			RowBuffer buffer = new RowBuffer(tableDefinition, 1);
			buffer.addValue(111);
			buffer.addValue(9999);
			buffer.addValue("AAA");
			buffer.addValue("BBB");
			buffer.addValue("");
			buffer.addValue("4");
			buffer.addValue("5");
			buffer.addValue("6");
			buffer.addValue("7");
			buffer.addValue("8");
			buffer.addValue("9");
			buffer.addValue("10");

			oci.loadBuffer(buffer.getBuffer(), buffer.getRowCount());

			oci.commit();
		}

	}

}
