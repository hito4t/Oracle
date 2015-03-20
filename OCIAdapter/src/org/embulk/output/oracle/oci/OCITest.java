package org.embulk.output.oracle.oci;



public class OCITest {

	public static void main(String[] args) throws Exception {
		OCI oci = new OCI();
		byte[] context = oci.createContext();
		boolean succeeded = false;

		try {
			if (!oci.open(context, "TESTDB", "TEST_USER", "test_pw")) {
				return;
			}

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
			if (!oci.prepareLoad(context, tableDefinition)) {
				return;
			}

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

			if (!oci.loadBuffer(context, buffer.getBuffer(), buffer.getRowCount())) {
				return;
			}

			if (!oci.commit(context)) {
				return;
			}

			succeeded = true;



		} finally {
			if (!succeeded) {
				byte[] message = oci.getLasetMessage(context);
				String s1 = new String(message, "MS932");
				System.out.println(s1);
			}

			oci.close(context);
			/*
			byte[] b = e.getMessage().getBytes("ISO_8859_1");
			for (byte bb : b) {
				System.out.print(String.format("%02X ", bb));
			}

			String s1 = new String(e.getMessage().getBytes("ISO_8859_1"), "MS932");
			System.out.println(s1);
			String s2 = new String(e.getMessage().getBytes("ISO_8859_1"), "UTF-8");
			System.out.println(s2);
			*/
		}

	}

}
