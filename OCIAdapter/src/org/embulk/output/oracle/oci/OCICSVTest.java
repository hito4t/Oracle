package org.embulk.output.oracle.oci;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;



public class OCICSVTest {

	public static void main(String[] args) throws Exception {
		OCI oci = new OCI();
		byte[] context = oci.createContext();
		boolean succeeded = false;

		long csv = 0;
		long load = 0;
		long time1 = System.currentTimeMillis();

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

			RowBuffer buffer = new RowBuffer(tableDefinition, 10000);

			File file = new File("C:\\Development\\Java\\Eclipse\\workspace(4.4)\\embulk-performance-test\\example1m.csv");
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				String line;
				while ((line = reader.readLine()) != null) {
					String[] items = line.split(",");
					buffer.addValue(Integer.parseInt(items[0]));
					buffer.addValue(Integer.parseInt(items[1]));
					for (int i = 0; i < 10; i++) {
						buffer.addValue(items[2 + i]);
					}

					if (buffer.isFull()) {
						long time2 = System.currentTimeMillis();
						csv += time2 - time1;

						if (!oci.loadBuffer(context, buffer.getBuffer(), buffer.getRowCount())) {
							return;
						}

						time1 = System.currentTimeMillis();
						load += time1 - time2;

						buffer.clear();
					}
				}
			}
/*
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
*/
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
		}

		System.out.println(String.format("%,d ms (csv: %,d ms, load: %,d ms)", csv + load, csv, load));

	}

}
