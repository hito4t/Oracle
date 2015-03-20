/*
 * $Id: typical.epf 2627 2010-03-18 01:40:13Z tiba $
 */
package org.embulk.output.oracle.oci;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

public class OCITest {

	public static void main(String[] args) throws Exception {
		ColumnDefinition[] c = new ColumnDefinition[4];
		Class c1 = ColumnDefinition.class;
		Class cc = c.getClass();

		OCI oci = new OCI();
		byte[] context = oci.createContext();
		boolean succeeded = false;

		try {
			if (!oci.open(context, "TESTDB", "TEST_USER", "test_pw")) {
				return;
			}

			TableDefinition tableDefinition = new TableDefinition();
			tableDefinition.tableName = "EXAMPLE";
			if (!oci.prepareLoad(context, tableDefinition)) {
				return;
			}

			succeeded = true;



		} finally {
			if (!succeeded) {
				byte[] message = oci.getLasetMessage(context);
				String s1 = new String(message, "MS932");
				System.out.println(s1);
			}
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
