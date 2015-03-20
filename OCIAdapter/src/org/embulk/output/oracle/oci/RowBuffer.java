/*
 * $Id: typical.epf 2627 2010-03-18 01:40:13Z xxx $
 */
package org.embulk.output.oracle.oci;

public class RowBuffer {

	private final TableDefinition table;
	private final int rowCount;
	private final byte[] buffer;
	private int currentRow = 0;
	private int currentColumn = 0;
	private int currentPosition = 0;


	public RowBuffer(TableDefinition table, int rowCount) {
		this.table = table;
		this.rowCount = rowCount;

		int rowSize = 0;
		for (ColumnDefinition column : table.columns) {
			rowSize += column.columnSize;
		}

		buffer = new byte[rowSize * rowCount];
	}

	public void addValue(int value) {
		if (currentRow >= rowCount) {
			throw new IllegalStateException();
		}

		buffer[currentPosition] = (byte)value;
		buffer[currentPosition + 1] = (byte)(value >> 8);
		buffer[currentPosition + 2] = (byte)(value >> 16);
		buffer[currentPosition + 3] = (byte)(value >> 24);

		next();
	}

	public void addValue(String value) {
		if (currentRow >= rowCount) {
			throw new IllegalStateException();
		}

		byte[] bytes = value.getBytes();
		System.arraycopy(bytes, 0, buffer, currentPosition, bytes.length);
		if (bytes.length < table.columns[currentColumn].columnSize) {
			buffer[currentPosition + bytes.length] = 0;
		}

		next();
	}

	private void next() {
		currentPosition += table.columns[currentColumn].columnSize;
		currentColumn++;
		if (currentColumn == table.columns.length) {
			currentColumn = 0;
			currentRow++;
		}
	}

	public byte[] getBuffer() {
		return buffer;
	}

	public int getRowCount() {
		return currentRow;
	}

}
