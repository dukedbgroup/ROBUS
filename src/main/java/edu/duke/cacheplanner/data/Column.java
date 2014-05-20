package edu.duke.cacheplanner.data;

public class Column {

	String colName;
	
	/**
	 * Store statistics on the column here.
	 */
	double estimatedSize;
	ColumnType columnType;

	public Column(double size, String name, ColumnType type) {
		estimatedSize = size;
		colName = name;
		columnType = type;
	}

	public double getEstimatedSize() {
		return estimatedSize;
	}

	public void setEstimatedSize(double estimatedSize) {
		this.estimatedSize = estimatedSize;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public ColumnType getColumnType() {
		return columnType;
	}
}
