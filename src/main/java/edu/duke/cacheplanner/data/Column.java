package edu.duke.cacheplanner.data;

public class Column {

	String colName;
	/**
	 * Store statistics on the column here.
	 */
	double estimatedSize;

	public Column(double size, String name) {
		estimatedSize = size;
		colName = name;
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
}
