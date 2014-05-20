package edu.duke.cacheplanner.data;

public enum ColumnType {
	STRING, 
	INT, 
	FLOAT,
	DOUBLE,
	TIMESTAMP, 
	BOOLEAN;

	@Override
	public String toString() {
		switch(this) {
		case STRING: return "STRING";
		case INT: return "INT";
		case FLOAT: return "FLOAT";
		case DOUBLE: return "DOUBLE";
		case TIMESTAMP: return "TIMESTAMP";
		case BOOLEAN: return "BOOLEAN";

		default: throw new IllegalArgumentException();
		}
	}

}
