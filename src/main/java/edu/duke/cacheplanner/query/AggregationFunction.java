/**
 * 
 */
package edu.duke.cacheplanner.query;

/**
 * @author mayuresh
 *
 */
public enum AggregationFunction {
	COUNT, 
	SUM, 
	MIN, 
	MAX, 
	NONE; // to be treated as a simple projection without any aggregation clause
	
	@Override
	public String toString() {
		switch(this) {
		case COUNT: return "COUNT";
		case SUM: return "SUM";
		case MIN: return "MIN";
		case MAX: return "MAX";
		case NONE: return "";
		default: throw new IllegalArgumentException();
		}
	}

}
