/**
 * 
 */
package edu.duke.cacheplanner.query;

import java.io.Serializable;

import edu.duke.cacheplanner.data.Column;

/**
 * @author mayuresh
 *
 */
@SuppressWarnings("serial")
public class Projection implements Serializable {

	protected AggregationFunction aggregationFunction;
	protected Column col;

	public Projection(AggregationFunction type, Column column) {
		aggregationFunction = type;
		col = column;
	}

	public Column getColName() {
		return col;
	}

	public AggregationFunction getAggregationFunction() {
		return aggregationFunction;
	}
}


