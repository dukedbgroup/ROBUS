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
public class Projection implements Serializable {

	protected AggregationFunction aggregationFunction;
	protected Column col;

	public Projection(AggregationFunction type, Column column) {
		aggregationFunction = type;
		col = column;
	}

	public Column getColumn() {
		return col;
	}
	
	public String getColName() {
		return col.getColName();
	}

	public AggregationFunction getAggregationFunction() {
		return aggregationFunction;
	}

  public String toString() {
    if(aggregationFunction == AggregationFunction.NONE) {
      return col.getColName();
    }
    else {
      return aggregationFunction.toString() + "(" + col.getColName() + ")";
    }
  }
}


