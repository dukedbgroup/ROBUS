/**
 * 
 */
package edu.duke.cacheplanner.query;

import java.io.Serializable;

import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.ColumnType;

/**
 * @author mayuresh
 *
 */

@SuppressWarnings("serial")
public class Selection implements Serializable {

	protected Column col;
	protected String value; 
	protected SelectionOperator operator;

	public Selection(Column column, String val, SelectionOperator cond) {
		col = column;
		value = val;
		operator = cond;
	}

	/**
	 * @return the col
	 */
	public Column getCol() {
		return col;
	}

	/**
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * @return the condition
	 */
	public SelectionOperator getOperator() {
		return operator;
	}

  public String toString() {
    ColumnType type = col.getColumnType();
    if(type == ColumnType.DOUBLE || type == ColumnType.FLOAT || type == ColumnType.INT) {
      return col.getColName() + " " + operator.toString() + " " + value;
    }
    else {
      return col.getColName() + " " + operator.toString() + " \'" + value + "\'";
    }
  }
}

