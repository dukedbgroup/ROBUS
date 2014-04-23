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

}

