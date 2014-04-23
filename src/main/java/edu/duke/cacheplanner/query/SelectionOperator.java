/**
 * 
 */
package edu.duke.cacheplanner.query;

/**
 * @author mayuresh
 *
 */
public enum SelectionOperator {
	GREATER, 
	LESSER, 
	EQUAL;

	@Override
	public String toString() {
		switch(this) {
		case GREATER: return ">";
		case LESSER: return "<";
		case EQUAL: return "=";
		default: throw new IllegalArgumentException();
		}
	}
}
