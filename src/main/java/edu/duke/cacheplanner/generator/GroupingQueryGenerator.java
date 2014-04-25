package edu.duke.cacheplanner.generator;

public class GroupingQueryGenerator extends AbstractQueryGenerator {

	public GroupingQueryGenerator(double lamb) {
		super(lamb);
	}
	
	/**
	 * implement query generation for grouping query & return the id of the queue
	 */
	@Override
	public int generateQuery() {
		System.out.println("Query Generated");
		return 0;
	}
}
