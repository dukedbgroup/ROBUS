package edu.duke.cacheplanner.generator;

import edu.duke.cacheplanner.query.AbstractQuery;

public class GroupingQueryGenerator extends AbstractQueryGenerator {
  public GroupingQueryGenerator(double lamb) {
    super(lamb);
  }
  
  /**
   * implement query generation for grouping query 
   */
  @Override
  public AbstractQuery generateQuery() {
    System.out.println("Query Generated");
    return null;
  }
}
