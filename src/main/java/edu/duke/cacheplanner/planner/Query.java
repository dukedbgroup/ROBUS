package edu.duke.cacheplanner.planner;

import java.io.Serializable;
import java.util.List;

/**
 * Query Object representation
 * This codes are ported from previous query object implemented by mayuresh
 */
public class Query implements Serializable {

  protected Dataset dataset1;                 // necessary
  protected Dataset dataset2;                 // optional
  protected List<Aggregation> projections;    // at least one is necessary
  protected List<Selection> selections;       // optional
  protected List<String> grouping;            // optional
  protected int parallelism;                  // degree of parallelsim

  public Query(Dataset d1, Dataset d2, List<Aggregation> projection, List<Selection> selection, List<String> group, int parallel) {
    dataset1 = d1;
    dataset2 = d2;
    projections = projection;
    selections = selection;
    grouping = group;
    parallelism = parallel;
  }
  
  /**
   * can we put translator inside of Query object? or separate class?
   */
  public String translateToHiveQL() {
    return "";
  }
}


class Dataset implements Serializable {
  protected String myName;
  protected long mySize;

  public Dataset(String name, long size) {
    myName = name;
    mySize = size;
  }

  public String getName() {
    return myName;
  }

  public long getSize() {
    return mySize;
  }
}

class Aggregation implements Serializable {

  protected QueryOperationType operationType;
  protected String colName;
  
  public Aggregation(QueryOperationType type, String name) {
    operationType = type;
    colName = name;
  }
  
  public String getColName() {
      return colName;
  }
  
  public QueryOperationType getOperationType() {
      return operationType;
  }
}

/**
 * An enumeration of the type of operations the CachePlanner currently supports
 */
enum QueryOperationType {
  COUNT, SUM //more
}

class Selection implements Serializable {
    
  protected String columnName;
  protected String value; 
  protected SelectionCondition condition;
  
  public Selection(String name, String val, SelectionCondition cond) {
    columnName = name;
    value = val;
    condition = cond;
  }
  
}

/**
 * An enumeration of the selection conditions
 */
enum SelectionCondition {
  GREATER, LESSER, EQUAL
}
