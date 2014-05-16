package edu.duke.cacheplanner.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.data.DatasetDistribution;
import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.query.GroupingQuery;
import edu.duke.cacheplanner.query.Projection;
import edu.duke.cacheplanner.query.Selection;
import edu.duke.cacheplanner.queue.ExternalQueue;

public class GroupingQueryGenerator extends AbstractQueryGenerator {
  private int count = 0;

  public GroupingQueryGenerator(double lamb, int id, int colNum) {
    super(lamb, id, colNum);
  }
  
  public GroupingQueryGenerator(double lamb, ExternalQueue queue, 
    ListenerManager manager, List<Dataset> data) {
    super(lamb, queue, manager, data);
  }
  
  /**
   * implement query generation for grouping query 
   */
  @Override
  public GroupingQuery generateQuery() {
    return null;
  }
     
}


