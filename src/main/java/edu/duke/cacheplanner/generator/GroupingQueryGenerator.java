package edu.duke.cacheplanner.generator;

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
  
  public GroupingQueryGenerator(double lamb, int id, int colNum) {
	  super(lamb, id, colNum);
  }
	
  public GroupingQueryGenerator(double lamb, ExternalQueue queue, 
		  ListenerManager manager, List<Dataset> data) {
    super(lamb, queue, manager, data);
  }
  
  
  /**
   *   String queryID
   *   String queueID
   *   Dataset dataset 
   *   Column groupingColumn
   *   List<Projection> aggregation, 
   *   List<Selection> selections
   */
	
	
  /**
   * implement query generation for grouping query 
   */
  @Override
  public GroupingQuery generateQuery() {
	Dataset target = getTargetDataset();
	if(target != null) {
		int colNum = pickColumnNumber();
	}
	
	//get the number of columns
	
	//pick the columns from the col distribution
	
	//pick grouping 
    System.out.println("Query Generated: ");
    System.out.println("dataset: " + target.getName());
    
    return null;
  }
  
  public Dataset getTargetDataset() {
	//first look at the dataset distribution
	Random rand = new Random();
	double p = rand.nextDouble();
	double cumulativeProb = 0;
	for(String d : queryDistribution.getQueueDistributionMap(queueId).keySet()) {
	  cumulativeProb = cumulativeProb + queryDistribution.getDataDistribution(queueId, d);
	  if(p <= cumulativeProb) {
		System.out.println("dataset: " + d + " is picked");
	    return getDataset(d);
	  }
	}
	//error
	return null;
  }
  
  public int getColNumber() {
	  double mean = (double)meanColNum;
	  double std = 1.0;
	  
	  Random rand = new Random();
	  
	  mean + 
	  rand.nextGaussian();
	  meanColNum
	  meanColNum
  }
  
  public int pickColumnNumber() {
	  return 0;
  }
  
}


