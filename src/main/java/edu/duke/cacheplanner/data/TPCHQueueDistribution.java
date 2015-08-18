package edu.duke.cacheplanner.data;

import java.util.Map;
import edu.duke.cacheplanner.query.TPCHQuery;

/**
 * This class contains the information on the dataset distribution for an ExternalQueue
 * 
 * @author Seunghyun
 *
 */
public class TPCHQueueDistribution {
  private Map<TPCHQuery, Double> queueDistribution;

  public TPCHQueueDistribution(Map<TPCHQuery, Double> map) {
	  queueDistribution = map;
  }
  
  public void setQueueDistributionMap(Map<TPCHQuery, Double> map) {
	  queueDistribution = map;
  }
  
  public Map<TPCHQuery, Double> getQueueDistributionMap() {
	  return queueDistribution;
  }
  
  public Double getQueryProb(TPCHQuery query) {
	  return queueDistribution.get(query);
  }
  
}
