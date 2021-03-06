package edu.duke.cacheplanner.data;

import java.util.HashMap;
import java.util.Map;

public class QueryDistribution {
  private Map<Integer, QueueDistribution> queryDistribution;
  
  public QueryDistribution(Map<Integer, QueueDistribution> map) {
	  queryDistribution = map;
  }
  
  public QueryDistribution() {
	  queryDistribution = new HashMap<Integer, QueueDistribution> ();
  }
  
  public double getDataProb(int queueId, String dataName) {
	  return queryDistribution.get(queueId).getDataProb(dataName);
  }
  
  public double getColProb(int queueId, String dataName, String colName) {
    return queryDistribution.get(queueId).getColumnProb(dataName, colName);
  }
  
  public Map<String, Double> getColDistributionMap(int queueId, String dataName) {
	  return queryDistribution.get(queueId).getQueueDistributionMap().get(dataName).getColumnDistribution();
  }
  
  public Map<String, DatasetDistribution> getQueueDistributionMap(int queueId) {
	  return queryDistribution.get(queueId).getQueueDistributionMap();
  }
  
  public void setQueueDistribution(int id, QueueDistribution queue) {
	  queryDistribution.put(id, queue);
  }
  
  public void setQueryDistribution(Map<Integer, QueueDistribution> map) {
	  queryDistribution = map;
  }
}
