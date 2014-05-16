package edu.duke.cacheplanner.data;

import java.util.Map;

/**
 * This class contains the information on the dataset distribution for an ExternalQueue
 * 
 * @author shlee0605
 *
 */
public class QueueDistribution {
  private Map<String, DatasetDistribution> queueDistribution;
  
  public QueueDistribution(Map<String, DatasetDistribution> map) {
	  queueDistribution = map;
  }
  
  public void setQueueDistribution(Map<String, DatasetDistribution> map) {
	  queueDistribution = map;
  }
  
  public Map<String, DatasetDistribution> getQueueDistribution() {
	  return queueDistribution;
  }
  
  public Double getDataProb(String name) {
	  return queueDistribution.get(name).getDataProb();
  }
  
  public Double getColumnProb(String name, String col) {
	  return queueDistribution.get(name).getColumnDistribution().get(col);
  }
}
