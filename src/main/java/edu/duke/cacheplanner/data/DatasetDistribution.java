package edu.duke.cacheplanner.data;

import java.util.Map;

public class DatasetDistribution {
  private double dataProb;
  private Map<String, Double> columnDistribution;
  
  public DatasetDistribution(double prob, Map<String, Double> distribution) {
	  dataProb = prob;
	  columnDistribution = distribution;
  }
  
  public void setDataDistribution(double prob) {
	  dataProb = prob;
  }
  
  public double getDataProb() {
	  return dataProb;
  }
  
  public void setColumnDistribution(Map<String, Double> colDistribution) {
	  columnDistribution = colDistribution;
  }
  
  public Map<String, Double> getColumnDistribution() {
	  return columnDistribution;
  }
}
