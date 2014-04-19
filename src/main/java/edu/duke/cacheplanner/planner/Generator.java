package edu.duke.cacheplanner.planner;

import java.util.List;

import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.listener.QueryGenerated;

public class Generator implements Runnable {
  
  private double lambda; // the rate of generation, poisson distribution parameter per """second"""
  private double[] gamma; // probability distribution of queries to queue
  private double[] delta; //probability distribution over the clusters of columns
  private int p; //not yet decided => for n/p distinct grouping columns
  private int zeta; //average number of aggregations per query
  private List<ExternalQueue> queueList;
  private boolean stop = false;
  private ListenerManager listenerManager;
  
  public Generator(double lamb) {
    lambda = lamb;
  }
   
  /**
   * generate the query and put it into the one of the ExternalQueue,
   * return the id of the chosen queue
   */
  public int generateQuery() {
    return 0;
  }
  
  /**
   * calculate the delayed time using poisson arrival
   */
  public double getPoissonDelay() {
    double mean = 1.0 / (lambda*1000); // convert the number in sec
    return Math.log(Math.random())/-mean;
  }

  /**
   * run thread
   */
  @Override
  public void run() {
    while(true) {
      if(stop = true) {
        return;
      }
    
      int id = generateQuery();
      listenerManager.postEvent((new QueryGenerated(id)));
    
      //get delay
      long delay = (long)getPoissonDelay();
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
      e.printStackTrace();
      }
    }
  }
  
  //test use
  public static void main(String[] args) {
    Generator hi = new Generator(2.5); //2.5sec
    double sum = 0.0;
    for(int i = 0; i < 1000; i++ ) {
    sum += hi.getPoissonDelay();
    }
    System.out.println("average: " + sum/1000 + "ms");
    System.out.println("average: " + sum/1000/1000 + "sec");
  }
}
