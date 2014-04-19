package edu.duke.cacheplanner.generator;

import java.util.List;

import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.listener.QueryGenerated;
import edu.duke.cacheplanner.queue.ExternalQueue;

public abstract class AbstractQueryGenerator implements Runnable {
  
  protected double lambda; // the rate of generation, poisson distribution parameter per """second"""
  protected double[] gamma; // probability distribution of queries to queue
  protected double[] delta; //probability distribution over the clusters of columns
  protected int p; //not yet decided => for n/p distinct grouping columns
  protected int zeta; //average number of aggregations per query
  protected List<ExternalQueue> queueList;
  protected boolean stop = false;
  protected ListenerManager listenerManager;
  
  public AbstractQueryGenerator(double lamb) {
    lambda = lamb;
  }
   
  /**
   * generate the query and put it into the one of the ExternalQueue,
   * return the id of the chosen queue
   */
  public abstract int generateQuery();
  
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
}
