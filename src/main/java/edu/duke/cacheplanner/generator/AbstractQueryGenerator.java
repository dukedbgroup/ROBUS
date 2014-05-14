package edu.duke.cacheplanner.generator;

import java.util.List;

import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.listener.QueryGenerated;
import edu.duke.cacheplanner.query.AbstractQuery;
import edu.duke.cacheplanner.queue.ExternalQueue;

public abstract class AbstractQueryGenerator {
  //Distribution over query arrival rate (per second)
  protected double lambda;
  
  //Distribution over input
  protected double x;
  protected double u;

  //distribution over local window size(optional)
  protected double mu;
  protected double sigma;

  protected ExternalQueue myExternalQueue;
  protected boolean started = false;
  protected ListenerManager listenerManager;
  protected Thread generatorThread;
  
  public AbstractQueryGenerator(double lamb) {
    lambda = lamb;
    generatorThread = new Thread("QueryGenerator") {
      @Override
      public void run() {
        while(true) {
          if(!started) {
            return;
          }
          //get delay
          long delay = (long)getPoissonDelay();
          System.out.println(delay);
          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
          e.printStackTrace();
          }
          //generate the query & post the event to the listener
          AbstractQuery query = generateQuery();
          listenerManager.postEvent(new QueryGenerated
              (Integer.parseInt(query.getQueryID()),Integer.parseInt(query.getQueueID())));

        }
      }     
    };
  }
   
  /**
   * generate the query and put it into the one of the ExternalQueue,
   * return the id of the chosen queue
   */
  public abstract AbstractQuery generateQuery();
  
  /**
   * calculate the delayed time using poisson arrival
   */
  public double getPoissonDelay() {
    double mean = 1.0 / (lambda*1000); // convert the number in sec
    return Math.log(Math.random())/-mean;
  }
  
  public void start() {
    started = true;
    generatorThread.start();
  }
  
  public void stop() throws InterruptedException {
    if (!started) {
      throw new IllegalStateException("cannot be done because a listener has not yet started!");
    }
    started = false;
    generatorThread.join();
  }
 
}
