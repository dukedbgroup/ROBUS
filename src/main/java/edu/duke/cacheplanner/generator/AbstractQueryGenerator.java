package edu.duke.cacheplanner.generator;

import java.util.List;
import java.util.Random;

import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.listener.QueryGenerated;
import edu.duke.cacheplanner.query.AbstractQuery;
import edu.duke.cacheplanner.query.AggregationFunction;
import edu.duke.cacheplanner.query.Projection;
import edu.duke.cacheplanner.query.Selection;
import edu.duke.cacheplanner.queue.ExternalQueue;
import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.data.QueryDistribution;

public abstract class AbstractQueryGenerator {
  //Distribution over query arrival rate (per second)
  protected double lambda;
  protected int queueId;
  protected int meanColNum;
  
  
  protected List<Dataset> datasets;
  protected QueryDistribution queryDistribution;
  protected ExternalQueue externalQueue;
  protected ListenerManager listenerManager;

  protected Thread generatorThread;
  protected boolean started = false;
  
  public AbstractQueryGenerator(double lamb, int id, int colNum) {
	  lambda = lamb;
	  queueId = id;
	  meanColNum = colNum;
	  generatorThread = createThread();
  }
  
  public AbstractQueryGenerator(double lamb, ExternalQueue queue, 
		  ListenerManager manager, List<Dataset> data) {
    lambda = lamb;
    externalQueue = queue;
	listenerManager = manager;
    datasets = data;
    externalQueue.setListenerManager(listenerManager);
    generatorThread = createThread();
  }
  
  public Thread createThread() {
	  return new Thread("QueryGenerator") {
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
	          externalQueue.addQuery(query);
	          listenerManager.postEvent(new QueryGenerated
	              (Integer.parseInt(query.getQueryID()),Integer.parseInt(query.getQueueID())));
	        }
	      }     
	    };
  }
    
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
  
  public void setListenerManager(ListenerManager manager) {
	  listenerManager = manager;
  }
  
  public void setDatasets(List<Dataset> data) {
	  datasets = data;
  }
  
  public void setExternalQueue(ExternalQueue queue) {
	  externalQueue = queue;
	  externalQueue.setListenerManager(listenerManager);	  
  }
  
  public void setQueryDistribution(QueryDistribution distribution) {
    queryDistribution = distribution;
  }

  public int getQueueId() {
	  return queueId;
  }
  
  public Dataset getDataset(String name) {
	  for(Dataset d: datasets) {
		  if(d.getName().equals(name)) {
			  return d;
		  }
	  }
	  return null;
  }
  
  public Dataset getRandomDataset() {
    //first look at the dataset distribution
    Random rand = new Random();
    double p = rand.nextDouble();
    double cumulativeProb = 0;
    for(String d : queryDistribution.getQueueDistributionMap(queueId).keySet()) {
      cumulativeProb = cumulativeProb + queryDistribution.getDataProb(queueId, d);
      if(p <= cumulativeProb) {
        System.out.println("dataset: " + d + " is picked");
        return getDataset(d);
      }
    }
    
    //error
    return null;
  }
  
  public int getRandomColNumber() {
    double mean = (double)meanColNum;
    double std = 1.0;
    Random rand = new Random();
    return 1;
  }
  
  public Projection getRandomProjection(Dataset data) {
	return new Projection(AggregationFunction.SUM, getRandomColumn(data));
  }
  
  public Column getRandomColumn(Dataset data) {
	    Random rand = new Random();
	    double p = rand.nextDouble();
	    double cumulativeProb = 0;
	    String name = data.getName();
	    System.out.println(name);
	    for(String col : queryDistribution.getColDistributionMap(queueId, name).keySet()) {
	      cumulativeProb = cumulativeProb + queryDistribution.getColProb(queueId, name, col);
	      if(p <= cumulativeProb) {
	        System.out.println("column: " + col + " is picked");
	        return getColumn(data, col);
	      }
	    }
    	System.out.println("column null!! ERROR");

		return null;
  }
  
  public Column getColumn(Dataset data, String colName) {
	  for(Column c : data.getColumns()) {
		  if (colName.equals(c.getColName())) {
			  return c;
		  }
	  }
	  return null;
  }
  
  public Selection getRandomSelection(Dataset data) {
	  return null;
  }

  /**
   * generate the query and put it into the one of the ExternalQueue,
   * return the id of the chosen queue
   */
  public abstract AbstractQuery generateQuery();
 
}
