package edu.duke.cacheplanner.generator;

import java.util.List;

import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.listener.QueryGenerated;
import edu.duke.cacheplanner.listener.QuerySerialize;
import edu.duke.cacheplanner.query.AbstractQuery;
import edu.duke.cacheplanner.queue.ExternalQueue;
import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.data.QueryDistribution;

public abstract class AbstractQueryGenerator {
  //Distribution over query arrival rate (per second)
  protected double lambda;
  protected int queueId;
  protected double waitingTime;
  protected double groupingQueryProb;

  protected static double meanColNum;
  protected static double stdColNum;

  protected List<Dataset> datasets;
  protected QueryDistribution queryDistribution;
  protected ExternalQueue externalQueue;
  protected ListenerManager listenerManager;
  protected Thread generatorThread;
  protected boolean started = false;

  public AbstractQueryGenerator(double lamb, int id, double mean, double std, double grouping) {
    lambda = lamb;
    queueId = id;
    meanColNum = mean;
    stdColNum = std;
    groupingQueryProb = grouping;
    waitingTime = 0.0;
    generatorThread = createThread();
  }

  public Thread createThread() {
    return new Thread("QueryGenerator") {
      @Override
      public void run() {
        while (true) {
          if (!started) {
            return;
          }
          //get delay
          long delay = (long) getPoissonDelay();
          waitingTime = delay;

          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          //generate the query & post the event to the listener
          AbstractQuery query = generateQuery();
          query.setTimeDelay(waitingTime);
          externalQueue.addQuery(query);
          listenerManager.postEvent(new QuerySerialize(query));
          listenerManager.postEvent(new QueryGenerated
                  (Integer.parseInt(query.getQueryID()), Integer.parseInt(query.getQueueID())));
        }
      }
    };
  }

  /**
   * calculate the delayed time using poisson arrival
   */
  public double getPoissonDelay() {
    double mean = 1.0 / (lambda * 1000); // convert the number in sec
    return Math.log(Math.random()) / -mean;
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
    for (Dataset d : datasets) {
      if (d.getName().equals(name)) {
        return d;
      }
    }
    return null;
  }
  
  public Column getColumn(Dataset data, String colName) {
    for (Column c : data.getColumns()) {
      if (colName.equals(c.getColName())) {
        return c;
      }
    }
    return null;
  }

  /**
   * generate the query and put it into the one of the ExternalQueue,
   * return the id of the chosen queue
   */
  public abstract AbstractQuery generateQuery();

}
