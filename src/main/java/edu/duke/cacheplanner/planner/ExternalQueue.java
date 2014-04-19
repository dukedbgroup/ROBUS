package edu.duke.cacheplanner.planner;

import java.util.LinkedList;
import java.util.Queue;

import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.listener.QueryFetchedByCachePlanner;

/**
 * External Queue class
 *  
 */
public class ExternalQueue {
  private int queueID;
  private int weight;
  private int minShare;
  private int batchSize; // do we need here? or planner requests the batch size whatever he wants?
  private ListenerManager listenerManager;
  private Queue<Query> queue;

  public ExternalQueue(int id, int w, int min, int size, ListenerManager manager) {
    queueID = id;
    weight = w;
    minShare = min;
    listenerManager = manager;
    batchSize = size;
    queue = new LinkedList<Query>();
  }
  
  /**
   * request a queue to fetch a batch
   */
  public Query[] fetchABatch() { 
    Query[] queries = new Query[batchSize];
    for(int i = 0; i < batchSize; i++) {
      queries[i] = (Query) queue.poll();  
    }
    //notify an event to the listeners
    listenerManager.postEvent(new QueryFetchedByCachePlanner(queueID));
    return queries;
  }
  
  /**
   * add query to the queue
   */
  public void addQuery(Query query) {
    queue.add(query);
  }
  
}
