package edu.duke.cacheplanner.queue;

import java.util.LinkedList;
import java.util.Queue;

import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.listener.QueryFetchedByCachePlanner;
import edu.duke.cacheplanner.query.GroupingQuery;

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
  private Queue<GroupingQuery> queue;

  public ExternalQueue(int id, int w, int min, int size, ListenerManager manager) {
	this(id, w, min, size);
	listenerManager = manager;
  }
  
  public ExternalQueue(int id, int w, int min, int size) {
	    queueID = id;
	    weight = w;
	    minShare = min;
	    batchSize = size;
	    queue = new LinkedList<GroupingQuery>();
  }
  
  /**
   * request a queue to fetch a batch
   */
  public GroupingQuery[] fetchABatch() { 
    GroupingQuery[] queries = new GroupingQuery[batchSize];
    for(int i = 0; i < batchSize; i++) {
      queries[i] = (GroupingQuery) queue.poll();  
    }
    //notify an event to the listeners
    listenerManager.postEvent(new QueryFetchedByCachePlanner(queueID));
    return queries;
  }
  
  /**
   * add query to the queue
   */
  public void addQuery(GroupingQuery query) {
    queue.add(query);
  }
  
  public void setListenerManager(ListenerManager manager) {
	  listenerManager = manager;
  }
  
}
