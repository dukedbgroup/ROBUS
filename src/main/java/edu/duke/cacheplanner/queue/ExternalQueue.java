package edu.duke.cacheplanner.queue;

import java.util.LinkedList;
import java.util.Queue;

import edu.duke.cacheplanner.listener.ListenerManager;
import edu.duke.cacheplanner.listener.QueryFetchedByCachePlanner;
import edu.duke.cacheplanner.query.AbstractQuery;

/**
 * External Queue class
 *  
 */
public class ExternalQueue {
	private int queueID;
	private int weight;
	private int minShare;
	private int batchSize; 
	private ListenerManager listenerManager;
	private Queue<AbstractQuery> queue;

	public ExternalQueue(int id, int w, int min, int size) {
		queueID = id;
		setWeight(w);
		setMinShare(min);
		batchSize = size;
		queue = new LinkedList<AbstractQuery>();
	}

	public ExternalQueue(int id, int w, int min, int size, ListenerManager manager) {
		this(id, w, min, size);
		listenerManager = manager;
	}

	/**
	 * request a queue to fetch a batch
	 */
	public synchronized AbstractQuery[] fetchABatch() { 
		AbstractQuery[] queries = new AbstractQuery[batchSize];
		for(int i = 0; i < batchSize; i++) {
			if(queue.peek() != null) {
				queries[i] = queue.poll();  
				//notify an event to the listeners
				listenerManager.postEvent(new QueryFetchedByCachePlanner
						(Integer.parseInt(queries[i].getQueryID()), queueID));
			}
		} 
		return queries;
	}

	/**
	 * add query to the queue
	 */
	public synchronized void addQuery(AbstractQuery query) {
		queue.add(query);
	}

	public void setListenerManager(ListenerManager manager) {
		listenerManager = manager;
	}  

	public int getId() {
		return queueID;
	}

	/**
	 * @return the weight
	 */
	public int getWeight() {
		return weight;
	}

	/**
	 * @param weight the weight to set
	 */
	public void setWeight(int weight) {
		this.weight = weight;
	}

	/**
	 * @return the minShare
	 */
	public int getMinShare() {
		return minShare;
	}

	/**
	 * @param minShare the minShare to set
	 */
	public void setMinShare(int minShare) {
		this.minShare = minShare;
	}
}
