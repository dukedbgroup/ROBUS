package edu.duke.cacheplanner.query;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class AbstractQuery implements Serializable {

	protected String QueryID;
	protected String QueueID;
	protected double timeDelay;
	// weight of the queue
	protected double weight;

	/**
	 * @return the weight
	 */
	public double getWeight() {
		return weight;
	}

	public String getQueryID() {
		return QueryID;
	}

	public String getQueueID() {
		return QueueID;
	}

	public double getTimeDelay() {
		return timeDelay;
	}

	/**
	 * @param weight the weight to set
	 */
	public void setWeight(double weight) {
		this.weight = weight;
	}

	public void setTimeDelay(double delay) {
		timeDelay = delay;
	}

	public void setQueryID(String id) {
		QueryID = id;
	}

	public void setQueueID(String id) {
		QueueID = id;
	}
	/**
	 * @param cached true if the query is on cached table in Shark, false otherwise
	 * @return HiveQL translation of the query
	 */
	public abstract String toHiveQL(Boolean cached);
}
