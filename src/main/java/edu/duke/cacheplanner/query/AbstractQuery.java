package edu.duke.cacheplanner.query;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class AbstractQuery implements Serializable {

	protected int queryID;
	protected int queueID;
	protected double timeDelay;
	// weight of the queue
	protected double weight;

	protected String name;

	/**
	 * @return the weight
	 */
	public double getWeight() {
		return weight;
	}

	public int getQueryID() {
		return queryID;
	}

	public int getQueueID() {
		return queueID;
	}

	public double getTimeDelay() {
		return timeDelay;
	}

	public String getName() {
		return name;
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

	public void setQueryID(int id) {
		queryID = id;
	}

	public void setQueueID(int id) {
		queueID = id;
	}

	public void setName(String val) {
		name = val;
	}
	/**
	 * @param cached true if the query is on cached table in Shark, false otherwise
	 * @return HiveQL translation of the query
	 */
	public abstract String toHiveQL(Boolean cached);
}
