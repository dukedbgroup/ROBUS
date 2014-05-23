package edu.duke.cacheplanner.query;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class AbstractQuery implements Serializable {

	protected String QueryID;
	protected String QueueID;
	protected double timeDelay;

	public String getQueryID() {
		return QueryID;
	}

	public String getQueueID() {
		return QueueID;
	}

	public double getTimeDelay() {
		return timeDelay;
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
