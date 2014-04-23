package edu.duke.cacheplanner.query;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class AbstractQuery implements Serializable {

	protected String QueryID;
	protected String QueueID;

	public String getQueryID() {
		return QueryID;
	}

	public String getQueueID() {
		return QueueID;
	}

	/**
	 * @param cached true if the query is on cached table in Shark, false otherwise
	 * @return HiveQL translation of the query
	 */
	public abstract String toHiveQL(Boolean cached);
}
