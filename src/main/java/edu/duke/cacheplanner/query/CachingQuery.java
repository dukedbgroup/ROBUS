/**
 * 
 */
package edu.duke.cacheplanner.query;

import java.util.Set;

import edu.duke.cacheplanner.data.Column;
import edu.duke.cacheplanner.data.Dataset;

/**
 * @author mayuresh
 * Query to cache provided columns of a given dataset to Shark cache
 */
@SuppressWarnings("serial")
public class CachingQuery extends AbstractQuery {

	public CachingQuery(Dataset dataset, Set<Column> columns) {
		// TODO: find what queryID to set
		// TODO: set queueID to combined queue
	}
	
	@Override
	public String toHiveQL(Boolean cached) {
		// TODO Auto-generated method stub
		return null;
	}

}
