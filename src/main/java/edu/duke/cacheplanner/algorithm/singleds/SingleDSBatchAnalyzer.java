/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.List;

import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.query.SingleDatasetQuery;

/**
 * @author mayuresh
 *
 */
public interface SingleDSBatchAnalyzer {
	/**
	 * abstract method to analyze batch
	 * Parameters:
	 * 1. queries: list of queries in the batch
	 * 2. cachedDatasets: list of datasets already in cache
	 * 3. memorySize: size of available memory (RDD fraction)
	 * Returns:
	 * List of datasets recommended for caching for the batch
	 */
	public List<Dataset> analyzeBatch(List<SingleDatasetQuery> queries, 
	    List<Dataset> cachedDatasets, 
	    double cacheSize);
}
