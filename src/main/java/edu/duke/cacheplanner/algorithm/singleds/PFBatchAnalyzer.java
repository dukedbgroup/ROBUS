/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.List;

import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.query.SingleDatasetQuery;

/**
 * Heuristic algorithm to find an allocation that is proportionally fair in 
 * terms of utilities to tenants
 * @author mayuresh
 *
 */
public class PFBatchAnalyzer extends AbstractSingleDSBatchAnalyzer {

	public PFBatchAnalyzer(List<Dataset> datasets) {
		super(datasets);
	}

	/* (non-Javadoc)
	 * @see edu.duke.cacheplanner.algorithm.singleds.SingleDSBatchAnalyzer#analyzeBatch(java.util.List, java.util.List, double)
	 */
	@Override
	public List<Dataset> analyzeBatch(List<SingleDatasetQuery> queries,
			List<Dataset> cachedDatasets, double memorySize) {
		// TODO Auto-generated method stub
		return null;
	}

}
