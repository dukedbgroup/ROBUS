/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.query.SingleDatasetQuery;

/**
 * Heuristic algorithm to find an allocation that is max-min fair in terms of 
 * utilities to tenants
 * @author mayuresh
 *
 */
public class MMFBatchAnalyzer extends AbstractSingleDSBatchAnalyzer {

	public MMFBatchAnalyzer(List<Dataset> datasets) {
		super(datasets);
	}

	/* (non-Javadoc)
	 * @see edu.duke.cacheplanner.algorithm.singleds.SingleDSBatchAnalyzer#analyzeBatch(java.util.List, java.util.List, double)
	 */
	@Override
	public List<Dataset> analyzeBatch(List<SingleDatasetQuery> queries,
			List<Dataset> cachedDatasets, double cacheSize) {
		
		// Following code is just for demonstration
		Dataset goingToCacheThis = cachedDatasets.get(0);	//just for example
		int luckyTenant = 0;	//just for example
		
		// feasibility check
		double size = goingToCacheThis.getEstimatedSize();
		if(size > cacheSize) {
			return null;
		}

		// count utility of luckytenant
		double utility = 0;
		for(SingleDatasetQuery query: queries) {
			if(query.getDataset().equals(goingToCacheThis)) {
				// in single tenant mode, no need of checking owner of query
				if(singleTenant || (!singleTenant && query.getQueueID() == luckyTenant)) {
					utility += benefits.get(goingToCacheThis);
				}
			}
		}
		
		// boost utility if already cached
		if(warmCache && cachedDatasets.contains(goingToCacheThis)) {
			utility *= 2;
		}

		System.out.println("Total utility to tenant 0: " + utility);

		// return set to cache
		return new ArrayList<Dataset>(Arrays.asList(new Dataset[]{goingToCacheThis}));
	}

}
