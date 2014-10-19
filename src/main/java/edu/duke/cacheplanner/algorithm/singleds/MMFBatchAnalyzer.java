/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.List;

import edu.duke.cacheplanner.algorithm.singleds.allocation.Allocation;
import edu.duke.cacheplanner.algorithm.singleds.allocation.AllocationDistribution;
import edu.duke.cacheplanner.algorithm.singleds.allocation.MergedAllocationDistribution;
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

		initDataStructures(queries, cachedDatasets);

		buildUStars(cacheSize);

		AllocationDistribution Q = new MergedAllocationDistribution(
				generateQ(cacheSize));

		//Randomly select a specific allocation from Q distribution
		Allocation output = new Allocation();
		output = Q.getRandomAllocation();

		return getCacheAllocation(queries, output);

	}

}

