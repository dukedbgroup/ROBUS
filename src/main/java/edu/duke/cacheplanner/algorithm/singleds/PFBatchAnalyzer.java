/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.List;
import java.util.Random;

import edu.duke.cacheplanner.algorithm.singleds.allocation.Allocation;
import edu.duke.cacheplanner.algorithm.singleds.allocation.AllocationDistribution;
import edu.duke.cacheplanner.algorithm.singleds.allocation.MergedAllocationDistribution;
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
			List<Dataset> cachedDatasets, double cacheSize) {

		/*___________________COPIED FROM MMFBatchAnalyzer.java_________*/

		initDataStructures(queries, cachedDatasets);

		buildUStars(cacheSize);

		AllocationDistribution Q = new MergedAllocationDistribution(
				generateQ(cacheSize));

		//Algorithm 2 Iteration 
		int M = Q.size();
		for (int i = 0; i < M; i++) {
			double[] y = new double[M];
			y = generateRandomDirection(M);
			Q.newtonsMethodPF(y, N);
		}

		Allocation output = new Allocation();
		output = Q.getRandomAllocation();

		return getCacheAllocation(queries, output);

	}

	/*______________NEW FUNCTIONS FOR PFBatchAnalyzer______________*/

	private static double[] generateRandomDirection(int size) {
		Random generator = new Random();
		double negativeSum = 0.0, positiveSum = 0.0;
		double[] direction = new double[size];
		for (int i = 0; i < size; i++) {
			if(generator.nextBoolean()) {
				direction[i] = generator.nextDouble();
				positiveSum = positiveSum + direction[i];
			}
			else {
				direction[i] = (-1) * generator.nextDouble();
				negativeSum = negativeSum - direction[i];
			}
		}
		for (int i = 0; i < size; i++) {
			if (direction[i] > 0.0) {
				direction[i] = direction[i] / positiveSum;
			}
			else {
				direction[i] = direction[i] / negativeSum;
			}

		}
		return direction;
	}

}



