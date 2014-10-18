/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

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

		/*___________________COPIED FROM MMFBatchAnalyzer.java_________*/

		List<Integer> queueSeen = new ArrayList<Integer>();
		List<String> datasetSeen = new ArrayList<String>();
		double warmCacheConstant = 2.0;

		int num_columns = 0, N = 0;
		double cache_size = memorySize;
		AllocationDistribution Q = new AllocationDistribution();

		//API forward conversion
		int singleTenantQueue = 1;
		if(singleTenant) {
			N = 1;
			singleTenantQueue = queries.get(0).getQueueID(); // just setting to first queueID seen
			queueSeen.add(singleTenantQueue);
		}
		for(SingleDatasetQuery query: queries) {
			if(!singleTenant && !(queueSeen.contains(query.getQueueID()))) {
				N++;
				queueSeen.add(query.getQueueID());
			}
			if(!(datasetSeen.contains(query.getDataset().getName()))) {
				num_columns++;
				datasetSeen.add(query.getDataset().getName());
			}
		}

		//Old variable declarations / allocations		
		double size_columns_powerset = Math.pow(2.0,num_columns);
		Column [] columns = new Column [num_columns];  		
		double [][] utility_table = new double [N][num_columns];	
		double [][] lookup_table = new double [(int)size_columns_powerset][N];
		boolean [][] lookup_table_column_indices = new boolean [(int)size_columns_powerset][num_columns];

		//Construction of internal utility table
		for(SingleDatasetQuery query: queries) {
			(columns[datasetSeen.indexOf(query.getDataset().getName())]) = new Column();
			(columns[datasetSeen.indexOf(query.getDataset().getName())]).setID(datasetSeen.indexOf(query.getDataset().getName()));
			(columns[datasetSeen.indexOf(query.getDataset().getName())]).setSize(query.getDataset().getEstimatedSize());
			int queueIndex = singleTenantQueue;
			if(!singleTenant) {
				queueIndex = query.getQueueID();
			}
			if (warmCache) {
				if (cachedDatasets.contains(query.getDataset())) {
					utility_table[queueSeen.indexOf(queueIndex)][datasetSeen.indexOf(query.getDataset().getName())] += ((warmCacheConstant) * benefits.get(query.getDataset().getName()));
				}
				else {
					utility_table[queueSeen.indexOf(queueIndex)][datasetSeen.indexOf(query.getDataset().getName())] += benefits.get(query.getDataset().getName());
				}
			}
			else {
				utility_table[queueSeen.indexOf(queueIndex)][datasetSeen.indexOf(query.getDataset().getName())] += benefits.get(query.getDataset().getName());
			}		
		}

		/*_______________________NEW CODE FOR PF____________________*/

		// Creation of lookup table
		lookup_table = Combinatorics.powerSetSum(lookup_table_column_indices,
				utility_table, 0, num_columns, N, num_columns);

		// Algorithm 2 Initialization
		int M = N * num_columns * 2;
		int maxIterations = M;
		double[][] precomputed = new double[M][N];
		double[][] weights = new double[M][N];
		Allocation S = new Allocation();
		weights = generateRandomWeights(M,N);
		for (int i = 0; i < M; i++) {
			S = new Allocation();
			(S).Oracle(weights[i], lookup_table, lookup_table_column_indices, columns,
					num_columns, N, (int) size_columns_powerset, cache_size, precomputed[i]);
			S.setCacheProb(1.0 / (double) M);
			Q.addAllocation(S);
		}

		//Algorithm 2 Iteration 
		for (int i = 0; i < maxIterations; i++) {
			double[] y = new double[M];
			y = generateRandomDirection(M);
			Q.newtonsMethodPF(y, precomputed, N);
		}

		Allocation output = new Allocation();
		output = Q.getRandomAllocation();

		/*________________Copied From MMFBatchAnalyzer____________*/

		return getCacheAllocation(queries, datasetSeen, columns, output);

	}

	private List<Dataset> getCacheAllocation(List<SingleDatasetQuery> queries,
			List<String> datasetSeen, Column[] columns, Allocation output) {
		//Backward API conversion
		List<Dataset> cacheThese = new ArrayList<Dataset>();
		for(Column current:columns) {
			if (output.contains(current)) {
				String datasetName = new String();
				datasetName = datasetSeen.get(current.getID());
				int queriesIndex = -1;
				for (SingleDatasetQuery query: queries) {
					if ((query.getDataset().getName()).equals(datasetName)) {
						queriesIndex = queries.indexOf(query);
						break;
					}
				}
				if(queriesIndex >= 0) {
					cacheThese.add(queries.get(queriesIndex).getDataset());
				}
			}
		}

		return cacheThese;
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

	private static double[][] generateRandomWeights(int numWeights, int sizeWeight) {
		double[][] w = new double[numWeights][sizeWeight];
		double normalization = 0.0, temp = 0.0;
		Random generator = new Random();

		for (int i = 0; i < numWeights; i++) {
			for (int j = 0; j < sizeWeight; j++) {
				temp =  generator.nextDouble();
				normalization = normalization + temp;
				w[i][j] = temp;
			}
			for (int j = 0; j < sizeWeight; j++) {
				w[i][j] = w[i][j] / normalization; 
			}
			normalization = 0.0;
		}
		return w;
	}

}



