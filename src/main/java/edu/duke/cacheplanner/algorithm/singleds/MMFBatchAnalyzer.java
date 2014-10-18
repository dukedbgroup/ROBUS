/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.ArrayList;
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

		/*________________________NEW CODE____________________________*/ 

		List<Integer> queueSeen = new ArrayList<Integer>();
		List<String> datasetSeen = new ArrayList<String>();
		double warmCacheConstant = 2.0;

		int num_columns = 0, N = 0;
		double cache_size = cacheSize;
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
		double [] w = new double [N];
		double [] u_star = new double [N];		
		Column [] columns = new Column [num_columns];  		
		double [][] utility_table = new double [N][num_columns];	
		double [][] lookup_table = new double [(int)size_columns_powerset][N];
		boolean [][] lookup_table_column_indices = new boolean [(int)size_columns_powerset][num_columns];
		double[] throwAway = new double[N];
		
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

		//Creation of lookup table
		lookup_table = Combinatorics.powerSetSum(lookup_table_column_indices, utility_table, 0, num_columns, N, num_columns);

		//Calculating each users optimal utility
		for (int i = 0; i < N; i++) {
			for (int l = 0; l < N; l++) {
				if (l==i) {
					w[l] = 1.0;
				}
				else {
					w[l] = 0.0;
				}
			}
			Allocation S = new Allocation();
			S.Oracle(w, lookup_table, lookup_table_column_indices, columns, num_columns, N, (int)size_columns_powerset, cache_size, throwAway);
			// case of single tenant
			if(N==1) {
				return getCacheAllocation(queries, datasetSeen, columns, S);
			}
			double utility = 0.0;
			for (int l = 0; l < num_columns; l++) {
				if (S.contains(columns[l])) {
					utility = utility + utility_table[i][l];
				}
			}
			u_star[i] = utility;
		}


		//Algorithm 1 Initialization
		double epsilon = 0.1;
		double T = (4.0) * (N) * ((Math.log(N/(epsilon * epsilon))) / (Math.log(2.0)));
		for (int i = 0; i < N; i++) {
			w[i] = (1.0 / (double) N);
		}

		//Algorithm 1 Iteration
		for (int k = 0; k < T; k++) {
			Allocation S = new Allocation();
			S.Oracle(w, lookup_table, lookup_table_column_indices, columns, num_columns, N, (int)size_columns_powerset, cache_size, throwAway);
			S.setCacheProb(1.0/T);
			double w_sum = 0.0, utility = 0.0;
			for (int i = 0; i < N; i++) {
				for (int l = 0; l < num_columns; l++) {
					if (S.contains(columns[l])) {
						utility = utility + utility_table[i][l];
					}
				}
				w[i] = w[i] *  Math.exp( ((-1.0)*(epsilon)*(utility)) / (u_star[i]) );
				w_sum = w_sum + w[i];
				utility = 0.0;
			}
			for (int i = 0; i < N; i++) {
				w[i] = (w[i] / w_sum);
			}
			Q.addAllocation(S);
		}

		//Randomly select a specific allocation from Q distribution
		Allocation output = new Allocation();
		output = Q.getRandomAllocation();

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
}

class Combinatorics {

	public static double [][] powerSetSum(boolean [][] column_indices, double [][] utility_table, int start, int num_columns, int N, int total_num_columns) {
		double size_pwr_set = Math.pow(2.0,num_columns);
		double [][] sums_set = new double [(int) size_pwr_set][N];
		double [] sum = new double [N];
		for (int i = 0; i < N; i++) {
			sum[i] = utility_table[i][start]; 
			sums_set[0][i] = sum[i];
		}
		column_indices[0][start] = true;
		if (num_columns == 1) {   
			return sums_set;
		}
		else {
			for (int i = 0; i < N; i++) {
				sums_set[0][i] = 0;
			}
			column_indices[0][start] = false;
		}

		double [][] half_sums_set = new double [(int)(size_pwr_set / 2.0)][N];
		boolean [][] half_indices = new boolean [(int)(size_pwr_set / 2.0)][total_num_columns];
		half_sums_set = powerSetSum (half_indices, utility_table, (start+1), (num_columns-1), N, total_num_columns);
		for (int i = 0; i < (size_pwr_set / 2); i++) {
			for (int l = 0; l < N; l++) {
				sums_set[i][l] = half_sums_set[i][l];
			}
			for (int l = 0; l < total_num_columns; l++) {
				column_indices[i][l] = half_indices[i][l]; 
			}
		}
		for (int i = (int)(size_pwr_set / 2); i < (int)size_pwr_set; i++) {
			for (int l = 0; l < N; l++) {
				sums_set[i][l] = half_sums_set[i-((int)size_pwr_set / 2)][l] + sum[l];
			}
			for (int l = 0; l < total_num_columns; l++) {
				column_indices[i][l] = half_indices[i-((int)size_pwr_set / 2)][l];
				column_indices[i][start] = true;
			}
		}
		return sums_set;
	}
}



/*______________________END NEW CODE__________________________*/

/*
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

}*/
