/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import edu.duke.cacheplanner.algorithm.singleds.allocation.Allocation;
import edu.duke.cacheplanner.algorithm.singleds.allocation.AllocationDistribution;
import edu.duke.cacheplanner.algorithm.singleds.allocation.Column;
import edu.duke.cacheplanner.algorithm.singleds.allocation.MergedAllocationDistribution;
import edu.duke.cacheplanner.data.Dataset;
import edu.duke.cacheplanner.query.SingleDatasetQuery;


/**
 * @author mayuresh
 *
 */
public abstract class AbstractSingleDSBatchAnalyzer 
implements SingleDSBatchAnalyzer {

	/**
	 *  set of all datasets
	 */
	List<Dataset> allDatasets;

	/**
	 * Map of benefit per dataset
	 */
	Map<String, Double> benefits;

	/**
	 * When set, the batch is analyzed as if belonging to a single user 
	 * discarding queue a query belongs to.
	 * In other words, when set, we will get performance optimal solution
	 * with no fairness guarantees.
	 */
	boolean singleTenant = false;

	/**
	 * When set, preference would be given to datasets already in cache
	 */
	boolean warmCache = false;

	public AbstractSingleDSBatchAnalyzer(List<Dataset> datasets) {
		if(datasets == null) {
			throw new IllegalArgumentException("datasets can't be null");
		}
		allDatasets = datasets;
		buildBenefitMap();
	}

	/**
	 * Build a map giving benefit of caching each dataset
	 */
	private void buildBenefitMap() {
		benefits = new HashMap<String, Double>();
		System.out.println("Benefits computed for each dataset: ");
		for(Dataset ds: allDatasets) {
			benefits.put(ds.getName(), ds.getEstimatedSize());
			System.out.println(" " + ds.getName() + " -> " + ds.getEstimatedSize());
		}
	}

	/**
	 * @return the singleTenant
	 */
	public boolean isSingleTenant() {
		return singleTenant;
	}

	/**
	 * @param singleTenant the singleTenant to set
	 */
	public void setSingleTenant(boolean singleTenant) {
		this.singleTenant = singleTenant;
	}

	/**
	 * @return the warmCache
	 */
	public boolean isWarmCache() {
		return warmCache;
	}

	/**
	 * @param warmCache the warmCache to set
	 */
	public void setWarmCache(boolean warmCache) {
		this.warmCache = warmCache;
	}

	int num_columns = 0, N = 0;
	List<String> datasetSeen = new ArrayList<String>();
	final double warmCacheConstant = 2.0;
	//Old variable declarations / allocations		
	Column [] columns = new Column[0];
	double [][] utility_table = new double[0][0];	
	double [][] lookup_table = new double[0][0];
	boolean [][] lookup_table_column_indices = new boolean[0][0];
	double [] u_star = new double [N];		

	/**
	 * Initializes data structures including columns, utility_table, and lookup_table.
	 * @param queries
	 * @param cachedDatasets
	 */
	protected void initDataStructures(List<SingleDatasetQuery> queries,
			List<Dataset> cachedDatasets) {
		num_columns = 0;
		N = 0;
		datasetSeen = new ArrayList<String>();
		List<Integer> queueSeen = new ArrayList<Integer>();

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

		columns = new Column[num_columns];
		utility_table = new double [N][num_columns];

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
		double size_columns_powerset = Math.pow(2.0,num_columns);
		lookup_table = new double [(int)size_columns_powerset][N];
		lookup_table_column_indices = new boolean [(int)size_columns_powerset][num_columns];
		lookup_table = Combinatorics.powerSetSum(lookup_table_column_indices, utility_table, 0, num_columns, N, num_columns);

	}

	/**
	 * Builds optimal utility vector by setting weights to unit vectors
	 * @param cache_size
	 */
	protected void buildUStars(double cache_size) {
		double [] w = new double [N];
		u_star = new double[N];
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
			S.Oracle(w, lookup_table, lookup_table_column_indices, columns, 
					num_columns, N, lookup_table.length, cache_size);
			double utility = 0.0;
			for (int l = 0; l < num_columns; l++) {
				if (S.contains(columns[l])) {
					utility = utility + utility_table[i][l];
				}
			}
			u_star[i] = utility;
		}
	}

	// generate weights on a binary hypercube
	private double[][] generateHypercubeWeights(int sizeWeight) {
		double[][] w = new double[(1 << sizeWeight)-1][sizeWeight];
		double sumWeights = 0;
		for(int i = 0; i < w.length; i++) {
			sumWeights = 0;
			for(int j = 0; j < sizeWeight; j++) {
				if((1 << j & (i+1)) != 0) {	//skipping 0
					w[i][j] = 1.0 / u_star[j];
					sumWeights += w[i][j];
				}
			}
			for(int j = 0; j < sizeWeight; j++) {
				w[i][j] /= sumWeights;
			}
		}
		return w;
	}

	private double[][] generateRandomWeights(int numWeights, int sizeWeight) {
		double[][] w = new double[numWeights][sizeWeight];
		double normalization = 0.0, temp = 0.0;
		Random generator = new Random();

		for (int i = 0; i < numWeights; i++) {
			for (int j = 0; j < sizeWeight; j++) {
				temp =  (generator.nextDouble() / u_star[j]);
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

	private void addHypercubeAllocations(double cacheSize, 
			AllocationDistribution Q) {
		double[][] weights = generateHypercubeWeights(N);
		Allocation S = new Allocation();
		for(int i=0; i<weights.length; i++) {
			S = new Allocation();
			S.Oracle(weights[i], lookup_table, lookup_table_column_indices, columns,
					num_columns, N, lookup_table.length, cacheSize);
			S.setCacheProb(1.0 / weights.length);
			Q.addAllocation(S);
		}
	}

	private void addRandomAllocations(double cacheSize,
			AllocationDistribution Q) {
		int M = 1;
		if(N > 1) {
			M = (int) (4 * N * N * Math.log(N) / Math.log(2.0));	// TODO: Tune this setting, need a large number
		}
		double[][] weights = new double[M][N];
		Allocation S = new Allocation();
		weights = generateRandomWeights(M,N);
		for (int i = 0; i < M; i++) {
			S = new Allocation();
			S.Oracle(weights[i], lookup_table, lookup_table_column_indices, columns,
					num_columns, N, lookup_table.length, cacheSize);
			S.setCacheProb(1.0 / (double) M);
			Q.addAllocation(S);
		}
	}

	private void addSimpleMMFAllocations(double cacheSize,
			AllocationDistribution Q) {
		//Algorithm 1 Initialization
		double [] w = new double [N];
		double epsilon = 0.1;
		double T = 1.0;
		if(N > 1) {
			T = (4.0) * (N * N) * Math.log(N) / Math.log(2.0) / (epsilon * epsilon);
		}
		for (int i = 0; i < N; i++) {
			w[i] = (1.0 / (double) N);
		}

		//Algorithm 1 Iteration
		for (int k = 0; k < T; k++) {
			Allocation S = new Allocation();
			S.Oracle(w, lookup_table, lookup_table_column_indices, columns, 
					num_columns, N, lookup_table.length, cacheSize);
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
	}

	/**
	 * Creates a list of configurations Q by including both the configurations 
	 * given by SimpleMMF algorithm as well as the configurations produced for 
	 * random vectors
	 * @param cacheSize
	 * @return
	 */
	protected AllocationDistribution generateQ(double cacheSize) {
		AllocationDistribution Q = new AllocationDistribution();
		addSimpleMMFAllocations(cacheSize, Q);
//		addHypercubeAllocations(cacheSize, Q);
		addRandomAllocations(cacheSize, Q);
		return new MergedAllocationDistribution(Q);
	}

	/**
	 * 
	 * @param queries
	 * @param output
	 * @return datasets to cache, picked from the allocation output
	 */
	protected List<Dataset> getCacheAllocation(List<SingleDatasetQuery> queries,
			Allocation output) {
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
