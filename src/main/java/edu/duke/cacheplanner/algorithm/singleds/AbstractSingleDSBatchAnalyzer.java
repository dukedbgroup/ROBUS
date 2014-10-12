/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.duke.cacheplanner.data.Dataset;


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
	Map<Dataset, Double> benefits;

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
		benefits = new HashMap<Dataset, Double>();
		System.out.println("Benefits computed for each dataset: ");
		for(Dataset ds: allDatasets) {
			benefits.put(ds, ds.getEstimatedSize());
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
}
