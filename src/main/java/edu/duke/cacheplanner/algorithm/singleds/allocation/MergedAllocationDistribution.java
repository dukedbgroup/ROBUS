/**
 * 
 */
package edu.duke.cacheplanner.algorithm.singleds.allocation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Used to combine multiple copies of same configurations
 * @author mayuresh
 *
 */
public class MergedAllocationDistribution extends AllocationDistribution {

	public MergedAllocationDistribution(AllocationDistribution dist) {
		super.allocations = mergeAllocations(dist);
	}

	private List<Allocation> mergeAllocations(AllocationDistribution dist) {
		List<Allocation> merged = new ArrayList<Allocation>();
		double sumProb = 0;
		Iterator<Allocation> iterator = dist.allocations.iterator();
		while(iterator.hasNext()) {
			Allocation S = iterator.next();
			System.out.println("*** considering allocation: ");
			S.print();
			if(merged.contains(S)) {
				int index = merged.indexOf(S);
				double prevCacheProb = merged.get(index).getCacheProb();
				merged.get(index).setCacheProb(prevCacheProb + S.getCacheProb());
				System.out.println("*** new probability: " + merged.get(index).getCacheProb());
			} else {
				Allocation newAlloc = new Allocation();
				newAlloc.copy(S);
				newAlloc.setPrecomputed(S.getPrecomputed());
				merged.add(newAlloc);
				System.out.println("*** copied probability: " + merged.get(merged.size()-1).getCacheProb());
			}
			sumProb += S.getCacheProb();
		}
		// normalize probabilities
		for(Allocation S: merged) {
			S.setCacheProb(S.getCacheProb() / sumProb);
		}		
		System.out.println("*** before merging: " + dist.size());
		System.out.println("*** after merging: " + merged.size());
		return merged;
	}
}
