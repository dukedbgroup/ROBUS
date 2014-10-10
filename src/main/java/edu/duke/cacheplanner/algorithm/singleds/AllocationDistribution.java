package edu.duke.cacheplanner.algorithm.singleds;

/*
  File: AllocationDistribution.java
  Author: Brandon Fain
  Date: 09_30_2014

  Description:
 */

import java.util.ArrayList;
import java.util.Random;

public class AllocationDistribution {
    
    //// PRIVATE DATA ////
    
    private ArrayList<Allocation> allocations;

    //// CONSTRUCTORS ////

    AllocationDistribution () {
	allocations = new ArrayList<Allocation>();
    }

    void copy (AllocationDistribution copyThis) {
	(this.allocations).clear();
	for (int i = 0; i < (copyThis.allocations).size(); i++) {
	    (this.allocations).add(copyThis.item(i));
	}
    } 

    //// PUBLIC METHODS ////

    Allocation item (int index) {
	Allocation return_item = new Allocation();
	return_item.copy((this.allocations).get(index));
	return (return_item);
    }

    void addAllocation (Allocation S) {
	(this.allocations).add(S);
    }

    void print () {
	for (int i = 0; i < allocations.size(); i++) {
	    ((this.allocations).get(i)).print();
	}
    }

    Allocation getRandomAllocation () {
	  Random generator = new Random();
	  int randomIndex = generator.nextInt(allocations.size());
	  return ((this.allocations).get(randomIndex));
    }
    
    //// PRIVATE METHODS ////


}
