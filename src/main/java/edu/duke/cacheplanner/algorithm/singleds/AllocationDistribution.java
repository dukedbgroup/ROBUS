/*
  File: AllocationDistribution.java
  Author: Brandon Fain
  Date: 09_30_2014

  Description:
 */
package edu.duke.cacheplanner.algorithm.singleds;

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

	Allocation getRandomAllocation() {
		Random generator = new Random();
		double[][] ranges = new double[allocations.size()][2];
		ranges[0][0] = 0.0;
		ranges[0][1] = (allocations.get(0)).getCacheProb();
		for (int i = 1; i < allocations.size(); i++) {
			ranges[i][0] = ranges[i-1][1];
			ranges[i][1] = ranges[i][0] + (allocations.get(i)).getCacheProb();
		}
		double randomNum = generator.nextDouble();
		int indexOfReturn = -1;
		for (int i = 0; i < allocations.size(); i++) {
			if ((randomNum >= ranges[i][0]) && (randomNum <= ranges[i][1])) {
				indexOfReturn = i;
			}
		}
		return ((this.allocations).get(indexOfReturn));
	}
    
	//Utility[from this allocation][to this user]
	public void newtonsMethodPF(double[] y, double[][] utility, int N) {
		int iterations = 5;
		double r = 0.0, xSum = 0.0;

		for (int t = 0; t < iterations; t++) {
			r = r - (functionDerivativePF(y, utility, N, r) / functionSecondDerivativePF(y, utility, N, r));
		}
		for (int s = 0; s < allocations.size(); s++) {
			(allocations.get(s)).setCacheProb((r * y[s]) + (allocations.get(s)).getCacheProb());
			if ((allocations.get(s)).getCacheProb() < 0.0) {
				(allocations.get(s)).setCacheProb(0.0);
			}
			xSum = xSum + (allocations.get(s)).getCacheProb();
		}
		for (int s = 0; s < allocations.size(); s++) {
			(allocations.get(s)).setCacheProb((allocations.get(s)).getCacheProb() / xSum);
		}
	}

	private double functionSecondDerivativePF(double[] y, double[][] utility,
			int n, double r) {
		
		double sumI = 0.0, sumNumeratorQ = 0.0, sumDenominatorQ = 0.0;
		
		for (int i = 0; i < n; i++) {
			for (int s = 0; s < allocations.size(); s++) {
				sumDenominatorQ = sumDenominatorQ + (((allocations.get(s)).getCacheProb() + (r * y[s])) * utility[s][i]);
				sumNumeratorQ = sumNumeratorQ + (y[s] * utility[s][i]);
			}
			sumNumeratorQ = (-1) * Math.pow(sumNumeratorQ, 2.0);
			sumDenominatorQ = Math.pow(sumDenominatorQ, 2.0);
			if (Math.abs(sumDenominatorQ) <= 0.001) {
				sumDenominatorQ = sumDenominatorQ + 0.01;
			}
			sumI = sumI + (sumNumeratorQ / sumDenominatorQ);
			sumDenominatorQ = 0.0;
			sumNumeratorQ = 0.0;
		}
		if (Math.abs(sumI) <= 0.001) {
			sumI = sumI + 0.01;
		}
		return sumI;
		
	}

	private double functionDerivativePF(double[] y, double[][] utility, int n,
			double r) {

		double sumQ = 0.0;
		double sumI = 0.0, sumNumeratorQ = 0.0, sumDenominatorQ = 0.0;
		for (int s = 0; s < allocations.size(); s++) {
			sumQ = sumQ + (r * y[s]);
		}
		
		for (int i = 0; i < n; i++) {
			for (int s = 0; s < allocations.size(); s++) {
				sumDenominatorQ = sumDenominatorQ + (((allocations.get(s)).getCacheProb() + (r * y[s])) * utility[s][i]);
				sumNumeratorQ = sumNumeratorQ + (y[s] * utility[s][i]);
			}
			if (Math.abs(sumDenominatorQ) <= 0.001) {
				sumDenominatorQ = sumDenominatorQ + 0.01;
			}
			sumI = sumI + (sumNumeratorQ / sumDenominatorQ);
			sumDenominatorQ = 0.0;
			sumNumeratorQ = 0.0;
		}
		return sumI;
	}


}
