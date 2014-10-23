/*
  File: AllocationDistribution.java
  Author: Brandon Fain
  Date: 09_30_2014

  Description:
 */
package edu.duke.cacheplanner.algorithm.singleds.allocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AllocationDistribution {

	//// PRIVATE DATA ////

	protected List<Allocation> allocations;

	//// CONSTRUCTORS ////

	public AllocationDistribution () {
		allocations = new ArrayList<Allocation>();
	}

	public void copy (AllocationDistribution copyThis) {
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

	public void addAllocation (Allocation S) {
		(this.allocations).add(S);
	}

	public void print () {
		for (int i = 0; i < allocations.size(); i++) {
			((this.allocations).get(i)).print();
		}
	}

	public int size() {
		return allocations.size();
	}

	public List<Allocation> getAllocations() {
		return allocations;
	}

	public Allocation getRandomAllocation() {
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
	public void newtonsMethodPF(double[] y, int N) {
		int iterations = 5;
		double r = 0.0, xSum = 0.0;

		for (int t = 0; t < iterations; t++) {
			r = r - (functionDerivativePF(y, N, r) / functionSecondDerivativePF(y, N, r));
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

	private double functionSecondDerivativePF(double[] y, int n, double r) {

		double sumI = 0.0, sumNumeratorQ = 0.0, sumDenominatorQ = 0.0;

		for (int i = 0; i < n; i++) {
			for (int s = 0; s < allocations.size(); s++) {
				Allocation current = allocations.get(s);
				sumDenominatorQ = sumDenominatorQ + ((current.getCacheProb() + (r * y[s])) * current.getPrecomputed()[i]);
				sumNumeratorQ = sumNumeratorQ + (y[s] * current.getPrecomputed()[i]);
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

	private double functionDerivativePF(double[] y, int n,
			double r) {

		double sumQ = 0.0;
		double sumI = 0.0, sumNumeratorQ = 0.0, sumDenominatorQ = 0.0;
		for (int s = 0; s < allocations.size(); s++) {
			sumQ = sumQ + (r * y[s]);
		}

		for (int i = 0; i < n; i++) {
			for (int s = 0; s < allocations.size(); s++) {
				Allocation current = allocations.get(s);
				sumDenominatorQ = sumDenominatorQ + ((current.getCacheProb() + (r * y[s])) * current.getPrecomputed()[i]);
				sumNumeratorQ = sumNumeratorQ + (y[s] * current.getPrecomputed()[i]);
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

	public double[] generateGradientDirection() {
		double[] x_new = new double[allocations.size()];
		double normalization = 0.0, L = 0.0;
		for (int i = 0; i < allocations.size(); i++) {
			if(Math.abs((allocations.get(i)).getCacheProb()) > 0.001) {
				x_new[i] = (1.0 / (allocations.get(i)).getCacheProb()); 
				normalization = normalization + x_new[i];
			}
			else {
				x_new[i] = 1.0;
				normalization = normalization + x_new[i];
			}
		}
		L = (normalization / allocations.size());
		for (int i = 0; i < allocations.size(); i++) {
			x_new[i] = x_new[i] - L;
		}
		return x_new;
	}


}
