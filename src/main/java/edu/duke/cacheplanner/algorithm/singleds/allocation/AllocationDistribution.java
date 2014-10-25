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
		int maxIterations = N*10;
		double r = 0.0, convergeConstant = 0.001;

		for (int t = 0; t < maxIterations; t++) {
			double rprime = r - (functionDerivativePF(y, N, r) / functionSecondDerivativePF(y, N, r));
			if (Math.abs(rprime - r) < convergeConstant) {
				r = rprime;
				break;
			}
			r = rprime;
		}
		for (int s = 0; s < allocations.size(); s++) {
			(allocations.get(s)).setCacheProb((r * y[s]) + (allocations.get(s)).getCacheProb());
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
			if (Math.abs(sumDenominatorQ) <= 0.0001) {
				if (sumDenominatorQ > 0) {
					sumDenominatorQ = sumDenominatorQ + 0.0001;
				}
				else {
					sumDenominatorQ = sumDenominatorQ - 0.0001;
				}
			}
			sumI = sumI + (sumNumeratorQ / sumDenominatorQ);
			sumDenominatorQ = 0.0;
			sumNumeratorQ = 0.0;
		}
		if (Math.abs(sumI) <= 0.0001) {
			if (sumI > 0) {
				sumI = sumI + 0.0001;
			}
			else {
				sumI = sumI + 0.0001;
			}
		}
		return sumI;

	}

	private double functionDerivativePF(double[] y, int n,
			double r) {

		double sumY = 0.0, result = 0.0;
		double sumI = 0.0, sumNumeratorQ = 0.0, sumDenominatorQ = 0.0;
		for (int s = 0; s < allocations.size(); s++) {
			sumY = sumY + y[s];
		}

		for (int i = 0; i < n; i++) {
			for (int s = 0; s < allocations.size(); s++) {
				Allocation current = allocations.get(s);
				sumDenominatorQ = sumDenominatorQ + ((current.getCacheProb() + (r * y[s])) * current.getPrecomputed()[i]);
				sumNumeratorQ = sumNumeratorQ + (y[s] * current.getPrecomputed()[i]);
			}
			if (Math.abs(sumDenominatorQ) <= 0.0001) {
				if (sumDenominatorQ > 0) {
					sumDenominatorQ = sumDenominatorQ + 0.0001;
				}
				else {
					sumDenominatorQ = sumDenominatorQ - 0.0001;
				}
			}
			sumI = sumI + (sumNumeratorQ / sumDenominatorQ);
			result = sumI - (n * sumY); 
			sumDenominatorQ = 0.0;
			sumNumeratorQ = 0.0;
		}
		return result;
	}

	public double[] generateGradientDirection(int N) {
		double[] x_new = new double[allocations.size()];
		double L = N;
		for (int i = 0; i < allocations.size(); i++) {
			if(Math.abs((allocations.get(i)).getCacheProb()) > 0.0001) {
				x_new[i] = (1.0 / (allocations.get(i)).getCacheProb()); 
			}
			else {
				if ((allocations.get(i)).getCacheProb() > 0.0) {
					x_new[i] = (1.0 / (0.0001 + (allocations.get(i)).getCacheProb()));
				}
				else {
					x_new[i] = (1.0 / (-0.0001 + (allocations.get(i)).getCacheProb()));
				}
				
			}
		}
		for (int i = 0; i < allocations.size(); i++) {
			x_new[i] = x_new[i] - L;
		}
		return x_new;
	}

	public void normalize() {
		double normalization = 0.0;
		for (int i = 0; i < allocations.size(); i++) {
			normalization = normalization + (allocations.get(i)).getCacheProb();
		}
		for (int i = 0; i < allocations.size(); i++) {
			(allocations.get(i)).setCacheProb((allocations.get(i)).getCacheProb() / normalization);
		}
	}

	public void projectNonNegative() {
		for (int s = 0; s < allocations.size(); s++) {
			if ((allocations.get(s)).getCacheProb() < 0.0) {
				(allocations.get(s)).setCacheProb(0.0);
			}
		}
	}

	public boolean convergedFrom(AllocationDistribution Q) {
		boolean flag = true;
		int index = -1;
		double convergeConstant = 0.001;
		for (int s = 0; s < allocations.size(); s++) {
			index = Q.getAllocations().indexOf(allocations.get(s));
			if (index >= 0) {
				if (Math.abs((allocations.get(s)).getCacheProb() - (Q.item(index)).getCacheProb()) > convergeConstant) {
					flag = false;
				}
			}
			else {
				flag = false;
			}
		}
		return flag;
	}

}
