package edu.duke.cacheplanner.algorithm.singleds.allocation;
/*
  File: Allocation.java
  Author: Brandon Fain
  Date: 09_30_2014

  Description:
 */

import java.util.ArrayList;
import java.util.List;


public class Allocation {

	// // PRIVATE DATA ////

	private List<Column> columns;
	private double cacheProb;
	
	/**
	 * this is probably not the place to store these indices, but Newton's 
	 * method in {@code AllocationDistribution} requires this.
	 */
	private double[] precomputed;

	// // CONSTRUCTORS ////

	public Allocation() {
		columns = new ArrayList<Column>();
		cacheProb = 0.0;
	}

	public void copy(Allocation copyThis) {
		(this.columns).clear();
		for (int i = 0; i < (copyThis.columns).size(); i++) {
			(this.columns).add(copyThis.item(i));
		}
		this.cacheProb = copyThis.getCacheProb();
	}

	// // PUBLIC METHODS ////

	public void setCacheProb(double newCacheProb) {
		this.cacheProb = newCacheProb;
	}

	public void setPrecomputed(double[] precomupted) {
		this.precomputed = precomupted;
	}

	public boolean contains(Column checkThis) {
		for (int i = 0; i < columns.size(); i++) {
			if (checkThis.getID() == ((this.columns).get(i)).getID()) {
				return true;
			}
		}
		return false;
	}

	Column item(int index) {
		Column return_item = new Column();
		return_item.copy((this.columns).get(index));
		return (return_item);
	}

	public List<Column> getColumns() {
		return columns;
	}

	public double getCacheProb() {
		return this.cacheProb;
	}

	public double[] getPrecomputed() {
		return precomputed;
	}

	public void Oracle(double[] w, double[][] lookup_table, boolean[][] table_indices,
			Column[] columns, int num_columns, int num_users, int table_size,
			double max_size) {

		double size = 0.0;
		double best_weighted_sum = 0.0;
		int best_allocation_index = 0;
		double[] weighted_sums = new double[table_size];
		double[] unweighted_sums = new double[table_size];
		for (int i = 0; i < table_size; i++) {
			for (int l = 0; l < num_users; l++) {
				weighted_sums[i] = weighted_sums[i]
						+ (w[l] * lookup_table[i][l]);
				unweighted_sums[i] = unweighted_sums[i] + lookup_table[i][l];
			}
			if (weighted_sums[i] > best_weighted_sum) {
				for (int j = 0; j < num_columns; j++) {
					if (table_indices[i][j]) {
						System.out.println("**debug: (" + i + "," + j + ") " + table_indices[i][j] + "; " + columns[j]);
						columns[j].print();
						size = size + (columns[j]).getSize();
					}
				}
				if (size <= max_size) {
					best_weighted_sum = weighted_sums[i];
					best_allocation_index = i;
				}
				size = 0.0;
			}
		}

		precomputed = new double[num_users];
		for (int j = 0; j < num_users; j++) {
			precomputed[j] = lookup_table[best_allocation_index][j];
		}
		for (int i = 0; i < num_columns; i++) {
			if (table_indices[best_allocation_index][i]) {
				(this.columns).add(columns[i]);
			}
		}
	}

	void print() {
		for (int i = 0; i < columns.size(); i++) {
			(this.columns.get(i)).print();
		}
		System.out.println("Probability: " + cacheProb);
	}

	void addColumn(Column new_column) {
		(this.columns).add(new_column);
	}

	public boolean equals(Object obj) {
		if(obj == null || !Allocation.class.equals(obj.getClass())) {
			return false;
		}
		Allocation other = (Allocation) obj;
		return this.columns.equals(other.getColumns());
	}
}
