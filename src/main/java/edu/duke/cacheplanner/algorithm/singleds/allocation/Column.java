package edu.duke.cacheplanner.algorithm.singleds.allocation;

/*
  File: Column.java
  Author: Brandon Fain
  Date: 09_30_2014

  Description:
 */


public class Column {

	//// PRIVATE DATA ////

	private int ID;
	private double size;

	//// CONSTRUCTORS ////

	public Column () {
		size = 0;
	}

	public void copy (Column copyThis) {	
		this.ID = copyThis.getID();
		this.size = copyThis.getSize();
	} 

	//// PUBLIC METHODS ////

	public int getID () {
		return this.ID;
	}

	public double getSize () {
		return this.size;
	}

	public void setID (int new_ID) {
		this.ID = new_ID;
	}

	public void setSize (double new_size) {
		this.size = new_size;
	}

	void print () {
		System.out.println("ID: ");
		System.out.println(this.ID);
		System.out.println("size: ");
		System.out.println(this.size);
		System.out.println();
	}

	public boolean equals(Object obj) {
		if(obj == null || !Column.class.equals(obj.getClass())) {
			return false;
		}
		Column other = (Column) obj;
		return this.ID == other.getID() && this.size == other.getSize();
	}

}
