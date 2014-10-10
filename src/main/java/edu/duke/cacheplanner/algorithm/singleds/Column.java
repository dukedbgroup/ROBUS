package edu.duke.cacheplanner.algorithm.singleds;

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

    Column () {
	size = 0;
    }

    void copy (Column copyThis) {	
	this.ID = copyThis.getID();
	this.size = copyThis.getSize();
    } 

    //// PUBLIC METHODS ////

    int getID () {
	return this.ID;
    }

    double getSize () {
	return this.size;
    }

    void setID (int new_ID) {
	this.ID = new_ID;
    }

    void setSize (double new_size) {
	this.size = new_size;
    }

    void print () {
	System.out.println("ID: ");
	System.out.println(this.ID);
	System.out.println("size: ");
	System.out.println(this.size);
	System.out.println();
    }
    //// PRIVATE METHODS ////


}
