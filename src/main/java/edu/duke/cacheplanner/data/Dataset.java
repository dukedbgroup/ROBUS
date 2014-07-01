/**
 * 
 */
package edu.duke.cacheplanner.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author mayuresh
 *
 */
public class Dataset implements Serializable {

	final static String CACHED_TABLE_SUFFIX = "_cached"; 

	String name;
	String cachedName;
	String path;
	Set<Column> columns;
	
	//may be changed?
	/**
	 * Define a zipfian distribution over the columns of table
	 * 1. Ranks of columns are provided here. Could be Map<Column, Double>
	 * 2. Exponent of zipfian used to evaluate probabilities
	 */
	Set<Double> columnRanks;
	double zipfExponent;

	public Dataset(String name, String path) {
		this.name = name;
		this.cachedName = name + CACHED_TABLE_SUFFIX;
		this.path = path;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the name
	 */
	public String getCachedName() {
		return cachedName;
	}

	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}

	/**
	 * @return the columns
	 */
	public Set<Column> getColumns() {
		return columns;
	}
	
	public List<Column> getColumnList() {
		List<Column> list = new ArrayList<Column>();
		for(Column c: columns) {
			list.add(c);
		}
		return list;
		
	}
	/**
	 * @param columns the columns to set
	 */
	public void setColumns(Set<Column> columns) {
		this.columns = columns;
	}

	/**
	 * @return the columnRanks
	 */
	public Set<Double> getColumnRanks() {
		return columnRanks;
	}

	/**
	 * @param columnRanks the columnRanks to set
	 */
	public void setColumnRanks(Set<Double> columnRanks) {
		this.columnRanks = columnRanks;
	}
	
	/**
	 * @return the zipfExponent
	 */
	public double getZipfExponent() {
		return zipfExponent;
	}

	/**
	 * @param zipfExponent the zipfExponent to set
	 */
	public void setZipfExponent(double zipfExponent) {
		this.zipfExponent = zipfExponent;
	}
}
