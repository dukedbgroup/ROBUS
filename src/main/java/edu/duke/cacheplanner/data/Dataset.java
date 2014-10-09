/**
 * 
 */
package edu.duke.cacheplanner.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author mayuresh
 *
 */
@SuppressWarnings("serial")
public class Dataset implements Serializable {

	final static String CACHED_TABLE_SUFFIX = "_cached"; 

	String name;
	String cachedName;
	String path;
	Set<Column> columns;
	/**
	 * cache size of dataset.
	 */
	double estimatedSize;
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
	 * @deprecated
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

	public double getEstimatedSize() {
		return estimatedSize;
	}

	public void setEstimatedSize(double estimatedSize) {
		this.estimatedSize = estimatedSize;
	}

	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		}
		if(!Dataset.class.equals(obj.getClass())) {
			return false;
		}
		Dataset other = (Dataset)obj;
		if(this.name.equals(other.name) && 
				this.estimatedSize == other.estimatedSize && 
				this.path.equals(other.path)) {
			return true;
		}
		return false;
	}
}
