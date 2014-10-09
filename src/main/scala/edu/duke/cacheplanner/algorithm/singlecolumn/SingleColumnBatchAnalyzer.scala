/**
 *
 */
package edu.duke.cacheplanner.algorithm.singlecolumn

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.query.SingleDatasetQuery

/**
 * @author mayuresh
 *
 */
trait SingleColumnBatchAnalyzer {

	// column identifier will be <dataset-name><separator><column-name>
	val colIDSeparator = ":"
	val columnMap: Map[String, Column] = createColumnMap()

	// factor to boost benefit of a column that is already present in cache
	var boostFactor: Double = 1.5
	
	def setBoostFactor(factor: Double) = {
	  boostFactor = factor
	}

	/**
	 * Create a map of ColumnID to column object
	 */
	def createColumnMap(): Map[String, Column] = {
			var cMap = scala.collection.mutable.Map[String, Column]()
			for (ds <- Factory.datasets.toList) {
				for(col <- ds.getColumns().toList) {
					cMap(ds.getName()+colIDSeparator+col.getColName()) = col
				}
			}
			cMap.toMap
	}
	
	/**
	 * Given a map of column name to (size, benefit) and a list of cached columns,
	 * boosts up the benefits of cached columns by a factor of @boostFactor
	 */
	def boostBenefitsForCachedColumns(optMap: Map[String, (Double, Double)], 
	    cachedCols: List[Column]): Map[String, (Double, Double)] = {
	  println("original choice of map " + optMap); //TODO: Remove after debugging
	  val boostedMap = optMap map {t => 
	    if(cachedCols.contains(columnMap(t._1))) {t._1 -> (t._2._1, t._2._2 * boostFactor)} 
	    else {t} }
	  println("after accounting for already cached columns " + boostedMap); //TODO: Remove after debugging
	  return boostedMap
	}
	
	/**
	 * abstract method to analyze batch
	 * Parameters:
	 * 1. queries: list of queries in the batch
	 * 2. cachedColumns: list of columns already in cache
	 * 3. memorySize: size of available memory (RDD fraction)
	 * Returns:
	 * List of columns recommended for caching for the batch
	 */
	def analyzeBatch(queries: List[SingleDatasetQuery], 
	    cachedColumns: List[Column], 
	    memorySize: Double): List[Column]

}