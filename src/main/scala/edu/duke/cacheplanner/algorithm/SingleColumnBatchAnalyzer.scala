/**
 *
 */
package edu.duke.cacheplanner.algorithm

import scala.collection.mutable.MutableList
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.AbstractQuery

import edu.duke.cacheplanner.query.SingleTableQuery

/**
 * @author mayuresh
 *
 */
object SingleColumnBatchAnalyzer {

	//  val env:GRBEnv = new GRBEnv()
	//  val model:GRBModel = new GRBModel(env)
	// column identifier will be <dataset-name><separator><column-name>
	val colIDSeparator = ":"
	val columnMap: Map[String, Column] = createColumnMap()

	// factor to boost benefit of a column that is already present in cache
	val boostFactor = 1.5

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
	 * Build a mapping of column to its size and benefit.
	 * Benefit for column C in proportional fairness formulation is defined as:
	 * \sum_{i in queries accessing C} W_i log (size_C * indicator_C)
	 */
	def buildMapForKnapsack(queries:java.util.List[SingleTableQuery]): 
		Map[String, (Double, Double)] = {
			var knapMap = scala.collection.mutable.Map[String, (Double, Double)]()
			for (query <- queries.toList) {
				val columns = query.getRelevantColumns() 
				// assuming there is only one relevant column
				if(columns == null || columns.size() == 1) {
					//ignore the query
				} else {
					val column = columns.iterator().next()
					println("name: " + column.getColName())
					val colID = query.getDataset().getName() + colIDSeparator + column.getColName()
					val size = column.getEstimatedSize()
					val weight = query.getWeight()
					val benefit = weight * scala.math.log(size)
					println(size, weight, benefit)
					val oldValue = knapMap.getOrElse(colID, null)
					println(oldValue)
					knapMap(colID) = oldValue match {
						case null => (size, benefit)
						case _ => (size, oldValue._2 + benefit)
					}
					println("map: " +knapMap(colID))
				}      
			}
			knapMap.toMap
	}
	
	def boostBenefitsForCachedColumns(knapMap: Map[String, (Double, Double)], 
	    cachedCols: List[Column]): Map[String, (Double, Double)] = {
	  println("original choice of map " + knapMap); //TODO: Remove after debugging
	  val boostedMap = knapMap map {t => 
	    if(cachedCols.contains(columnMap(t._1))) {t._1 -> (t._2._1, t._2._2 * boostFactor)} 
	    else {t} }
	  println("after accounting for already cached columns " + boostedMap); //TODO: Remove after debugging
	  return boostedMap
	}

	def analyzeGreedily(
			queries:java.util.List[SingleTableQuery], 
			cachedColumns: List[Column], 
			memorySize:Double) : List[Column] = {
			// create a map of [columnID -> (size, benefit)]
			// boost benefits for already cached columns
			val knapMap = boostBenefitsForCachedColumns(
			    buildMapForKnapsack(queries), cachedColumns)
			// sort columns in decreasing order of benefit/size
			val sortedMap = knapMap.toSeq.sortBy(t => (t._2._2/t._2._1)).reverse
			// start picking columns until size filled
			var budget = memorySize
			var selectedCols = new MutableList[Column]
			for(column <- sortedMap) {
				if(column._2._1 < budget) {
					selectedCols += (columnMap.get(column._1) match {case Some(c:Column) => c})
							budget = budget - column._2._1
				}
			}
			// return
			selectedCols.toList
	}

}