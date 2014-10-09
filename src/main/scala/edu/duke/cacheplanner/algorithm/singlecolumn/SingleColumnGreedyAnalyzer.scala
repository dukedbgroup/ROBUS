/**
 *
 */
package edu.duke.cacheplanner.algorithm.singlecolumn

import edu.duke.cacheplanner.query.SingleDatasetQuery
import edu.duke.cacheplanner.data.Column
import scala.collection.mutable.MutableList

/**
 * @author mkunjir
 *
 */
object SingleColumnGreedyAnalyzer extends SingleColumnBatchAnalyzer {
  
	/**
	 * Build a mapping of column to its size and benefit.
	 * Benefit for column C in proportional fairness formulation is defined as:
	 * \sum_{i in queries accessing C} W_i log (size_C * indicator_C)
	 */
	def columnToSizeAndBenefit(queries:List[SingleDatasetQuery]): 
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
					println("map: " + knapMap(colID))
					
				}      
			}
			knapMap.toMap
	}

  override def analyzeBatch(
			queries: List[SingleDatasetQuery], 
			cachedColumns: List[Column], 
			memorySize: Double): List[Column] = {
			// create a map of [columnID -> (size, benefit)]
			// boost benefits for already cached columns
			val knapMap = boostBenefitsForCachedColumns(
			    columnToSizeAndBenefit(queries), cachedColumns)
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