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
				if(columns == null || columns.size() != 1) {
					//ignore the query
				} else {
					val column = columns.iterator().next()
					val colID = query.getDataset().getName() + colIDSeparator + column.getColName()
					val size = column.getEstimatedSize()
					val weight = query.getWeight()
					val benefit = weight * scala.math.log(size)

					val oldValue = knapMap(colID)
					knapMap(colID) = oldValue match {
					case null => (size, benefit)
					case _ => (size, oldValue._2 + benefit)
					}
				}      
			}
			knapMap.toMap
	}

	def analyzeGreedily(
			queries:java.util.List[SingleTableQuery], memorySize:Double) : List[Column] = {
			// create a map of [columnID -> (size, benefit)]
			val knapMap = buildMapForKnapsack(queries)
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