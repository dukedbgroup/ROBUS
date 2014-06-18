/**
 *
 */
package edu.duke.cacheplanner.algorithm

import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.query.AbstractQuery
import gurobi.GRBEnv
import gurobi.GRBModel
import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.SingleTableQuery

/**
 * @author mayuresh
 *
 */
object BatchAnalyzer {
  
  val env:GRBEnv = new GRBEnv()
  val model:GRBModel = new GRBModel(env)
  // column identifier will be <dataset-name><separator><column-name>
  val colIDSeparator = ":"
  val columnMap: Map[String, Column] = createColumnMap()
  
  /**
   * Create a map of ColumnID to column object
   */
  def createColumnMap(): Map[String, Column] = {
    var cMap = scala.collection.mutable.Map[String, Column]()
     for (ds <- Factory.datasets.asInstanceOf[List[Dataset]]) {
       for(col <- ds.getColumns().asInstanceOf[List[Column]]) {
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
    for (query <- queries.asInstanceOf[List[SingleTableQuery]]) {
      val column = query.getRelevantColumns().get(0) // assuming there is only one relevant column
      val colID = query.getDataset().getName() + colIDSeparator 
      + column.getColName()
      val size = column.getSize()
      val weight = query.getWeight()
      
      knapMap
    }
    knapMap.toMap
  }
  
  def analyzeSingleTableQueries(
      queries:java.util.List[SingleTableQuery]): List[Column] = {
    // create a map of [columnID -> (size, benefit)]

    // define indicator variables z_i saying whether column i is cached or not
  }
  
}