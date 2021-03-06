/**
 *
 */
package edu.duke.cacheplanner.algorithm.singlecolumn

import scala.collection.immutable.List
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.query.SingleDatasetQuery
//import gurobi.GRBEnv
//import gurobi.GRBModel

/**
 * @author mkunjir
 *
 */
class SingleColumnOptimalAnalyzer extends SingleColumnBatchAnalyzer {
  
//  val env:GRBEnv = new GRBEnv()
//  val model:GRBModel = new GRBModel(env)

  override def analyzeBatch(
			queries: List[SingleDatasetQuery], 
			cachedColumns: List[Column], 
			memorySize: Double): List[Column] = {
    cachedColumns
  }

}