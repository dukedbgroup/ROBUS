package edu.duke.cacheplanner.query

import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.{SparkConf, SparkContext}

class SalesGroupQ(appName: String, query: AbstractQuery, sc: SparkContext, datasetsCached: java.util.List[Dataset]) 
	extends SalesQ(appName, query, sc, datasetsCached) {

  val groupExpression = {
    // extract query
    val q = query.asInstanceOf[GroupingQuery]
    val groupCol = q.getGroupingColumn.getColName()

    // group by count
    expression.groupBy(groupCol).count()
  }

  override def submit() {
    try { groupExpression.collect } catch { case e: Exception => e.printStackTrace }
  }

}
