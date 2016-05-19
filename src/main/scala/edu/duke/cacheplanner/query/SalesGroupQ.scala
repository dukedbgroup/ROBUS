package edu.duke.cacheplanner.query

import edu.duke.cacheplanner.data.Dataset

class SalesGroupQ(appName: String, query: AbstractQuery, memory: String, cores: String, datasetsCached: java.util.List[Dataset]) 
	extends SalesQ(appName, query, memory, cores, datasetsCached) {

  val groupExpression = {
    // extract query
    val q = query.asInstanceOf[GroupingQuery]
    val groupCol = q.getGroupingColumn.getColName()

    // group by count
    expression.groupBy(groupCol).count()
  }

  override def submit() {
    groupExpression.collect
  }

}
