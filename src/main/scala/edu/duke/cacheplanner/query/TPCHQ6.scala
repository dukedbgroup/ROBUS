package edu.duke.cacheplanner.query

import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}

class TPCHQ6(appName: String, query: AbstractQuery, sc: SparkContext, datasetsCached: java.util.List[Dataset]) 
	extends AbstractTPCHQuery(appName, sc, datasetsCached) {

  import sqlContext.implicits._

  def submit() {
    val res = lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))

    try { res.collect } catch { case e: Exception => e.printStackTrace }
  }

}
