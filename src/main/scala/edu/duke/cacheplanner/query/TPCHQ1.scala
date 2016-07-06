package edu.duke.cacheplanner.query

import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}

class TPCHQ1(appName: String, query: AbstractQuery, sc: SparkContext, datasetsCached: java.util.List[Dataset]) 
	extends AbstractTPCHQuery(appName, sc, datasetsCached) {

  import sqlContext.implicits._

  def submit() {
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    val res = lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
        sum(decrease($"l_extendedprice", $"l_discount")),
        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")

    try { res.collect } catch { case e: Exception => e.printStackTrace }
  }

}
