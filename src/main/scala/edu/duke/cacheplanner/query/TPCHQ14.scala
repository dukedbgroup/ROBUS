package edu.duke.cacheplanner.query

import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}

class TPCHQ14(appName: String, query: AbstractQuery, sc: SparkContext, datasetsCached: java.util.List[Dataset]) 
	extends AbstractTPCHQuery(appName, sc, datasetsCached) {

  import sqlContext.implicits._

  def submit() {
    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    val res = part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))

    try { res.collect } catch { case e: Exception => e.printStackTrace }
  }

}
