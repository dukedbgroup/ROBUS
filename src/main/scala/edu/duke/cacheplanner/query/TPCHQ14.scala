package edu.duke.cacheplanner.query

import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

class TPCHQ14(appName: String, memory: String, cores: String, datasetsCached: java.util.List[Dataset]) 
	extends AbstractTPCHQuery(appName, memory, cores, datasetsCached) {

  import sqlContext.implicits._

  def submit() {
    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    val res = part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))

    res.collect
  }

}
