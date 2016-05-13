package edu.duke.cacheplanner.query

import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

class TPCHQ17(appName: String, memory: String, cores: String, datasetsCached: java.util.List[Dataset]) 
	extends AbstractTPCHQuery(appName, memory, cores, datasetsCached) {

  import sqlContext.implicits._

  def submit() {
    val mul02 = udf { (x: Double) => x * 0.2 }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")

    val res = fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)

    res.collect
  }

}
