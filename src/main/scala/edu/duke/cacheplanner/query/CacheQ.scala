package edu.duke.cacheplanner.query

import scala.collection.JavaConversions._
import edu.duke.cacheplanner.data.Dataset

class CacheQ (appName: String, query: AbstractQuery, memory: String, cores: String, datasets: java.util.List[Dataset]) 
	extends SubmitQuery(appName, memory, cores) {

  import sqlContext.implicits._

  def submit() {
    // cache datasets
    for (ds <- datasets) {
      println("Caching in Tachyon: " + ds.getName)
      val disk = sc.textFile(hdfsHOME + "/" + ds.getName)
      disk.saveAsTextFile(tachyonHOME + "/" + ds.getName) //.persist(org.apache.spark.storage.StorageLevel.OFF_HEAP)
    }

  }

}
