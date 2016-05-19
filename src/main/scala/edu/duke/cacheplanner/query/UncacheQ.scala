package edu.duke.cacheplanner.query

import scala.collection.JavaConversions._
import edu.duke.cacheplanner.data.Dataset
import tachyon.TachyonURI
import tachyon.client.TachyonFS
import tachyon.conf.TachyonConf

class UncacheQ (appName: String, query: AbstractQuery, memory: String, cores: String, datasets: java.util.List[Dataset]) 
	extends SubmitQuery(appName, memory, cores) {

  import sqlContext.implicits._

  def submit() {
    val client = TachyonFS.get(new TachyonURI(tachyonURL), new TachyonConf())
    // cache datasets
    for (ds <- datasets) {
      println("Uncaching from Tachyon: " + ds.getName)
      val file = tachyonHOME + "/" + ds.getName
      client.delete(new TachyonURI(file), true)
    }

  }

}
