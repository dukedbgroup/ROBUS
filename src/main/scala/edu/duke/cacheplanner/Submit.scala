package edu.duke.cacheplanner

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.Constants
import scala.collection.JavaConversions._

object Submit {

  /**
   * args(0): name of app
   * args(1): memory per executor
   * args(2): max total cores
   * args(3): name of the query
   * args(4): List of Datasets cached for the query
   */
  def main(args: Array[String]) {

    // deserialize datasets to cache
    val datasetListType = new TypeToken[java.util.Collection[Dataset]]() {}.getType();
    val datasets: java.util.List[Dataset] = new Gson().fromJson(args(4), datasetListType);

    // find the right query class from the name
    val c = Class.forName(f"edu.duke.cacheplanner.query.${args(3)}").getConstructor(classOf[String], classOf[String], classOf[String], classOf[java.util.List[Dataset]])
    val query = c.newInstance(args(0), args(1), args(2), datasets).asInstanceOf[{ def submit }]

    // submit query
    query.submit

  }

}
