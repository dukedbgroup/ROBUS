package edu.duke.cacheplanner

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.query.SingleDatasetQuery
import edu.duke.cacheplanner.query.GroupingQuery
import edu.duke.cacheplanner.query.TPCHQuery
import edu.duke.cacheplanner.query.Constants
import scala.collection.JavaConversions._

object Submit {

  /**
   * args(0): serialized AbstractQuery 
   * args(1): memory per executor
   * args(2): max total cores
   * args(3): serialized List of Datasets cached for the query
   */
  def main(args: Array[String]) {

    // deserialize query
    // HACK: cache and uncache queries just send a String instead of query object
    val gson = new Gson()
    val (passedQuery, name, appName) = {
     if(args(0) == Constants.CACHE_QUERY || args(0) == Constants.UNCACHE_QUERY) {
       (null, args(0), args(0))
     } else {
      val passedQuery = {
    	      if (args(0).contains("groupingColumn")) {
	        gson.fromJson(args(0), classOf[GroupingQuery])
	      } 
	      else if (args(0).contains("path")) {
                gson.fromJson(args(0), classOf[TPCHQuery])
	      }
	      else {
	        gson.fromJson(args(0), classOf[SingleDatasetQuery])
	      }
       }
       val name = passedQuery.getName
       val appName = "Queue:" + passedQuery.getQueueID + ",Query:" + passedQuery.getQueryID + "," + name
       (passedQuery, name, appName)
      }
     }

    // deserialize datasets to cache
    val datasetListType = new TypeToken[java.util.Collection[Dataset]]() {}.getType();
    val datasets: java.util.List[Dataset] = gson.fromJson(args(3), datasetListType);

    // find the right query class from the name
    val c = Class.forName(f"edu.duke.cacheplanner.query.${name}").getConstructor(classOf[String], classOf[AbstractQuery], classOf[String], classOf[String], classOf[java.util.List[Dataset]])
    val query = c.newInstance(appName, passedQuery, args(1), args(2), datasets).asInstanceOf[{ def submit }]

    // submit query
    query.submit

  }

}
