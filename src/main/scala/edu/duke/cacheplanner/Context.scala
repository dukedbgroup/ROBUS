package edu.duke.cacheplanner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.scheduler.JobLogger
import SparkContext._
import shark.{SharkContext, SharkEnv}
import shark.api.ResultSet
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.Listener
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import edu.duke.cacheplanner.algorithm.AbstractCachePlanner

class Context(manager: ListenerManager, generators: java.util.List[AbstractQueryGenerator], planner: AbstractCachePlanner) {
  
  private val listenerManager = manager
  //private val sc = initSharkContext
  private val queryGenerators = generators
  private val cachePlanner = planner
  
  def initSharkContext() : SharkContext = {
    SharkEnv.initWithSharkContext("Cache_Experiment")
    val sc = SharkEnv.sc.asInstanceOf[SharkContext]
    // attach JobLogger & StatsReportListener
    val joblogger = new JobLogger("cachePlanner", "cache_planner_job_logger")
    val listener = new StatsReportListener()
    sc.addSparkListener(joblogger)
    sc.addSparkListener(listener)
    sc
  }
  
  
  def setConf(attribute: String, value: String) {
    System.setProperty(attribute, value)
  }
  
  def addListener(listener : Listener) {
    listenerManager.addListener(listener)
  }
  
  def start() {
    listenerManager.start()
    for(gen <- queryGenerators.toArray()) {
      gen.asInstanceOf[AbstractQueryGenerator].start()
    }
    //planner.start()
  }
  
  def stop() {
    listenerManager.stop()
    for(gen <- queryGenerators.toArray()) {
      gen.asInstanceOf[AbstractQueryGenerator].stop()
    }
    planner.stop()
  }
}