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

class Context(manager: ListenerManager) {
  
  private val listenerManager = manager
  private val sc = initSharkContext
  
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
  
  def start {
    listenerManager.start()
  }
  
  def stop {
    listenerManager.stop()
  }
}