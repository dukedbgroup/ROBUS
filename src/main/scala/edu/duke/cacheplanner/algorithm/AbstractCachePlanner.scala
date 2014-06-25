package edu.duke.cacheplanner.algorithm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.queue.ExternalQueue
import java.util
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.QueryUtil

/**
 * Abstract class for CachePlanner
 */
abstract class AbstractCachePlanner(setup: Boolean, manager: ListenerManager, queues: util.List[ExternalQueue], data: java.util.List[Dataset]) {
  val listenerManager: ListenerManager = manager
  val datasets = data
  val isMultipleSetup = setup // true = multi app setup, false = single app setup
  var started = false
  val externalQueues = queues
  val sc = initSparkContext
  val hiveContext = initHiveContext

  private val plannerThread = initPlannerThread()

  def initSparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    conf.set("spark.eventLog.enabled", "true")
    //conf.set("spark.eventLog.dir", "file:///home/shlee0605/shlee_test/spark-sql/event_log")
    conf.setJars(Seq("target/scala-2.10/spark-example-assembly-0.1.0.jar"))
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.scheduler.allocation.file", "conf/internal.xml")

    val sc = new SparkContext(conf)
    sc
  }

  def initHiveContext: HiveContext = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext._
    initTables()
    hiveContext
  }

  def initTables() {
    for(data <- datasets.asInstanceOf[List[Dataset]]) {
      hiveContext.hql(QueryUtil.getTableCreateSQL(data))
    }
  }

  def start() {
    started = true    
    plannerThread.start()
  }
  
  def stop() {
    if (!started) {
	    throw new IllegalStateException("cannot be done because a listener has not yet started!");
	  }
    started = false
    plannerThread.join()
  }

  def getDataset(name: String): Dataset = {
    for (d <- datasets.asInstanceOf[List[Dataset]]) {
      if (d.getName == name) {
        return d
      }
    }
    return null
  }

  /**
   * initialize Planner Thread.
   */
  def initPlannerThread(): Thread

  
}