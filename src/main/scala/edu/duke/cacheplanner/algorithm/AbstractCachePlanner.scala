package edu.duke.cacheplanner.algorithm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._
import java.util
import edu.duke.cacheplanner.conf.ConfigManager
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.QueryUtil
import org.apache.spark.sql.SchemaRDD

/**
 * Abstract class for CachePlanner
 */
abstract class AbstractCachePlanner(setup: Boolean, manager: ListenerManager, 
    queues: util.List[ExternalQueue], datasets: java.util.List[Dataset], 
    config: ConfigManager) {
  val listenerManager: ListenerManager = manager
  val isMultipleSetup = setup // true = multi app setup, false = single app setup
  var started = false
  val externalQueues = queues
  val sc = initSparkContext
  val hiveContext = initHiveContext
  val plannerThread = initPlannerThread()
//  @volatile var schemaRDDs: scala.collection.mutable.Map[String, SchemaRDD] = 
//    new scala.collection.mutable.HashMap[String, SchemaRDD]()
  initTables


  /**
   * TODO: read all the hardcoded parameters from a config file
   */
  def initSparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("cacheplanner").setMaster("spark://yahoo047:7077")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(Seq("target/scala-2.10/CachePlanner-assembly-0.1.jar"))
    conf.set("spark.scheduler.mode", "FAIR")
    // HACK: assuming that internal file has same pool names as corresponding queue id
    // Also assuming weights and min shares match. 
    // Ideally there should be a single config file
    conf.set("spark.scheduler.allocation.file", "conf/internal.xml")
    conf.set("spark.executor.memory", "4096m")	// 4GB per node
    // this fraction makes cache space about 2GB, but we are going to use only 1GB for algorithms
    conf.set("spark.storage.memoryFraction", "0.0356")
    conf.set("spark.akka.frameSize", "512")

    conf.set("spark.eventLog.enabled", "true")
	conf.set("spark.eventLog.dir", "spark_event_log")

	val sc = new SparkContext(conf)
    sc
  }

  def initHiveContext: HiveContext = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext._
    hiveContext
  }

  def initTables() {
    for(data <- datasets) {
      println(QueryUtil.getTableCreateSQL(data))
      hiveContext.hql(QueryUtil.getDropTableSQL(data.getName()))
      val schema = hiveContext.hql(QueryUtil.getTableCreateSQL(data))
//      schemaRDDs(data.getName()) = schema
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
    for (d <- datasets.toList) {
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
