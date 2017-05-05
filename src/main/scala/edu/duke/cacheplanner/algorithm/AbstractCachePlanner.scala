package edu.duke.cacheplanner.algorithm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import java.util
import edu.duke.cacheplanner.conf.ConfigManager
import edu.duke.cacheplanner.conf.YarnSparkConf
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.CacheQ
import edu.duke.cacheplanner.query.QueryUtil

/**
 * Abstract class for CachePlanner
 */
abstract class AbstractCachePlanner(setup: Boolean, manager: ListenerManager, 
    queues: util.List[ExternalQueue], datasets: java.util.List[Dataset], 
    tpchDatasets: java.util.List[Dataset], config: ConfigManager) {
  val listenerManager: ListenerManager = manager
  val isMultipleSetup = setup // true = multi app setup, false = single app setup
  var started = false
  val externalQueues = queues
  val plannerThread = initPlannerThread()

  // container configuration for executors
  val memoryWorker = "10g"
  val totalMaxCores = "80"
  // val memoryExecutor = 10240/externalQueues.length + "m"   // FIXME: assuming 10GB reserved for spark on each worker
  // val coresMax = 10*8/externalQueues.length + ""  // FIXME: assuming 8 cores per worker

  // Following statements could be committed if hive warehouse has already loaded all the tables to save time
  // use this context to initialize tables 
  var sparkContext = initSparkContext("SingleContext")
  // create cache dataframe expressions for all datasets
//  CacheQ.createDataframes(sparkContext, datasets)
//  CacheQ.createDataframes(sparkContext, tpchDatasets)
sparkContext.stop  //HACK: for yarn

/*
  def initSparkContexts: java.util.Map[Integer, SparkContext] = {
    val contexts = new java.util.HashMap[Integer, SparkContext]()
    for (q <- externalQueues) {
      contexts.put(q.getId(), initSparkContext(q.getQueueName()))
    }
    contexts
  }

  def initSQLContexts: java.util.Map[Integer, SQLContext] = {
    val contexts = new java.util.HashMap[Integer, SQLContext]()
    for ((i, ctx) <- sparkContexts) {
      contexts.put(i, new SQLContext(ctx))
    }
    contexts 
  }

  def initHiveContexts: java.util.Map[Integer, HiveContext] = {
    val contexts = new java.util.HashMap[Integer, HiveContext]()
    for ((i, ctx) <- sparkContexts) {
      contexts.put(i, new HiveContext(ctx))
    }
    contexts
  }
*/

  /**
   * TODO: read all the hardcoded parameters from a config file
   */
  def initSparkContext(name: String): SparkContext = {
    val sc = new SparkContext(new YarnSparkConf(name).getConf)

    sc
  }

/*  def initHiveContext: HiveContext = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext._
    hiveContext
  }

  def initTables(hiveContext: HiveContext, datasets: java.util.List[Dataset]) {
//    for((i, hiveContext) <- hiveContexts) {
     for(data <- datasets) {
      println(QueryUtil.getTableCreateSQL(data))
      // hiveContext.sql(QueryUtil.getDropTableSQL(data.getName()))
      val schema = hiveContext.sql(QueryUtil.getTableCreateSQL(data))

      // We could query external tables as well, but loading each table in hive
      // warehouse to have a fair comparison of all the algorithms
      var drop_cache_table = QueryUtil.getDropTableSQL(data.getCachedName())
      hiveContext.sql(drop_cache_table)
      val queryString = QueryUtil.getCreateTableAsCachedSQL(data)
      try {
   	    hiveContext.sql(queryString)
      } catch {
        case e: Exception => 
          println("not able to create table. "); e.printStackTrace()
      }
     }
//    }
  }
*/

  def start() {
    started = true 
    plannerThread.setDaemon(true)   
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
