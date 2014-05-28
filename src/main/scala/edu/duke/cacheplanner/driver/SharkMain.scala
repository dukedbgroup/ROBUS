package edu.duke.cacheplanner.driver


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.scheduler.JobLogger
import SparkContext._
import shark.{SharkContext, SharkEnv}
import edu.duke.cacheplanner.listener.{LoggingListener, ListenerManager}


//initiates shark context. This code just copied from shark context testing 

object SharkMain {

  def main(args: Array[String]) {

    // create shark context (master configuration is set from "conf/shark-env.sh")
    // val conf = new SparkConf()
    //          .setMaster("spark://yahoo047:7077")
    //          .setAppName("Cache_Experiment")
    //          .set("spark.executor.memory", "6g")
    //          .set("spark.scheduler.mode", "FAIR")
    //          .set("spark.scheduler.allocation.file", "input/alloc.xml")
    // val sc = new SparkContext(conf)
    // SharkEnv.sc = sc
    System.setProperty("spark.executor.memory", "6g")
    System.setProperty("spark.scheduler.mode", "FAIR")
    System.setProperty("spark.scheduler.allocation.file", "conf/internal.xml")        

    SharkEnv.initWithSharkContext("Cache_Experiment")
    val sc = SharkEnv.sc.asInstanceOf[SharkContext]

    //attach listener
    val listenerManager = new ListenerManager
    val loggingListener = new LoggingListener
    listenerManager.addListener(loggingListener)

    // attach JobLogger & StatsReportListener
    val joblogger = new JobLogger("test", "cache_test")
    val listener = new StatsReportListener()
    val cache = new CacheListener(listenerManager)
    sc.addSparkListener(joblogger)
    sc.addSparkListener(listener)
    sc.addSparkListener(cache)

    SharkQuery.init(sc)

    val thread1 = new Thread(new SharkRunnable(sc))
    val thread2 = new Thread(new SharkRunnableTwo(sc))    
    listenerManager.start
    thread1.start()
    thread2.start()
  }
}
