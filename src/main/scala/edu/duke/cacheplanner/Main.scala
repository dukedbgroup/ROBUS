package edu.duke.cacheplanner

import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.listener.{SerializeListener, LoggingListener}
import java.io._
import edu.duke.cacheplanner.listener.CachePlannerMetrics

object Main {
  def main(args: Array[String]) {
    
    val context = Factory.createContext
    val config = Factory.getConfigManager
    context.addListener(new LoggingListener)
    context.addListener(new SerializeListener(config.getReplayFilePath()))
    val metrics = new CachePlannerMetrics(Factory.getQueues)
    context.addListener(metrics)
    context.start()
    
    Thread.sleep(config.getWorkloadTime * 1000)	// total time of batch
    println("Stopping the context!")
    context.stop()

    // compute final metrics
    println("------------- metrics ---------------")
    println("total wait time -> " + metrics.getTotalWaitTime)
    println("throughput -> " + metrics.getThroughput)
    println("num queries generated -> " + metrics.getNumQueriesGenerated)
    println("num queries fetched -> " + metrics.getNumQueriesFetched)
    println("num queries submitted -> " + metrics.getNumQueriesSubmitted)
    println("num queries finished -> " + metrics.getNumQueriesFinished)
    println("mean query exec times -> ")
    metrics.getMeanExecTimePerQueue.foreach(println)
    println("num queries that used cache -> ")
    metrics.getNumQueriesCached.foreach(println)
    println("fraction of queries that used cache -> ")
    metrics.getFractionQueriesCached.foreach(println)
    println("cache utilization per batch -> " 
        + metrics.getTotalCacheLoaded * config.getPlannerBatchTime() / config.getWorkloadTime())
    println("resource fairness index -> " + metrics.getResourceFairnessIndex)
    println("core fairness index -> " + metrics.getCoreFairnessIndex)
    println("wait time fairness -> " + metrics.getWaitTimeFairnessIndex)
    println("histogram on datasets loaded to cache -> ")
    metrics.getDatasetLoadHistogram.foreach(println)
    println("histogram on datasets retained in cache -> ")
    metrics.getDatasetLoadHistogram.foreach(println)
    println("histogram on datasets unloaded from cache -> ")
    metrics.getDatasetLoadHistogram.foreach(println)
      //oos.close()
      //fos.close()
  }
}