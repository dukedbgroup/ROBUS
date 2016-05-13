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
    println("Stopping the contexts!")
    context.stop()

    // compute final metrics
    val writer = new PrintWriter(new File(config.getLogDirPath + "/metrics"))
//    println("------------- metrics ---------------")
    writer.println("sum wait time -> " + metrics.getTotalWaitTime)
    writer.println("sum execution time -> " + metrics.getTotalExecTime)
    writer.println("workload time -> " + metrics.getTimeOfWorkload)
    writer.println("throughput -> " + metrics.getThroughput)
    writer.println("num queries generated -> " + metrics.getNumQueriesGenerated)
    writer.println("num queries fetched -> " + metrics.getNumQueriesFetched)
    writer.println("num queries submitted -> " + metrics.getNumQueriesSubmitted)
    writer.println("num queries finished -> " + metrics.getNumQueriesFinished)
    writer.println("mean query exec times -> ")
    metrics.getMeanExecTimePerQueue.foreach(writer.println)
    writer.println("mean wait times -> ")
    metrics.getWaitTimePerQueue.foreach(writer.println)
    writer.println("num queries that used cache -> ")
    metrics.getNumQueriesCached.foreach(writer.println)
    writer.println("fraction of queries that used cache -> ")
    metrics.getFractionQueriesCached.foreach(writer.println)
    writer.println("cache utilization per batch -> " 
        + metrics.getTotalCacheLoaded * config.getPlannerBatchTime() / config.getWorkloadTime())
    writer.println("resource fairness index -> " + metrics.getResourceFairnessIndex)
    writer.println("core fairness index -> " + metrics.getCoreFairnessIndex)
    writer.println("wait time fairness -> " + metrics.getWaitTimeFairnessIndex)
    writer.println("histogram on datasets loaded to cache -> ")
    metrics.getDatasetLoadHistogram.foreach(writer.println)
    writer.println("histogram on datasets retained in cache -> ")
    metrics.getDatasetRetainHistogram.foreach(writer.println)
    writer.println("histogram on datasets unloaded from cache -> ")
    metrics.getDatasetUnloadHistogram.foreach(writer.println)
    writer.close()

    // write execution times
    val writer2 = new PrintWriter(new File(config.getLogDirPath + "/runtimes"))
    writer2.println("queueID\tqueryID\tqueryString\ttime(ms)")
    metrics.getFormattedQueryExecTimes.foreach(t => writer2.println(
        t._1 + "\t" + t._2 + "\t\"" + t._3 + "\"\t" + t._4))
    writer2.close()
  }
}
