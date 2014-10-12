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
    print("\n------------- metrics ---------------")
    print("\ntotal wait time -> " + metrics.getTotalWaitTime)
    print("\nthroughput -> " + metrics.getThroughput)
    print("\nresource fairness -> " + metrics.getResourceFairnessIndex)
      //oos.close()
      //fos.close()
  }
}