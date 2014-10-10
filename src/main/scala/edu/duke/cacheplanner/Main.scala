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
    context.addListener(new CachePlannerMetrics(Factory.getQueues))
    context.start()
    
    Thread.sleep(config.getWorkloadTime)	// total time of batch
    context.stop()
      //oos.close()
      //fos.close()
  }
}