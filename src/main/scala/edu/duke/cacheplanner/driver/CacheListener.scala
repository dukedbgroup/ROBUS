package edu.duke.cacheplanner.driver

import edu.duke.cacheplanner.listener.ListenerManager

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobStart
import edu.duke.cacheplanner.listener.QueryFinished

class CacheListener(manager: ListenerManager) extends SparkListener{
  val listenerManager = manager

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    listenerManager.postEvent(new QueryFinished(1,1))
  }
}
