package edu.duke.cacheplanner.driver

import edu.duke.cacheplanner.listener.{QueryGenerated, ListenerManager, QueryFinished}

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobStart

class CacheListener(manager: ListenerManager) extends SparkListener{
  val listenerManager = manager

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    listenerManager.postEvent(new QueryFinished(1,1))
    listenerManager.postEvent(new QueryGenerated(1,1))
  }

  def onTaskStart(taskStart : org.apache.spark.scheduler.SparkListenerTaskStart) : Unit = {
    listenerManager.postEvent(new QueryFinished(2,1))
    listenerManager.postEvent(new QueryGenerated(1,1))
  }

}
