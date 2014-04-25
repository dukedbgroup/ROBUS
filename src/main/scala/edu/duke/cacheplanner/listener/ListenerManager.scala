package edu.duke.cacheplanner.listener

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.LinkedBlockingQueue

/**
 * Listener Manager notifies all the listeners attached when
 * an event is issued. 
 */
class ListenerManager {
  
  private val listenerList = new ArrayBuffer[Listener]
  
  private val QUEUE_CAPACITY = 1000
  private val eventQueue = new LinkedBlockingQueue[ListenerEvent](QUEUE_CAPACITY)
  private var started = false

  /**
   * define a thread
   */
  private val listenerThread = new Thread("ListenerManager") {
    setDaemon(true)
    override def run() {
      while(true) {
        val event = eventQueue.take
        if(event == ListenerShutdown) {
          return
        }
        notifyListeners(event)
      }
    }
  }
  
  /**
   * notifies an event to all attached listeners. (internal use)
   */
  private def notifyListeners(event: ListenerEvent) {
    event match {
      case queryGenerated: QueryGenerated =>
        listenerList.foreach(_.onQueryGenerated(queryGenerated))
      case queryFetchedByCachePlanner: QueryFetchedByCachePlanner =>
        listenerList.foreach(_.onQueryFetchedByCachePlanner(queryFetchedByCachePlanner))
      case queryPushedToSharkScheduler: QueryPushedToSharkScheduler =>
        listenerList.foreach(_.onQueryPushedToSharkScheduler(queryPushedToSharkScheduler))
      case ListenerShutdown => 
    }
  }


  /**
   * Listener Manager API: postEvent(), addListener(), start(), stop() 
   */
  
  
  /**
   * send an event to the attached listeners
   */
  def postEvent(event: ListenerEvent) {
    val eventAdded = eventQueue.offer(event)
    if (!eventAdded) {
    	println("eventQueue is full")	// error handling -> logging?
    }
  }

  /**
   * attach a listener to the listener manager
   */
  def addListener(listener: Listener) {
    listenerList += listener
  }
  
  /**
   * start a listener thread
   */
  def start() {
    if (started) {
      throw new IllegalStateException("Listener bus already started!")
    }
    listenerThread.start()
    started = true
  }

  /**
   * stop a listener thread
   */
  def stop() {
    if (!started) {
      throw new IllegalStateException("cannot be done because a listener has not yet started!")
    }
    postEvent(ListenerShutdown)
    listenerThread.join()
  }
   
}

