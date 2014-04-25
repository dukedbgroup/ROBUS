package edu.duke.cacheplanner.algorithm

import edu.duke.cacheplanner.listener.ListenerManager

/**
 * Abstract class for CachePlanner
 */
trait AbstractCachePlanner {
  //val listenerManager: ListenerManager
  var started = false
  
  private val plannerThread = new Thread("ListenerManager") {
    setDaemon(true)
    override def run() {
      while(true) {
        if(!started){
          return
        }
        Thread.sleep(2000) //modify this, wait for the event?
        analyzeBatch
      }
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
  
  /**
   * analyze batch from ExternalQueues. 
   */
  def analyzeBatch()

}