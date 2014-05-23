package edu.duke.cacheplanner.algorithm

import shark.{SharkContext, SharkEnv}

import edu.duke.cacheplanner.listener.ListenerManager

/**
 * Abstract class for CachePlanner
 */
abstract class AbstractCachePlanner(mode: Boolean, manager: ListenerManager, sharkContext: SharkContext) {
  val listenerManager: ListenerManager = manager
  val sc = sharkContext
  val isMultipleSetup = mode // true = multi app setup, false = single app setup
  var started = false

  private val plannerThread = initPlannerThread()
  
  
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
   * initialize Planner Thread.
   */
  def initPlannerThread(): Thread

  
}