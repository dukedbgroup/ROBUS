package edu.duke.cacheplanner.planner

/**
 * Interface for CachePlanner
 */
trait CachePlanner {
  
  /**
   * analyze batch from ExternalQueues
   */
  def analyzeBatch { }
}