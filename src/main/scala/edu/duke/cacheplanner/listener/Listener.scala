package edu.duke.cacheplanner.listener

import edu.duke.cacheplanner.query.AbstractQuery

/**
 * Defining Listener Events
 */
trait ListenerEvent

case class QuerySerialize(query: AbstractQuery) extends ListenerEvent

case class QueryGenerated(queryID: Int, queueID: Int) extends ListenerEvent
  
case class QueryFetchedByCachePlanner(queryID: Int, queueID: Int) extends ListenerEvent

case class QueryPushedToSharkScheduler(queryID: Int, queueID: Int) extends ListenerEvent

/** An event to shutdown the listener thread. */
case object ListenerShutdown extends ListenerEvent


/**
 * an interface for the listener.
 */
trait Listener {
  def onQuerySerialize(event: QuerySerialize) { }
  /**
   * called when query is generated from QueryGenerator
   */
  def onQueryGenerated(event: QueryGenerated) { }
  
  /**
   * called when the query is fetched by CachePlanner
   */
  def onQueryFetchedByCachePlanner(event: QueryFetchedByCachePlanner) { }
  
  /**
   * called when the query is pushed to SharkScheduler
   */
  def onQueryPushedToSharkScheduler(event: QueryPushedToSharkScheduler) { }

}