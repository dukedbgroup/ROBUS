package edu.duke.cacheplanner.listener

import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.data.Dataset

/**
 * Defining Listener Events
 */
trait ListenerEvent

case class QuerySerialize(query: AbstractQuery) extends ListenerEvent

case class QueryGenerated(query: AbstractQuery) extends ListenerEvent
  
case class QueryFetchedByCachePlanner(query: AbstractQuery) extends ListenerEvent

case class QueryPushedToSparkScheduler(query: AbstractQuery, 
    cacheUsed: Double) extends ListenerEvent

case class QueryFinished(query: AbstractQuery) extends ListenerEvent

case class DatasetLoadedToCache(dataset: Dataset) extends ListenerEvent

case class DatasetRetainedInCache(dataset: Dataset) extends ListenerEvent

case class DatasetUnloadedFromCache(dataset: Dataset) extends ListenerEvent
    
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
   * called when the query is pushed to Spark Scheduler
   */
  def onQueryPushedToSparkScheduler(event: QueryPushedToSparkScheduler) { }

  /**
   * called when the query is finished executing
   */
  def onQueryFinished(event: QueryFinished) { }

  /**
   * called when a dataset is loaded to spark cache
   */
  def onDatasetLoadedToCache(event: DatasetLoadedToCache) {}

  /**
   * called when a dataset is retained in spark cache
   */
  def onDatasetRetainedInCache(event: DatasetRetainedInCache) {}

  /**
   * called when a dataset is unloaded from spark cache
   */
  def onDatasetUnloadedFromCache(event: DatasetUnloadedFromCache) {}

}