/**
 * 
 */
package edu.duke.cacheplanner.listener

import scala.collection.JavaConversions._

import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.queue.ExternalQueue

/**
 * @author mayuresh
 *
 */
class CachePlannerMetrics(queues: java.util.List[ExternalQueue])
extends Listener {

  // map of query to its wait time in queue
  var queryWaitTimes = scala.collection.mutable.Map[AbstractQuery, Long]()
  // map of query to amount of cache it used
  var queryCacheSize = scala.collection.mutable.Map[AbstractQuery, Double]()
  // count of number of queries generated
  var numQueriesGenerated: Long = 0L
  // count of number of queries fetched from queues
  var numQueriesFetched: Long = 0L
  // count of number of queries submitted to Spark
  var numQueriesSubmitted: Long = 0L
  // count of number of queries that used cached data per queue
  var numQueriesCachedPerQueue = scala.collection.mutable.Map[Int, Long]()

  // map of dataset to number of times it was cached
  var datasetLoaded = scala.collection.mutable.Map[Dataset, Long]()
  // map of dataset to number of times it was retained
  var datasetRetained = scala.collection.mutable.Map[Dataset, Long]()
  // map of dataset to number of times it was uncached
  var datasetUnloaded = scala.collection.mutable.Map[Dataset, Long]()

  // invariant: refers to time of first query seen by event QueryGenerated
  var timeFirstQueryGenerated:Long = 0L
  // invariant: refers to time of last query seen by event QueryPushedToSparkScheduler
  var timeLastQueryPushed:Long = 0L

  override def onQueryGenerated(event: QueryGenerated) {
    if(timeFirstQueryGenerated == 0L) {
      timeFirstQueryGenerated = System.currentTimeMillis()
    }
	queryWaitTimes(event.query) = System.currentTimeMillis()
	numQueriesGenerated = numQueriesGenerated + 1
  }

  /**
   * Assuming that every query that is generated is fetched by cache planner
   */
  override def onQueryFetchedByCachePlanner(event: QueryFetchedByCachePlanner) { 
	val startTime = queryWaitTimes(event.query)
	queryWaitTimes(event.query) = (System.currentTimeMillis() - startTime)
	numQueriesFetched = numQueriesFetched + 1
  }
  
  override def onQueryPushedToSparkScheduler(event: QueryPushedToSparkScheduler) {
    timeLastQueryPushed = System.currentTimeMillis()
    queryCacheSize(event.query) = event.cacheUsed
	numQueriesSubmitted = numQueriesSubmitted + 1
	val current = numQueriesCachedPerQueue.getOrElse(event.query.getQueueID(), 0L)
	numQueriesCachedPerQueue(event.query.getQueueID()) = current + 1
  }

  override def onDatasetLoadedToCache(event: DatasetLoadedToCache) {
    val count = datasetLoaded.getOrElse(event.dataset, 0L)
    datasetLoaded(event.dataset) = count + 1
  }

  override def onDatasetRetainedInCache(event: DatasetRetainedInCache) {
    val count = datasetRetained.getOrElse(event.dataset, 0L)
    datasetRetained(event.dataset) = count + 1
  }

  override def onDatasetUnloadedFromCache(event: DatasetUnloadedFromCache) {
    val count = datasetUnloaded.getOrElse(event.dataset, 0L)
    datasetUnloaded(event.dataset) = count + 1
  }

  def getTotalWaitTime(): Long = {
    var totalTime = 0L
    queryWaitTimes.foreach(t => totalTime += t._2)
    return totalTime
  }

  def getTotalCacheUsed(queueId: Int): Double = {
    var totalCache = 0d
    queryCacheSize.foreach(t => if(t._1.getQueueID == queueId) {
      totalCache += t._2
    })
    return totalCache
  }

  def getThroughput(): Double = {
    return (timeLastQueryPushed - timeFirstQueryGenerated).doubleValue / numQueriesGenerated
  }

  def getResourceFairnessIndex(): Double = {
    var runningSum = 0d;
    var runningSumSquares = 0d;
    for(queue <- queues) {
      val cacheByWeight = getTotalCacheUsed(queue.getId) / queue.getWeight
      runningSum += cacheByWeight
      runningSumSquares += cacheByWeight * cacheByWeight
    }
    return (runningSum * runningSum) / (queues.size() * runningSumSquares)
  }

  def getDatasetLoadHistogram(): List[(String, Long)] = {
    return datasetLoaded.toList map {t => (t._1.getName(), t._2)} sortBy {_._2}
  }

  def getDatasetRetainHistogram(): List[(String, Long)] = {
    return datasetRetained.toList map {t => (t._1.getName(), t._2)} sortBy {_._2}
  }

  def getDatasetUnloadHistogram(): List[(String, Long)] = {
    return datasetUnloaded.toList map {t => (t._1.getName(), t._2)} sortBy {_._2}
  }

  def getNumQueriesGenerated(): Long = {
    return numQueriesGenerated
  }

  def getNumQueriesFetched(): Long = {
    return numQueriesFetched
  }

  def getNumQueriesSubmitted(): Long = {
    return numQueriesSubmitted
  }

  def getNumQueriesCached(): List[(Int, Long)] = {
    return numQueriesCachedPerQueue.toList
  }
}