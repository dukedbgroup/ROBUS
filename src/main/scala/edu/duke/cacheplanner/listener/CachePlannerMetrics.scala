/**
 * 
 */
package edu.duke.cacheplanner.listener

import scala.collection.JavaConversions._

import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.queue.ExternalQueue

/**
 * @author mayuresh
 *
 */
class CachePlannerMetrics(queues: java.util.List[ExternalQueue])
extends Listener {

  var queryWaitTimes = scala.collection.mutable.Map[AbstractQuery, Long]()
  var queryCacheSize = scala.collection.mutable.Map[AbstractQuery, Double]()
  // invariant: refers to time of first query seen by event QueryGenerated
  var timeFirstQueryGenerated:Long = 0L
  // invariant: refers to time of last query seen by event QueryPushedToSparkScheduler
  var timeLastQueryPushed:Long = 0L

  override def onQueryGenerated(event: QueryGenerated) {
    if(timeFirstQueryGenerated == 0L) {
      timeFirstQueryGenerated = System.currentTimeMillis()
    }
	queryWaitTimes(event.query) = System.currentTimeMillis()
  }

  /**
   * Assuming that every query that is generated is fetched by cache planner
   */
  override def onQueryFetchedByCachePlanner(event: QueryFetchedByCachePlanner) { 
	val startTime = queryWaitTimes(event.query)
	queryWaitTimes(event.query) = (System.currentTimeMillis() - startTime)
  }
  
  override def onQueryPushedToSparkScheduler(event: QueryPushedToSparkScheduler) {
    timeLastQueryPushed = System.currentTimeMillis()
    queryCacheSize(event.query) = event.cacheUsed
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
    return (timeLastQueryPushed - timeFirstQueryGenerated).doubleValue / queryWaitTimes.size
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
}