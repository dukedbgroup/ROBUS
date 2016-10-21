/**
 * 
 */
package edu.duke.cacheplanner.listener

import scala.collection.JavaConversions._
import scalaz._, Scalaz._

import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.query.SingleDatasetQuery
import edu.duke.cacheplanner.query.TPCHQuery
import edu.duke.cacheplanner.queue.ExternalQueue

/**
 * @author mayuresh
 *
 */
class CachePlannerMetrics(queues: java.util.List[ExternalQueue])
extends Listener {

  // map of query to its wait time in external queue
  var queryWaitTimes = scala.collection.mutable.Map[AbstractQuery, Long]()
  // map of query to its scheduling delay
  var querySchedulingDelays = scala.collection.mutable.Map[AbstractQuery, Long]()
  // exec time per query
  var queryExecTimes = scala.collection.mutable.Map[AbstractQuery, Long]()
  // map of query to amount of cache it used
  var queryCacheSize = scala.collection.mutable.Map[AbstractQuery, Double]()
  // map of dataset to number of queries requesting it
  var queriesPerDataset = scala.collection.mutable.Map[String, Long]()
  // count of number of queries generated
  var numQueriesGenerated: Long = 0L
  // count of number of queries fetched from queues
  var numQueriesFetched: Long = 0L
  // count of number of queries submitted to Spark
  var numQueriesSubmitted: Long = 0L
  // count of number of queries finished
  var numQueriesFinished: Long = 0L
  // count of number of queries submitted to Spark per queue
  var numQueriesSubmittedPerQueue = scala.collection.mutable.Map[Int, Long]()
  // count of number of queries that used cached data per queue
  var numQueriesCachedPerQueue = scala.collection.mutable.Map[Int, Long]()

  // map of dataset to number of times it was cached
  var datasetLoaded = scala.collection.mutable.Map[String, Long]()
  // map of dataset to number of times it was retained
  var datasetRetained = scala.collection.mutable.Map[String, Long]()
  // map of dataset to number of times it was uncached
  var datasetUnloaded = scala.collection.mutable.Map[String, Long]()
  // sum total cache use, added when a dataset is loaded or retained.
  var totalCacheLoaded: Double = 0d

  // invariant: refers to time of first query seen by event QueryGenerated
  var timeFirstQueryGenerated:Long = 0L
  // invariant: refers to time of last query seen by event QueryFinished
  var timeLastQueryFinished:Long = 0L

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
  	val batchSize = 1000L * Factory.getConfigManager.getPlannerBatchTime
	val startTime = queryWaitTimes(event.query)
	val waitTime = batchSize - (startTime - timeFirstQueryGenerated + 2000L) % batchSize // HACK: assuming the workload started 3 seconds before first query
	queryWaitTimes(event.query) = waitTime
        querySchedulingDelays(event.query) = math.min(startTime + waitTime, System.currentTimeMillis()) // HACK: the first case simulates when queries are scheduled without any synchronization barriers
 	numQueriesFetched = numQueriesFetched + 1
  }
  
  override def onQueryPushedToSparkScheduler(event: QueryPushedToSparkScheduler) {
    querySchedulingDelays(event.query) = System.currentTimeMillis() - querySchedulingDelays(event.query)

    queryExecTimes(event.query) = System.currentTimeMillis()

	numQueriesSubmitted = numQueriesSubmitted + 1
	val current = numQueriesSubmittedPerQueue.getOrElse(
		    event.query.getQueueID(), 0L)
	numQueriesSubmittedPerQueue(event.query.getQueueID()) = current + 1

    queryCacheSize(event.query) = event.cacheUsed
	if(event.cacheUsed > 0) {
		val currentNum = numQueriesCachedPerQueue.getOrElse(
		    event.query.getQueueID(), 0L)
		numQueriesCachedPerQueue(event.query.getQueueID()) = currentNum + 1
	}

    if (event.query.isInstanceOf[SingleDatasetQuery]) {
      val query = event.query.asInstanceOf[SingleDatasetQuery]
      val count = queriesPerDataset.getOrElse(query.getDataset.getName, 0L)
      queriesPerDataset(query.getDataset.getName) = count + 1
    } else {
      val count = queriesPerDataset.getOrElse("tpch", 0L)
      queriesPerDataset("tpch") = count + 1
    }
  }

  override def onQueryFinished(event: QueryFinished) {
    timeLastQueryFinished = System.currentTimeMillis()
    val startTime = queryExecTimes(event.query)
    queryExecTimes(event.query) = System.currentTimeMillis() - startTime    
    numQueriesFinished = numQueriesFinished + 1
  }

  override def onDatasetLoadedToCache(event: DatasetLoadedToCache) {
    val count = datasetLoaded.getOrElse(event.dataset.getName, 0L)
    datasetLoaded(event.dataset.getName) = count + 1
    totalCacheLoaded = totalCacheLoaded + event.dataset.getEstimatedSize()
  }

  override def onDatasetRetainedInCache(event: DatasetRetainedInCache) {
    val count = datasetRetained.getOrElse(event.dataset.getName, 0L)
    datasetRetained(event.dataset.getName) = count + 1
    totalCacheLoaded = totalCacheLoaded + event.dataset.getEstimatedSize()
  }

  override def onDatasetUnloadedFromCache(event: DatasetUnloadedFromCache) {
    val count = datasetUnloaded.getOrElse(event.dataset.getName, 0L)
    datasetUnloaded(event.dataset.getName) = count + 1
  }

  def getTotalWaitTime(): Long = {
    var totalTime = 0L
    queryWaitTimes.foreach(t => totalTime += t._2)
    return totalTime
  }

  def getTotalExecTime(): Long = {
    var totalTime = 0L
    queryExecTimes.foreach(t => totalTime += t._2)
    return totalTime
  }

  // return (queueID, queryID, queryString, waitTime, schedulingDelay, execTime, cacheSize)
  def getFormattedQueryExecTimes: List[(Int, Int, String, Long, Long, Long, Long)] = {
    val allTimes = queryWaitTimes.toMap.mapValues{List(_)} |+| 
      querySchedulingDelays.toMap.mapValues{List(_)} |+|  
      queryExecTimes.toMap.mapValues{List(_)} |+| 
      queryCacheSize.toMap.mapValues{_.asInstanceOf[Number].longValue}.mapValues{List(_)}
println("---map: \n" + allTimes)
    allTimes.toList.map {t => (t._1.getQueueID, t._1.getQueryID, 
        t._1.toHiveQL(false), t._2(0), t._2(1), t._2(2), t._2(3))} sortBy {t => (t._1, t._2)}
  }

  def getMeanExecTimePerQueue: List[(Int, Long)] = {
    var totalExecTimes = scala.collection.mutable.Map[Int, Long]()
    queryExecTimes.foreach(q => {
      val queue = q._1.getQueueID;
      val time = totalExecTimes.getOrElse(queue, 0L);
      totalExecTimes(queue) = time + q._2
    })
    totalExecTimes map {t => 
      (t._1, t._2 / numQueriesSubmittedPerQueue(t._1))} toList
  }

  def getWaitTimePerQueue(): scala.collection.mutable.Map[Int, Long] = {
    var waitTimes = scala.collection.mutable.Map[Int, Long]()
    queryWaitTimes.foreach(q => {
      val queue = q._1.getQueueID;
      val time = waitTimes.getOrElse(queue, 0L);
      waitTimes(queue) = time + q._2
    })
    waitTimes
  }

  def getWaitTimeFairnessIndex(): Double = {
    var runningSum = 0d;
    var runningSumSquares = 0d;
    val waitTimes = getWaitTimePerQueue()
    for(queue <- queues) {
      val waitByWeight = waitTimes(queue.getId) / queue.getWeight
      runningSum += waitByWeight
      runningSumSquares += waitByWeight * waitByWeight
    }
    (runningSum * runningSum) / (queues.size() * runningSumSquares)
  }

  def getTotalCacheUsed(queueId: Int): Double = {
    var totalCache = 0d
    queryCacheSize.foreach(t => if(t._1.getQueueID == queueId) {
      totalCache += t._2
    })
    totalCache    
  }

  /**
   * Enumerate over all queries of the given queueId.
   * Add up cache share of each query. 
   * Cache share = (size in cache) / (total number of queries requesting the same view)
   */
  def getTotalCacheShareUsed(queueId: Int): Double = {
    var totalCacheShare = 0d
    queryCacheSize.foreach(t => if(t._1.getQueueID == queueId) {
      if(t._1.isInstanceOf[SingleDatasetQuery]) {
        totalCacheShare += t._2 / queriesPerDataset(
          t._1.asInstanceOf[SingleDatasetQuery].getDataset.getName)
      } else {
        totalCacheShare += t._2 / queriesPerDataset("tpch")
      }
    })
    totalCacheShare
  }

  def getTimeOfWorkload(): Long = {
    timeLastQueryFinished - timeFirstQueryGenerated
  }

  def getThroughput(): Double = {
    numQueriesGenerated.doubleValue * 1000 / getTimeOfWorkload.doubleValue
  }

  def getResourceFairnessIndex(): Double = {
    var runningSum = 0d;
    var runningSumSquares = 0d;
    for(queue <- queues) {
      val cacheByWeight = getTotalCacheUsed(queue.getId) / queue.getWeight
      runningSum += cacheByWeight
      runningSumSquares += cacheByWeight * cacheByWeight
    }
    (runningSum * runningSum) / (queues.size() * runningSumSquares)    
  }

  /**
   * compute core fairness index.
   */
  def getCoreFairnessIndex(): Double = {
    var runningSum = 0d;
    var runningSumSquares = 0d;
    for(queue <- queues) {
      val cacheByWeight = getTotalCacheShareUsed(queue.getId) / queue.getWeight
      runningSum += cacheByWeight
      runningSumSquares += cacheByWeight * cacheByWeight
    }
    (runningSum * runningSum) / (queues.size() * runningSumSquares)
  }

  def getDatasetLoadHistogram(): List[(String, Long)] = {
    datasetLoaded.toList sortBy {-_._2}
  }

  def getDatasetRetainHistogram(): List[(String, Long)] = {
    datasetRetained.toList sortBy {-_._2}
  }

  def getDatasetUnloadHistogram(): List[(String, Long)] = {
    datasetUnloaded.toList sortBy {-_._2}
  }

  def getNumQueriesGenerated(): Long = {
    numQueriesGenerated
  }

  def getNumQueriesFetched(): Long = {
    numQueriesFetched
  }

  def getNumQueriesSubmitted(): Long = {
    numQueriesSubmitted
  }

  def getNumQueriesFinished(): Long = {
    numQueriesFinished
  }

  def getNumQueriesCached(): List[(Int, Long)] = {
    numQueriesCachedPerQueue.toList
  }

  def getFractionQueriesCached(): List[(Int, Double)] = {
    numQueriesCachedPerQueue map {
      t => t._1 -> t._2.doubleValue / numQueriesSubmittedPerQueue.getOrElse(t._1, t._2)
    } toList
  }

  def getTotalCacheLoaded(): Double = { 
    totalCacheLoaded
  }
}
