/**
 *
 */
package edu.duke.cacheplanner.algorithm

import com.google.gson.Gson
import java.util.concurrent._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import edu.duke.cacheplanner.Submit
import edu.duke.cacheplanner.algorithm.singleds.MMFBatchAnalyzer
import edu.duke.cacheplanner.algorithm.singleds.PFBatchAnalyzer
import edu.duke.cacheplanner.conf.ConfigManager
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.data.QueryDistribution
import edu.duke.cacheplanner.listener._
import edu.duke.cacheplanner.query.QueryUtil
import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.query.CacheQ
import edu.duke.cacheplanner.query.Constants
import edu.duke.cacheplanner.query.SingleDatasetQuery
import edu.duke.cacheplanner.query.SubmitQuery
import edu.duke.cacheplanner.query.TPCHQuery
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.algorithm.singleds.AbstractSingleDSBatchAnalyzer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler._

/**
 * @author mayuresh
 *
 */
class OnlineCachePlannerSingleDS(setup: Boolean, manager: ListenerManager, 
    queues: java.util.List[ExternalQueue], data: java.util.List[Dataset], 
    tpchData: java.util.List[Dataset], 
    distribution: QueryDistribution, config: ConfigManager) extends 
    AbstractCachePlanner(setup, manager, queues, data, tpchData, config) {

  val batchTime = config.getPlannerBatchTime()

  override def initPlannerThread(): Thread = {
    new Thread("OnlineCachePlanner") {

      /**
       * Multiple setups of algorithms are captured by this interface
       */
      trait CachePartitionSetup {
        def init() = {}	//any initialization on algorithm and other data structures
        def run() = {}	//called on each invocation See @run of Thread
      }

      /**
       * When we want a performance optimal subject to fairness
       */
      class FairShareSetup extends CachePartitionSetup {
        
        var cachedDatasets = List[Dataset]()
        var cacheSize = config.getCacheSize().doubleValue()

        override def init() = {}

        override def run() = {
          val batch = fetchNextBatch
          if(batch == null || batch.size == 0) {
            throw new Exception("No more queries remained to process.")
          }
          val datasetsToCache = runAlgorithm(batch, cachedDatasets, cacheSize)
          cachedDatasets = scheduleBatch(batch, cachedDatasets, datasetsToCache)
        }

      }
      
      /**
       * When we want a performance optimal solution
       */
      class UnfairShareSetup extends FairShareSetup {

        override def init() = {
          super.init
          algo.setSingleTenant(true)
        }

      }

      /**
       * When we partition the cache among all tenants probabilistically
       * i.e. a tenant gets to own the entire batch with certain probability
       */
      class ProbabilisticPartitionSetup extends CachePartitionSetup {

        var cachedDatasets = List[Dataset]()
        var cacheSize = config.getCacheSize().doubleValue()
        var queueProbability = scala.collection.mutable.Map[Int, Double]()

        override def init() = {
          var totalWeight = 0
          queues.foreach(q => totalWeight = totalWeight + q.getWeight)
          queues.foreach(q => queueProbability(q.getId) = (q.getWeight.doubleValue / totalWeight))
        }

        override def run() = {
          // pick a queue at random to favor
          val rnd = Math.random 
          var cumulative = 0d
          var luckyQueue = 1
          val loop = new scala.util.control.Breaks
          loop.breakable {
            for (t <- queueProbability.toList) {
              cumulative += t._2 
              if(rnd < cumulative) {
	        luckyQueue = t._1
	        loop.break
              }
            }
          }

          // run algo only on queries from luckyQueue, but schedule all queries
          val batch = fetchNextBatch
          if(batch == null || batch.size == 0) {
            throw new Exception("No more queries remained to process.")
          }
          var filteredBatch = 
            scala.collection.mutable.ListBuffer[AbstractQuery]()
          batch.foreach(t => if(t.getQueueID == luckyQueue) {
            filteredBatch.add(t)
          })
          // HACK: if luckyQueue has no queries, maintain the cache state
          // Ideally, luckyQueue should be picked only from queues having queries.
          val datasetsToCache = if(filteredBatch.size > 0) {
            runAlgorithm(filteredBatch.toList, cachedDatasets, cacheSize)
          } else { cachedDatasets }
          cachedDatasets = scheduleBatch(batch, cachedDatasets, datasetsToCache)
        }
      }

      /**
       * When we partition the cache among all tenants
       */
      class PhysicalPartitionSetup extends ProbabilisticPartitionSetup {
        
        var cachePerQueue = scala.collection.mutable.Map[Int, Double]()
        var cachedDatasetsPerQueue = 
          scala.collection.mutable.Map[Int, List[Dataset]]()

        override def init() = {
          super.init()
          queueProbability.foreach(t => {
            cachePerQueue(t._1) = t._2 * cacheSize
            cachedDatasetsPerQueue(t._1) = List[Dataset]()
          })
        }

        override def run() = {
          val batch = fetchNextBatch
          if(batch == null || batch.size == 0) {
            throw new Exception("No more queries remained to process.")
          }
          var batchPerQueue = 
            scala.collection.mutable.Map[Int, scala.collection.mutable.ListBuffer[AbstractQuery]]()
          batch.foreach(q => {
            val queue = q.getQueueID;
            val current = batchPerQueue.getOrElse(queue,
                scala.collection.mutable.ListBuffer[AbstractQuery]());
            current.add(q)
            batchPerQueue(queue) = current
          })
          
          batchPerQueue.foreach(q => {
            val datasetsToCache = runAlgorithm(q._2.toList, 
                cachedDatasetsPerQueue(q._1), cachePerQueue(q._1))
            cachedDatasetsPerQueue(q._1) = scheduleBatch(q._2.toList, cachedDatasetsPerQueue(q._1), 
                datasetsToCache)
          })
        }
        
      }

      /**
       * When we want to cache only once 
       */
      class CacheOnceGreedySetup extends CachePartitionSetup {
        
        var datasetsToCache = scala.collection.mutable.ListBuffer[Dataset]()
        var cacheSize = config.getCacheSize().doubleValue()

        var firstRun = true

        def findDataset(name: String): Dataset = {
          var ds = data.get(0)
          data.foreach(d => if(d.getName.equals(name)) {ds = d})
          ds
        }

        override def init() = {
          var dataProb = scala.collection.mutable.Map[String, Double]()
          for(queue <- queues) {
        	  val dataDistri = distribution.getQueueDistributionMap(queue.getId)
        	  dataDistri.foreach(d => {
        	    val prob = d._2.getDataProb;
        	    val current = dataProb.getOrElse(d._1, 0d);
        	    dataProb(d._1) = current + queue.getWeight * prob
        	  })
          }
          val sortedProb: List[(String, Double)] = dataProb.toList.sortBy {-_._2}

          var remainingCache = cacheSize
          sortedProb.foreach(s => {
            val ds = findDataset(s._1);
            if(ds.getEstimatedSize <= remainingCache) {
              datasetsToCache.add(ds)
              remainingCache = remainingCache - ds.getEstimatedSize
            }
          })

          firstRun = true
        }

        override def run() = {
          val batch = fetchNextBatch
          if(batch == null || batch.size == 0) {
            throw new Exception("No more queries remained to process.")
          }

          if(firstRun) {
        	scheduleBatch(batch, List[Dataset](), datasetsToCache.toList)
        	firstRun = false
          } else {
            scheduleBatch(batch, datasetsToCache.toList, 
                datasetsToCache.toList)
          }
        }
        
      }

      /**
       * When we don't want to use the cache
       * Baseline case
       */
      class NoCacheSetup extends CacheOnceGreedySetup {

        override def init() = {}

      }

      /**
       * We cache for every query relying on LRU for evictions
       * Represents the case where we honour every cache directive
       */
      class GreedyCacheSetup extends CacheOnceGreedySetup {

        override def init() = {
          for(ds <- data) {
            if(ds.getEstimatedSize < cacheSize)
            {
              datasetsToCache.add(ds)
            }
          }
          for(ds <- tpchData) {
            if(ds.getEstimatedSize < cacheSize)
            {
              datasetsToCache.add(ds)
            }
          }
        }

      }

      /**
       * Algorithm specifications follow.
       */
      val algo = buildAlgo
      if(config.getCacheState().equals("warm")) {
        algo.setWarmCache(true)
      }

      val setup = buildSetup
      setup.init()

      /**
       * thread pool for query execution threads
       */
      val pool = new ThreadPoolExecutor(externalQueues.length, externalQueues.length, 0L, TimeUnit.MILLISECONDS,
                                        new LinkedBlockingQueue[Runnable](), 
        new RejectedExecutionHandler() {
            @Override
            def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
                // TODO: marking query as finished even when it has failed due to concurrent thread being rejected
                println("Caught rejected execution exception")
                val thread = r.asInstanceOf[ExecutorThread]
                manager.postEvent(new QueryFinished(thread.getQuery))
                s.release
                s_batch.release
            }
        })
      var numExecutors: Int = 0
      var numExecPerBatch: Int = 0
      val s = new java.util.concurrent.Semaphore(0)
      var s_batch = new java.util.concurrent.Semaphore(0)
      var cacheQCounter: Int = 0 //number of cache queries
      var QCounter: Int = 0 //total number of queries including cache

      def buildAlgo(): AbstractSingleDSBatchAnalyzer = {
        if(config.getAlgorithmName().equals("MMF")) {
          return new MMFBatchAnalyzer(data, tpchData)
        } else {
          return new PFBatchAnalyzer(data, tpchData)
        }
      }

      def buildSetup: CachePartitionSetup = {
        if(config.getAlgorithmMode().equals("online")) {
          val confValue = config.getCachePartitioningStrategy()
          if(confValue.equals("shareFairly")) {
            new FairShareSetup()
          } else if(confValue.equals("shareUnfairly")) {
            new UnfairShareSetup()
          } else if(confValue.equals("partitionProbabilistically")) {
            new ProbabilisticPartitionSetup()
          } else if(confValue.equals("partitionPhysically")) {
            new PhysicalPartitionSetup()
          } else if(confValue.equals("greedy")) {
            new GreedyCacheSetup()
          } else {
            new FairShareSetup()
          }
        } else {
          //HACK: overloading this class with offline algorithms as well
          val useCache = config.getUseCache()
          if(useCache) {
            new CacheOnceGreedySetup()
          } else {
            new NoCacheSetup()
          }
        }
      }

      /**
       * Returns next batch compiled from all queues
       */
      def fetchNextBatch(): List[AbstractQuery] = {
            var batch = 
              scala.collection.mutable.ListBuffer[AbstractQuery]()
            for (queue <- externalQueues.toList) {
              queue.fetchABatch().toList.foreach(
                  q =>  {
                    batch += q
                  })
            }
            return batch.toList 
      }

      /**
       * Runs algorithm on given batch with a list of datasets already in cache, 
       * and a given cache size.
       * Returns new allocation i.e. a list of datasets to be cached
       */
      def runAlgorithm(batch: List[AbstractQuery], 
          cachedDatasets: List[Dataset], cacheSize: Double): List[Dataset] = {
            val javaBatch: java.util.List[AbstractQuery] = batch
            val javaCachedDatasets: java.util.List[Dataset] = cachedDatasets
            val datasetsToCache : List[Dataset] = algo.analyzeBatch(
                javaBatch, javaCachedDatasets, cacheSize).toList      
            datasetsToCache
      }

      /**
       * Schedules a batch of queries. A list of datasets already in cache and 
       * a list of datasets that should be in cache is given. 
       * It first changes the state of cache and then runs the queries. 
       * Returns list of datasets cached in the batch
       */
      def scheduleBatch(batch: List[AbstractQuery], 
          cachedDatasets: List[Dataset], datasetsToCache: List[Dataset]): List[Dataset] = {
            println("cached from previous")
            cachedDatasets.foreach(c => println(c.getName()))
                
            println("datasets to cache from algorithm:")
            datasetsToCache.foreach(c=> println(c.getName()))
            
            // initialize drop & cache candidates to fire the query
            val dropCandidate : ListBuffer[Dataset] = new ListBuffer[Dataset]()
            cachedDatasets.foreach(c => dropCandidate += c)
            val cacheCandidate : ListBuffer[Dataset] = new ListBuffer[Dataset]()
            val finalCacheConfig: ListBuffer[Dataset] = new ListBuffer[Dataset]()     
            datasetsToCache.foreach(c => {cacheCandidate += c; finalCacheConfig += c})
       
            for (cache: Dataset <- cacheCandidate) {
              var matching = false
              var droppingCol: Dataset = null
              for (drop: Dataset <- dropCandidate) {
                if(cache.equals(drop)) {
                  matching = true
                  droppingCol = drop
                }
              }
              if(matching) {
                cacheCandidate -= cache
                dropCandidate -= droppingCol
                
                manager.postEvent(new DatasetRetainedInCache(cache))
                
              }
            }

            // if there is nothing to cache, no need to drop anything
            if(cacheCandidate.length == 0) {
              for (drop: Dataset <- dropCandidate) {
                dropCandidate -= drop
                finalCacheConfig += drop
                manager.postEvent(new DatasetRetainedInCache(drop))
              }
            }
            
            println("cache candidate:")
            cacheCandidate.foreach(c => println(c.getName()))
            println("drop candidate:")
            dropCandidate.foreach(c=> println(c.getName()))
            
            // We never drop a dataset except when a cache candidate seen as an uncached dataset is actually in cache from previous batch. These candidates will be brought in cache afresh immediately 
//            if(cacheCandidate.length > 0) {
//              try {
//                pool.execute(new CacheThread(cacheCandidate, true))
//              } catch {
//                case e: Exception => e.printStackTrace
//              }
//            }

            for(ds <- dropCandidate) {
              CacheQ.uncacheDataframe(ds.getName) // This has an immediate impact while the past batches might still be queued which are dependent on the cached data
              manager.postEvent(new DatasetUnloadedFromCache(ds))
            }

            // fire queries to cache columns
//            if(cacheCandidate.length > 0) {
//              cacheData(cacheCandidate)
//            }

            // reorder queries in the batch
            val newBatch = new java.util.ArrayList[AbstractQuery]

            // reordering queries efficiently using a comparator based on query ID
            newBatch.addAll(batch)
            java.util.Collections.sort(newBatch, new java.util.Comparator[AbstractQuery]() {
              @Override
              def compare(o1: AbstractQuery, o2: AbstractQuery): Int = {
                o1.getQueryID.compareTo(o2.getQueryID)
              }
            })

            // submit cache statements along side queries not using this cache first
            if(cacheCandidate.length > 0) {
              java.util.Collections.sort(newBatch, new java.util.Comparator[AbstractQuery]() {
                @Override
                def compare(o1: AbstractQuery, o2: AbstractQuery): Int = {
                  if((o1.isInstanceOf[TPCHQuery] && !cacheCandidate.contains(tpchData.get(0))) || 
                  	(o1.isInstanceOf[SingleDatasetQuery] && !cacheCandidate.contains(o1.asInstanceOf[SingleDatasetQuery].getDataset))) {
                    -1
                  } 
                  if((o2.isInstanceOf[TPCHQuery] && !cacheCandidate.contains(tpchData.get(0))) ||
                        (o2.isInstanceOf[SingleDatasetQuery] && !cacheCandidate.contains(o2.asInstanceOf[SingleDatasetQuery].getDataset))) {
                    1
                  }
                  0
                }
              })

              for(ds <- cacheCandidate) {
                try {
                  pool.execute(new CacheThread(sparkContext, ds, (QCounter % queues.length) + 1, false))
                  cacheQCounter = cacheQCounter + 1
                  QCounter = QCounter + 1
                } catch {
                  case e: Exception => e.printStackTrace
                }
                //CacheQ.cacheDataframe(ds.getName) // This has an immediate impact while the past batches might still be queued which are dependent on the cached data
                manager.postEvent(new DatasetLoadedToCache(ds))
              }
            }

            // fire sql queries
            for(query <- newBatch) {
              var cacheUsed: Double = 0
              val toCacheForQuery = new java.util.ArrayList[Dataset]()
              if(query.isInstanceOf[TPCHQuery]) {
                if(datasetsToCache.contains(tpchData.get(0))) {
                  for(i <- 0 to tpchData.size()-1) {
                    toCacheForQuery.add(tpchData.get(i))
                    cacheUsed += tpchData.get(i).getEstimatedSize()
                  }
                } else {
                  println("use external table: tpch")
                }
              } else {
                val q = query.asInstanceOf[SingleDatasetQuery]
                if(datasetsToCache.contains(q.getDataset())) {	//datasetsToCache are already cached at this time
                  toCacheForQuery.add(q.getDataset())
                  println("use cache table: " + q.getDataset())
                  cacheUsed = q.getScanBenefit()
                } else {
                  println("use external table: " + q.getDataset())
                }
              }
              println("query fired: " + query.getQueueID + ": " + query.toHiveQL(false))
              numExecutors = numExecutors + 1
              numExecPerBatch = numExecPerBatch + 1

              // submit query to spark through a thread
              val appName = "Queue:" + query.getQueueID + ",Query:" + query.getQueryID + "," + query.getName
              val c = Class.forName(f"edu.duke.cacheplanner.query.${query.getName}").getConstructor(classOf[String], classOf[AbstractQuery], classOf[SparkContext], classOf[java.util.List[Dataset]])
              val submitQuery = c.newInstance(appName, query, sparkContext, toCacheForQuery).asInstanceOf[{ def submit }]

              try {
                pool.execute(new ExecutorThread(query, submitQuery.asInstanceOf[SubmitQuery], sparkContext, (QCounter % queues.length) + 1, cacheUsed))
                QCounter = QCounter + 1
              } catch {
                case e: Exception => e.printStackTrace;
                // TODO: marking query as finished even when it has failed due to concurrent thread being rejected
                manager.postEvent(new QueryFinished(query));
                s.release
                s_batch.release
              }

            }
 
            finalCacheConfig.toList
 
      }
      
      override def run() {
        while (true) {
          try { 
            Thread.sleep(batchTime * 1000)
            s_batch.acquire(numExecPerBatch) // waiting for the current batch to finish
            println("--Batch finished with num queries = " + numExecPerBatch)
          } catch {
            case e:InterruptedException => e.printStackTrace
          }

          if (!started) {
            // before returning schedule remaining queries
            try {
              numExecPerBatch = 0
              s_batch = new java.util.concurrent.Semaphore(0)
              setup.run()
            } catch { case e: Exception => {
                // now there are no more queries
            	e.printStackTrace();
            	// wait for all executor threads to finish
            	//pool.shutdown;
            	//pool.awaitTermination(60, java.util.concurrent.TimeUnit.MINUTES);
                try { s.acquire(numExecutors) } catch { case e:Exception => e.printStackTrace() }
                pool.shutdownNow
                pool.awaitTermination(5, java.util.concurrent.TimeUnit.MINUTES)
                println("--Finished execution. Number of cache statements used: " + cacheQCounter)
            	return
            }}
          } else {
            try {
              numExecPerBatch = 0
              s_batch = new java.util.concurrent.Semaphore(0)
              setup.run()
            } catch { case e: Exception => {
              e.printStackTrace()
          }}}
        }
      }

      def uncacheData(toCache: java.util.List[Dataset]) {
        cacheData(toCache, true)
      }

      def cacheData(toCache: java.util.List[Dataset], uncache: Boolean = false)       {

             try {
               val name = { if(uncache) Constants.UNCACHE_QUERY else Constants.CACHE_QUERY }
               val separator = System.getProperty("file.separator")
               val classpath = System.getenv("SPARK_CLASSPATH") //System.getProperty("java.class.path")
               val path = System.getProperty("java.home") + separator + "bin" + separator + "java"
               val processBuilder = new ProcessBuilder(path, "-cp",
                   classpath,
                   Submit.getClass.getCanonicalName().dropRight(1), name, memoryWorker, totalMaxCores, new Gson().toJson(toCache))
               processBuilder.redirectErrorStream(true)
               processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
               val process = processBuilder.start()
               process.waitFor()

             } catch {
                case e: Exception =>
             } finally{
                // hopefully, this is called after query is finished
                // manager.postEvent(new QueryFinished(query))
             }

      }

      class CacheThread(sc: SparkContext, ds: Dataset, jobPool: Int, uncache: Boolean) extends java.lang.Runnable {

        override def run() {

          sc.setJobGroup(jobPool.toString, "C-" + cacheQCounter + ":" + jobPool + ":" + ds.getName)
          sc.setLocalProperty("spark.scheduler.pool", jobPool.toString)
 
          if(uncache) {
            CacheQ.uncacheDataframe(ds.getName)
          } else {
            CacheQ.cacheDataframe(ds.getName)
          }

        }

      }

      class ExecutorThread(query: AbstractQuery, submitQuery: SubmitQuery, sc: SparkContext, 
           jobPoolID: Int, cacheUsed: Double) extends java.lang.Runnable {

        def getQuery(): AbstractQuery = query
        
        override def run() {

              val qID = query.getQueryID.toString
              val jobPool = jobPoolID.toString
              sc.setJobGroup(jobPool, qID + ":" + query.getQueueID + ":" + query.getName)
              sc.setLocalProperty("spark.scheduler.pool", jobPool)
              sc.setLocalProperty("query.id", qID)

              val listener = new SparkListener() {

                var started = false

                override def onJobStart(jobObject: SparkListenerJobStart) {
                  try {
                    if(!started && qID == jobObject.properties.getProperty("query.id") && jobPool == jobObject.properties.getProperty("spark.scheduler.pool")) {
                      started = true
                      manager.postEvent(new QueryPushedToSparkScheduler(query, cacheUsed))
                    }
                  } catch {
                    case e: Exception => 
                  }
                }
              }
              sc.addSparkListener(listener)

              try {

/*
               val separator = System.getProperty("file.separator")
               val classpath = System.getenv("SPARK_CLASSPATH") //System.getProperty("java.class.path")
               val path = System.getProperty("java.home") + separator + "bin" + separator + "java"
               val processBuilder = new ProcessBuilder(path, "-cp", 
                   classpath, 
                   Submit.getClass.getCanonicalName().dropRight(1), new Gson().toJson(query), memory, maxCores, new Gson().toJson(toCache))
               processBuilder.redirectErrorStream(true)
               processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
               val process = processBuilder.start()
               lockFile("Queue:" + query.getQueueID + ",Query:" + query.getQueryID + "," + query.getName)
               process.waitFor()
*/

               submitQuery.submit

              } catch {
                case e: Exception => e.printStackTrace
              } finally {
                // query has finished 
                manager.postEvent(new QueryFinished(query))
                sc.removeSparkListener(listener)
                s.release
                s_batch.release
              } 
        }
      }
    }
  }
}

