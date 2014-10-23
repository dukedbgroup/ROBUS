/**
 *
 */
package edu.duke.cacheplanner.algorithm

import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import edu.duke.cacheplanner.algorithm.singleds.MMFBatchAnalyzer
import edu.duke.cacheplanner.algorithm.singleds.PFBatchAnalyzer
import edu.duke.cacheplanner.conf.ConfigManager
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.data.QueryDistribution
import edu.duke.cacheplanner.listener._
import edu.duke.cacheplanner.query.QueryUtil
import edu.duke.cacheplanner.query.SingleDatasetQuery
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.algorithm.singleds.AbstractSingleDSBatchAnalyzer

/**
 * @author mayuresh
 *
 */
class OnlineCachePlannerSingleDS(setup: Boolean, manager: ListenerManager, 
    queues: java.util.List[ExternalQueue], data: java.util.List[Dataset], 
    distribution: QueryDistribution, config: ConfigManager) extends 
    AbstractCachePlanner(setup, manager, queues, data, config) {

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
          scheduleBatch(batch, cachedDatasets, datasetsToCache)
          cachedDatasets = datasetsToCache
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
            scala.collection.mutable.ListBuffer[SingleDatasetQuery]()
          batch.foreach(t => if(t.getQueueID == luckyQueue) {
            filteredBatch.add(t)
          })
          // HACK: if luckyQueue has no queries, maintain the cache state
          // Ideally, luckyQueue should be picked only from queues having queries.
          val datasetsToCache = if(filteredBatch.size > 0) {
            runAlgorithm(filteredBatch.toList, cachedDatasets, cacheSize)
          } else { cachedDatasets }
          scheduleBatch(batch, cachedDatasets, datasetsToCache)
          cachedDatasets = datasetsToCache
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
            scala.collection.mutable.Map[Int, scala.collection.mutable.ListBuffer[SingleDatasetQuery]]()
          batch.foreach(q => {
            val queue = q.getQueueID;
            val current = batchPerQueue.getOrElse(queue,
                scala.collection.mutable.ListBuffer[SingleDatasetQuery]());
            current.add(q)
            batchPerQueue(queue) = current
          })
          
          batchPerQueue.foreach(q => {
            val datasetsToCache = runAlgorithm(q._2.toList, 
                cachedDatasetsPerQueue(q._1), cachePerQueue(q._1))
            scheduleBatch(q._2.toList, cachedDatasetsPerQueue(q._1), 
                datasetsToCache)
            cachedDatasetsPerQueue(q._1) = datasetsToCache            
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
      val pool:ExecutorService = Executors.newCachedThreadPool();

      def buildAlgo(): AbstractSingleDSBatchAnalyzer = {
        if(config.getAlgorithmName().equals("MMF")) {
          return new MMFBatchAnalyzer(data)
        } else {
          return new PFBatchAnalyzer(data)
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
      def fetchNextBatch(): List[SingleDatasetQuery] = {
            var batch = 
              scala.collection.mutable.ListBuffer[SingleDatasetQuery]()
            for (queue <- externalQueues.toList) {
              queue.fetchABatch().toList.foreach(
                  q =>  {
                    batch += q.asInstanceOf[SingleDatasetQuery]
                  })
            }
            return batch.toList
      }

      /**
       * Runs algorithm on given batch with a list of datasets already in cache, 
       * and a given cache size.
       * Returns new allocation i.e. a list of datasets to be cached
       */
      def runAlgorithm(batch: List[SingleDatasetQuery], 
          cachedDatasets: List[Dataset], cacheSize: Double): List[Dataset] = {
            val javaBatch: java.util.List[SingleDatasetQuery] = batch
            val javaCachedDatasets: java.util.List[Dataset] = cachedDatasets
            val datasetsToCache : List[Dataset] = algo.analyzeBatch(
                javaBatch, javaCachedDatasets, cacheSize).toList      
            datasetsToCache
      }

      /**
       * Schedules a batch of queries. A list of datasets already in cache and 
       * a list of datasets that should be in cache is given. 
       * It first changes the state of cache and then runs the queries. 
       */
      def scheduleBatch(batch: List[SingleDatasetQuery], 
          cachedDatasets: List[Dataset], datasetsToCache: List[Dataset]) = {
            println("cached from previous")
            cachedDatasets.foreach(c => println(c.getName()))
                
            println("datasets to cache from algorithm:")
            datasetsToCache.foreach(c=> println(c.getName()))
            
            //initialize drop & cache candidates to fire the query
            var dropCandidate : ListBuffer[Dataset] = new ListBuffer[Dataset]()
            cachedDatasets.foreach(c => dropCandidate += c)
            var cacheCandidate : ListBuffer[Dataset] = new ListBuffer[Dataset]()
            datasetsToCache.foreach(c => cacheCandidate += c)
            
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
            
            println("cache candidate:")
            cacheCandidate.foreach(c => println(c.getName()))
            println("drop candidate:")
            dropCandidate.foreach(c=> println(c.getName()))
            
            // fire queries to drop the cache
            for(ds <- dropCandidate) {
              try {
                hiveContext.hql("UNCACHE TABLE " + ds.getCachedName())
              } catch {
                case e: Exception => println("If is is physical partitioning case, someone else may have uncached already!"); 
                e.printStackTrace()
              }
//              hiveContext.hql(QueryUtil.getDropTableSQL(ds.getCachedName()))

              manager.postEvent(new DatasetUnloadedFromCache(ds))
            }

            // fire queries to cache columns
            for(ds <- cacheCandidate) {
//              var drop_cache_table = QueryUtil.getDropTableSQL(
//                  ds.getCachedName())
//              hiveContext.hql(drop_cache_table)
//
//              val queryString = QueryUtil.getCreateTableAsCachedSQL(ds)
//              try {
//           	    hiveContext.hql(queryString)
//              } catch{
//                case e: Exception => 
//                 println("not able to create table. "); e.printStackTrace()
//              }

              try {
                hiveContext.hql("CACHE TABLE " + ds.getCachedName())	// not cached at this stage since spark evaluates lazily
              } catch {
                case e: Exception => println("If is is physical partitioning case, someone else may have cached already!"); 
                e.printStackTrace()
              }
//              new CacheThread(ds).start()
                manager.postEvent(new DatasetLoadedToCache(ds))

            }
            
            // fire sql queries
            for(query <- batch) {
              var queryString: String = ""
              var cacheUsed: Double = 0
              if(datasetsToCache.contains(query.getDataset())) {	//datasetsToCache are already cached at this time
                println("use cache table: " + query.getDataset())
                queryString = query.toHiveQL(true)
                cacheUsed = query.getScanBenefit()
              } else {
                println("use external table: " + query.getDataset())
                queryString = query.toHiveQL(false)
              }
              println("query fired: " + queryString)

              // submit query to spark through a thread
              pool.execute(new ExecutorThread(query.getQueueID().toString, 
                  queryString, query))

              manager.postEvent(new QueryPushedToSparkScheduler(query, cacheUsed))
            }

      }
      
      override def run() {
        while (true) {
          println("single ds cacheplanner invoked")
          try { 
        	  Thread.sleep(batchTime * 1000)
          } catch {
            case e:InterruptedException => e.printStackTrace
          }

          if (!started) {
            // before returning schedule remaining queries
            try {
            	setup.run()
            } catch { case e: Exception => {
                // now there are no more queries
            	e.printStackTrace();
            	// wait for all executor threads to finish
            	pool.shutdown;
            	pool.awaitTermination(10, java.util.concurrent.TimeUnit.MINUTES);
            	return
            }}
          } else {
            try {
        	  setup.run()
            } catch { case e: Exception => {
              e.printStackTrace()
               println("not returning")
          }}}
        }
      }

      /**
       * Not being used
       */
      class CacheThread(ds: Dataset) extends java.lang.Thread {

        override def run() {
          // pick a queue at random
          // TODO: use weights of queues
/*          val queueString = queues.get(
              (Math.random() * queues.size()).intValue).getId().toString
*/
          // scraping above as it leads to concurrent modification exception in hive metastore
          val queueString = "default"

          val queryString = QueryUtil.getCreateTableAsCachedSQL(ds)
          sc.setJobGroup(queueString, queryString)
          sc.setLocalProperty("spark.scheduler.pool", queueString)

          try {
           	  hiveContext.hql(queryString)
          } catch{
              case e: Exception => 
              println("not able to create table. "); e.printStackTrace()
          }
          hiveContext.hql("CACHE TABLE " + ds.getCachedName())	// not cached at this stage since spark evaluates lazily
          
        }

      }

      class ExecutorThread(queueString: String, queryString: String, 
          query: SingleDatasetQuery) extends java.lang.Runnable {
        
        override def run() {
              sc.setJobGroup(queueString, queryString)
              sc.setLocalProperty("spark.scheduler.pool", queueString)
              try {
                val result = hiveContext.hql(queryString)
                result.collect()
              } catch {
                case e: Exception => 
              } finally{
                // hopefully, this is called after query is finished
                manager.postEvent(new QueryFinished(query))                
              }
        }

      }

    }
  }
}

