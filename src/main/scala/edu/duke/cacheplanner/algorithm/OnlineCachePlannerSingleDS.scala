/**
 *
 */
package edu.duke.cacheplanner.algorithm

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import edu.duke.cacheplanner.algorithm.singleds.MMFBatchAnalyzer
import edu.duke.cacheplanner.algorithm.singleds.PFBatchAnalyzer
import edu.duke.cacheplanner.conf.ConfigManager
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.listener.QueryPushedToSparkScheduler
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.query.QueryUtil
import edu.duke.cacheplanner.query.SingleDatasetQuery
import edu.duke.cacheplanner.queue.ExternalQueue
import scala.collection.mutable.ListBuffer

/**
 * @author mayuresh
 *
 */
class OnlineCachePlannerSingleDS(setup: Boolean, manager: ListenerManager, 
    queues: java.util.List[ExternalQueue], data: java.util.List[Dataset], 
    config: ConfigManager) extends AbstractCachePlanner(
        setup, manager, queues, data, config) {

  val batchTime = config.getPlannerBatchTime();
  var cachedDatasets = new scala.collection.mutable.ListBuffer[Dataset]()

  override def initPlannerThread(): Thread = {
    new Thread("OnlineCachePlanner") {

      val algo = new MMFBatchAnalyzer(data)	
      // TODO: read algorithm configuration from config
      // for static mode, set singleUser=true
      // for warm cache, set warmCache=true

      def fetchNextBatch(): 
      scala.collection.mutable.ListBuffer[SingleDatasetQuery] = {
            var batch = 
              scala.collection.mutable.ListBuffer[SingleDatasetQuery]()
            for (queue <- externalQueues.toList) {
              queue.fetchABatch().toList.foreach(
                  q =>  {
                    batch += q.asInstanceOf[SingleDatasetQuery]
                  })
            }
            return batch
      }

      def processBatch(
          batch: scala.collection.mutable.ListBuffer[SingleDatasetQuery]) = {
            val javaBatch: java.util.List[SingleDatasetQuery] = batch
            val javaCachedDatasets: java.util.List[Dataset] = cachedDatasets
            val datasetsToCache : List[Dataset] = algo.analyzeBatch(
                javaBatch, javaCachedDatasets, 
                config.getCacheSize().doubleValue()).toList      
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
              }
            }
            
            println("cache candidate:")
            cacheCandidate.foreach(c => println(c.getName()))
            println("drop candidate:")
            dropCandidate.foreach(c=> println(c.getName()))
            
            cachedDatasets = new ListBuffer[Dataset]()
            datasetsToCache.foreach(c => cachedDatasets += c)

            // fire queries to drop the cache
            for(ds <- dropCandidate) {
              hiveContext.hql("UNCACHE TABLE " + ds.getCachedName())
              hiveContext.hql(QueryUtil.getDropTableSQL(ds.getCachedName()))
            }

            // fire queries to cache columns
            for(ds <- cacheCandidate) {
              var drop_cache_table = QueryUtil.getDropTableSQL(
                  ds.getCachedName())
              var query_create = QueryUtil.getCreateTableAsCachedSQL(ds)
              println("running queries")
              println(drop_cache_table)
              println(query_create)
              hiveContext.hql(drop_cache_table)
              try {
            	  hiveContext.hql(query_create)
              } catch{
                case e: Exception => 
                println("not able to create table. "); e.printStackTrace()
                }
              hiveContext.hql("CACHE TABLE " + ds.getCachedName())	// not cached at this stage since spark evaluates lazily
            }
            
            // fire sql queries
            for(query <- batch) {
              var queryString: String = ""
              var cacheUsed: Double = 0
              if(cachedDatasets.contains(query.getDataset())) {
                println("use cache table: " + query.getDataset())
                queryString = query.toHiveQL(true)
                cacheUsed = query.getScanBenefit()
              } else {
                println("use external table: " + query.getDataset())
                queryString = query.toHiveQL(false)
              }
              println("query fired: " + queryString)
              manager.postEvent(new QueryPushedToSparkScheduler(query, 
                  cacheUsed))
              sc.setJobGroup(query.getQueueID().toString(), queryString)
              sc.setLocalProperty("spark.scheduler.pool", 
                  query.getQueueID().toString())
              val result = hiveContext.hql(queryString)
              result.collect()
            }

      }
      
      override def run() {
        while (true) {
          println("single ds cacheplanner workinggggggggggg")
          if (!started) {
            // before returning schedule remaining queries
            processBatch(fetchNextBatch)
            println("returning because stopped!")
            return
          }

          try { 
        	  Thread.sleep(batchTime * 1000)
          } catch {
            case e:InterruptedException => e.printStackTrace
          }

//          if (isMultipleSetup) {
          // create a batch of queries
          val batch = fetchNextBatch
          // analyze the batch to find columns to cache
          processBatch(batch)
          //wait for all the threads are done

//          }
//          else {
//            //single app mode
//          }
        }
      }
    }
  }
}

