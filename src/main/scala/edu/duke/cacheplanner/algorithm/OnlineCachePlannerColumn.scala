package edu.duke.cacheplanner.algorithm

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mutableSeqAsJavaList
import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import edu.duke.cacheplanner.algorithm.singlecolumn.SingleColumnGreedyAnalyzer
import edu.duke.cacheplanner.conf.ConfigManager
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.query.QueryUtil
import edu.duke.cacheplanner.query.SingleDatasetQuery
import edu.duke.cacheplanner.queue.ExternalQueue
import scala.collection.mutable.ListBuffer

class OnlineCachePlannerColumn(setup: Boolean, manager: ListenerManager, 
    queues: java.util.List[ExternalQueue], data: java.util.List[Dataset], 
    config: ConfigManager) extends AbstractCachePlanner(
        setup, manager, queues, data, config) {

  val batchTime = config.getPlannerBatchTime();
  var cachedCols = new scala.collection.mutable.ListBuffer[Column]()

  override def initPlannerThread(): Thread = {
    new Thread("OnlineCachePlanner") {

      override def run() {
        while (true) {
          println("cacheplanner workinggggggggggg")
          if (!started) {
            return
          }

          try { 
        	  Thread.sleep(batchTime)
          } catch {
            case e:InterruptedException => e.printStackTrace
          }

          if (isMultipleSetup) {
            // create a batch of queries
            var batch = scala.collection.mutable.ListBuffer[SingleDatasetQuery]()
            for (queue <- externalQueues.toList) {
              queue.fetchABatch().toList.foreach(
                  q => batch += q.asInstanceOf[SingleDatasetQuery])
            }
            
            // analyze the batch to find columns to cache
            val colsToCache : List[Column] = SingleColumnGreedyAnalyzer.analyzeBatch(
                batch.toList, cachedCols.toList, config.getCacheSize().doubleValue())               
            println("cached from previous")
            cachedCols.foreach(c => println(c.getColName()))
                
            println("cols to cache from algorithm:")
            colsToCache.foreach(c=> println(c.getColName()))
            
            //initialize drop & cache candidates to fire the query
            var dropCandidate : ListBuffer[Column] = new ListBuffer[Column]()
            cachedCols.foreach(c => dropCandidate += c)
            var cacheCandidate : ListBuffer[Column] = new ListBuffer[Column]()
            colsToCache.foreach(c => cacheCandidate += c)
            
            println("initialize drop")
            dropCandidate.foreach(c=> println(c.getColName()))
            println("initialize cache")
            cacheCandidate.foreach(c=> println(c.getColName()))

            
            for (cache: Column <- cacheCandidate) {
              var matching = false
              var droppingCol:Column = null
              for (drop: Column <- dropCandidate) {
                if(cache.getColName().equals(drop.getColName()) && 
                    cache.getDatasetName().equals(drop.getDatasetName())) {
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
            cacheCandidate.foreach(c => println(c.getColName()))
            println("drop candidate:")
            dropCandidate.foreach(c=> println(c.getColName()))
            
            //merging candidate columns if they are in the same table
           // var cacheCandidate : Map[String, ArrayBuffer[Column]] = new HashMap[String, ArrayBuffer[Column]]()
//            var cacheDropCandidate : ArrayBuffer[String] = new ArrayBuffer[String]()
//            for (col: Column <- colsToCache) {            
//              val candidate = cacheCandidate.getOrElse(col.getDatasetName, null)
//              if(candidate == null) {
//                val buffer = new ArrayBuffer[Column]()
//                buffer.append(col)
//                cacheCandidate(col.getDatasetName) = buffer
//              }
//              else {
//                cacheCandidate(col.getDatasetName).append(col)
//              }
//            }
//
//            val next_cached = cacheCandidate.clone
//            
//            //check whether they are already cached in the same format
//            for(datasetName <- cacheCandidate.keySet) {
//              //check if the table is already cached
//              val cached = cachedData.getOrElse(datasetName, null)
//
//              if(cached != null) {
//                //check the columns in dataset
//                val cached_set = cachedData(datasetName).toSet
//                val candidate_set = cacheCandidate(datasetName).toSet
//                if(cached_set.equals(candidate_set)) {
//                  //the candidate is already in cache
//                  cacheCandidate.remove(datasetName)
//                }
//                else {
//                  //need to be dropped
//                  cacheDropCandidate.append(datasetName)
//                }
//              }
//            }
//
//            cachedData = next_cached
            cachedCols = new ListBuffer[Column]()
            colsToCache.foreach(c => cachedCols += c)

            // fire queries to drop the cache
            for(col <- dropCandidate) {
              hiveContext.hql("UNCACHE TABLE " + col.getDatasetName() + "_" + col.getColName())
            }

            // fire queries to cache columns
            for(col <- cacheCandidate) {
              val table_name = col.getDatasetName() + "_" + col.getColName()
              var drop_cache_table = QueryUtil.getDropTableSQL(table_name)
              var query_create = QueryUtil.getCacheTableCreateSQL(col)
              var query_insert = QueryUtil.getCacheTableInsertSQL(col)
              println("running queries")
              println(drop_cache_table)
              println(query_create)
              println(query_insert)
              hiveContext.hql(drop_cache_table)
              hiveContext.hql(query_create)
              hiveContext.hql(query_insert)
              hiveContext.hql("CACHE TABLE " + table_name)	// not cached at this stage since spark evaluates lazily
            }
            
            // fire sql queries
            for(query <- batch.toList) {
              var queryString = ""
              val col = query.getRelevantColumns().asScala.toList(0)
              
              var exist = false
              for(cache <- cachedCols) {
                if(cache.getColName().equals(col.getColName())) {
                  exist = true
                }
              }
              
              if(exist) {
                println("use cache table: " + query.getDataset())
                queryString = query.toHiveQLSingle(true, col)
              }
              else {
                println("use external table: " + query.getDataset())
                queryString = query.toHiveQLSingle(false, col)
              }
              println("query fired: " + queryString)
              sc.setJobGroup(query.getQueueID().toString, queryString)
              sc.setLocalProperty("spark.scheduler.pool", 
                  query.getQueueID().toString())
              val result = hiveContext.hql(query.toHiveQL(false))
              result.collect()
            }
            //wait for all the threads are done

          }
          else {
            //single app mode
          }
        }
      }
    }
  }

}