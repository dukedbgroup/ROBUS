package edu.duke.cacheplanner.algorithm

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.QueryPushedToSharkScheduler
import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.query.SingleTableQuery
import edu.duke.cacheplanner.data.{Column, Dataset}

import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import org.apache.http.util.ByteArrayBuffer

class OnlineCachePlanner(setup: Boolean, manager: ListenerManager, queues: java.util.List[ExternalQueue], data: java.util.List[Dataset], time: Long)
  extends AbstractCachePlanner(setup, manager, queues, data) {

  val batchTime = time;
  var cachedData : scala.collection.mutable.Map[String, ArrayBuffer[Column]] = new HashMap[String, ArrayBuffer[Column]]()

  override def initPlannerThread(): Thread = {
    new Thread("ListenerManager") {
      setDaemon(true)

      override def run() {
        while (true) {
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
            var batch:java.util.List[SingleTableQuery] = new ArrayList()
            for (queue <- externalQueues.asInstanceOf[List[ExternalQueue]]) {
              batch.addAll(queue.fetchABatch().
                  asInstanceOf[java.util.List[SingleTableQuery]])
            }
            // analyze the batch to find columns to cache
            // TODO: use previously cached columns to influence the choice
            val colsToCache : List[Column] = SingleColumnBatchAnalyzer.analyzeGreedily(
                batch, 1000) //get the right memory size

            //merging candidate columns if they are in the same table
            var cacheCandidate : Map[String, ArrayBuffer[Column]] = new HashMap[String, ArrayBuffer[Column]]()
            var cacheDropCandidate : ArrayBuffer[String] = new ArrayBuffer[String]()
            for (col: Column <- colsToCache) {
              if(cacheCandidate(col.getDatasetName) == null) {
                val buffer = new ArrayBuffer[Column]()
                buffer.append(col)
                cacheCandidate(col.getDatasetName) = buffer
              }
              else {
                cacheCandidate(col.getDatasetName).append(col)
              }
            }

            val next_cached = cacheCandidate.clone

            //check whether they are already cached in the same format
            for(datasetName <- cacheCandidate.keySet) {
              //check if the table is already cached
              if(cachedData(datasetName) != null) {
                //check the # of columns
                val cached_set = cachedData(datasetName).toSet
                val candidate_set = cacheCandidate(datasetName).toSet
                if(cached_set.equals(candidate_set)) {
                  cacheCandidate.remove(datasetName)
                }
                else {
                  cacheDropCandidate.append(datasetNAme)
                }
              }
            }

            cachedData = next_cached


            // fire queries to drop the cache


            // fire queries to cache columns


            cachedData = cacheCandidate
            // fire other queries


          }
          else {
            //single app mode
          }
        }
      }
    }
  }

}