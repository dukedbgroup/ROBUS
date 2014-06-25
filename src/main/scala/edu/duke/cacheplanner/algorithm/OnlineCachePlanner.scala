package edu.duke.cacheplanner.algorithm

import shark.{SharkContext, SharkEnv}
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.QueryPushedToSharkScheduler
import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import edu.duke.cacheplanner.queue.ExternalQueue
import java.util.ArrayList
import edu.duke.cacheplanner.query.SingleTableQuery
import java.util
import edu.duke.cacheplanner.data.Dataset

class OnlineCachePlanner(setup: Boolean, manager: ListenerManager, queues: util.List[ExternalQueue], data: java.util.List[Dataset])
  extends AbstractCachePlanner(setup, manager, queues, data) {

  //TODO: set it from a configuration
  val batchTime = 1000; //in milliseconds
  
  override def initPlannerThread(): Thread = {
    new Thread("ListenerManager") {
      setDaemon(true)

      override def run() {
        while (true) {
          if (!started) {
            return
          }

          try { 
        	  Thread.sleep(batchTime);
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
            val colsToCache = SingleColumnBatchAnalyzer.analyzeGreedily(
                batch, 1000) //get the right memory size
            
            // fire queries to cache columns
            
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