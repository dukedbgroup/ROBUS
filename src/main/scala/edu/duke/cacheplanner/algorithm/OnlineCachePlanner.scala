package edu.duke.cacheplanner.algorithm

import shark.{SharkContext, SharkEnv}
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.QueryPushedToSharkScheduler
import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import edu.duke.cacheplanner.queue.ExternalQueue
import java.util.ArrayList

class OnlineCachePlanner(mode: Boolean, manager: ListenerManager, sharkContext: SharkContext)
  extends AbstractCachePlanner(mode, manager, sharkContext) {

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

          val queues = Factory.externalQueues         
          
          if (isMultipleSetup) {
            // create a batch of queries
            var batch:java.util.List[AbstractQuery] = new ArrayList()
            for (queue <- queues.asInstanceOf[List[ExternalQueue]]) {
              batch.addAll(queue.fetchABatch().
                  asInstanceOf[java.util.List[AbstractQuery]])
            }
            // analyze the batch to find columns to cache
            // TODO: use previously cached columns to influence the choice
            
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

  def analyzeBatch() {
    //call this when the planner send a query to spark context
    //    listenerManager.postEvent(QueryPushedToSharkScheduler
    //        (Integer.parseInt(query.getQueryID()),Integer.parseInt(query.getQueueID())));
  }

}