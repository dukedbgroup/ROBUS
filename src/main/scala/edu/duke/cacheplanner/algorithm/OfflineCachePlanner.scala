package edu.duke.cacheplanner.algorithm

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.QueryPushedToSharkScheduler
import edu.duke.cacheplanner.query.AbstractQuery
import java.util
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.data.Dataset

class OfflineCachePlanner(setup: Boolean, manager: ListenerManager, queues: util.List[ExternalQueue], data: java.util.List[Dataset])
    extends AbstractCachePlanner(setup, manager, queues, data) {
  
  //change this to config
  val threshold:Double = 1
  val tenent: Int = 1
  
  override def initPlannerThread(): Thread = {
	  new Thread("ListenerManager") {
	    setDaemon(true)
	    override def run() {
	      //calculate cache candidates
	      var cacheQueries:util.List[AbstractQuery] = null;
	      
	      if(isMultipleSetup) {
	        cacheQueries = candidateGeneratorMultiple(threshold, tenent)
	      }
	      else {
	        cacheQueries = candidateGeneratorSingle(threshold)
	      }
	      
	      //cache the dataset
	      //cacheQueries.foreach((q:AbstractQuery) => sc.runSql(q.toHiveQL(true)))
	      
	      //run requested queries
	      while(true) {
	        if(!started){
	          return
	        }
	        
	        if(isMultipleSetup) {
	          //multiple app mode - run query
	        }
	        else {
	          //single app mode->create thread
	        }
	      }
	    }
	  }
  }
  
  /**
   * used in Offline algorithm. Using a dynamic programming, 
   * find the best candidate for projection
   */
  def candidateGeneratorSingle(threshold: Double) : util.List[AbstractQuery] = {
    return null
  }
  
  def candidateGeneratorMultiple(threshold: Double, tenent: Integer) : util.List[AbstractQuery] = {
    return null
  }

}