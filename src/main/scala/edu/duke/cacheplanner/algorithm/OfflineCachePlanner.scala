package edu.duke.cacheplanner.algorithm

import shark.{SharkContext, SharkEnv}

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.QueryPushedToSharkScheduler
import edu.duke.cacheplanner.query.AbstractQuery

class OfflineCachePlanner(mode: Boolean, manager: ListenerManager, sharkContext: SharkContext) 
    extends AbstractCachePlanner(mode, manager, sharkContext) {
  
  //change this to config
  val threshold:Double = 1
  val tenent: Int = 1
  
  override def initPlannerThread(): Thread = {
	  new Thread("ListenerManager") {
	    setDaemon(true)
	    override def run() {
	      //calculate cache candidates
	      var cacheQueries:List[AbstractQuery] = null;
	      
	      if(isMultipleSetup) {
	        cacheQueries = candidateGeneratorMultiple(threshold, tenent)
	      }
	      else {
	        cacheQueries = candidateGeneratorSingle(threshold)
	      }
	      
	      //cache the dataset
	      cacheQueries.foreach((q:AbstractQuery) => sc.runSql(q.toHiveQL(true)))
	      
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
  def candidateGeneratorSingle(threshold: Double) : List[AbstractQuery] = {
    return null
  }
  
  def candidateGeneratorMultiple(threshold: Double, tenent: Integer) : List[AbstractQuery] = {
    return null
  }

}