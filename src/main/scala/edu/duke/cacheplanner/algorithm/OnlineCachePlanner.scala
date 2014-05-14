package edu.duke.cacheplanner.algorithm

import shark.{SharkContext, SharkEnv}

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.QueryPushedToSharkScheduler
import edu.duke.cacheplanner.query.AbstractQuery

class OnlineCachePlanner(mode: Boolean, manager: ListenerManager, sharkContext: SharkContext) 
    extends AbstractCachePlanner(mode, manager, sharkContext) {
  
   override def initPlannerThread(): Thread = {
	  new Thread("ListenerManager") {
	    setDaemon(true)
	    override def run() {
	      while(true) {
	        if(!started){
	          return
	        }
	        if(setUpMode) {
	          //multi mode
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