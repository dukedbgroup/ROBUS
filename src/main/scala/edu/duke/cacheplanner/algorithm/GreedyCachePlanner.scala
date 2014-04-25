package edu.duke.cacheplanner.algorithm

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.QueryPushedToSharkScheduler

class GreedyCachePlanner(manager: ListenerManager) extends AbstractCachePlanner(manager) {
  
  override def analyzeBatch() {
    //call this when the planner send a query to spark context
//    listenerManager.postEvent(QueryPushedToSharkScheduler
//        (Integer.parseInt(query.getQueryID()),Integer.parseInt(query.getQueueID())));
  }
}