package edu.duke.cacheplanner.listener

/**
 * an example of concrete listener that handles events
 */
class LoggingListener extends Listener {

  //implement the event handler
  override def onQueryGenerated(event: QueryGenerated) { 
    System.out.println("query generated(queryID, queueID) : " + event.queryID + ", " + event.queueID)
  }
  
  override def onQueryFetchedByCachePlanner(event: QueryFetchedByCachePlanner) { 
    System.out.println("query fetched by CachePlanner(queryID, queueID) : " + event.queryID + ", " + event.queueID)
  }
  
  override def onQueryPushedToSharkScheduler(event: QueryPushedToSharkScheduler) { 
    System.out.println("query pushed to SharkScheduler(queryID, queueID) : " + event.queryID + ", " + event.queueID)
  }  
}