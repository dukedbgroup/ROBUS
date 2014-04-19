package edu.duke.cacheplanner.listener

/**
 * an example of concrete listener that handles events
 */
class LoggingListener extends Listener {
  
  //implement the event handler
  override def onQueryGenerated(event: QueryGenerated) { }
  
  override def onQueryFetchedByCachePlanner(event: QueryFetchedByCachePlanner) { }
  
  override def onQueryPushedToSharkScheduler(event: QueryPushedToSharkScheduler) { }

  
}