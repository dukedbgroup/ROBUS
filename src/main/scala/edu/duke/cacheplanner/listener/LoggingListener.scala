package edu.duke.cacheplanner.listener

class LoggingListener extends Listener {
  
  
  override def onQueryGenerated(event: QueryGenerated) { }
  
  override def onQueryFetchedByCachePlanner(event: QueryFetchedByCachePlanner) { }
  
  override def onQueryPushedToSharkScheduler(event: QueryPushedToSharkScheduler) { }

  
}