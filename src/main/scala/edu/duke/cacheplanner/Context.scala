package edu.duke.cacheplanner

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.Listener
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import edu.duke.cacheplanner.algorithm.AbstractCachePlanner

class Context(manager: ListenerManager, generators: java.util.List[AbstractQueryGenerator], planner: AbstractCachePlanner) {
  
  private val listenerManager = manager
  //private val sc = initSharkContext
  private val queryGenerators = generators
  private val cachePlanner = planner
  
//  def initSharkContext() : SharkContext = {
//    SharkEnv.initWithSharkContext("Cache_Experiment")
//    val sc = SharkEnv.sc.asInstanceOf[SharkContext]
//    // attach JobLogger & StatsReportListener
//    sc.addSparkListener(joblogger)
//    sc.addSparkListener(listener)
//    sc
//  }
  
  
//  def setConf(attribute: String, value: String) {
//    System.setProperty(attribute, value)
//  }
  
  def addListener(listener : Listener) {
    listenerManager.addListener(listener)
  }
  
  def start() {
    listenerManager.start()
    for(gen <- queryGenerators.toArray()) {
      gen.asInstanceOf[AbstractQueryGenerator].start()
    }
    cachePlanner.start
  }
  
  def stop() {
    listenerManager.stop()
    for(gen <- queryGenerators.toArray()) {
      gen.asInstanceOf[AbstractQueryGenerator].stop()
    }
    cachePlanner.stop()
  }
}