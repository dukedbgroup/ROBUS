package edu.duke.cacheplanner.conf

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.LoggingListener
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.data.QueryDistribution
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.Context
import edu.duke.cacheplanner.generator.SingleTableQueryGenerator
import edu.duke.cacheplanner.algorithm._
import scala.reflect.internal.util
import edu.duke.cacheplanner.query.AbstractQuery
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import edu.duke.cacheplanner.generator.ReplayQueryGenerator

object Factory {
  val configManager = initConfigManager
  val listenerManager = initListener
  val datasets = initDatasets
  val distribution = initDistribution
  val externalQueues = initExternalQueue
  val queries = initQueries
  val generators = initGenerators
  val cachePlanner = initCachePlanner
  
  def initConfigManager : ConfigManager = {
    new ConfigManager(Parser.parseConfig("conf/config.xml"))
  }
  
  def initListener: ListenerManager = {
    val manager = new ListenerManager
    return manager
  }
  
  def initExternalQueue : java.util.List[ExternalQueue] = {
    Parser.parseExternalQueue("conf/external.xml")
  }
  
  def initDatasets: java.util.List[Dataset] = {
    Parser.parseDataSets("conf/dataset.xml")
  }
  
  def initDistribution: QueryDistribution = {
    Parser.parseQueryDistribution("conf/distribution.xml")
  }
  
  def initQueries: Map[Int, java.util.Queue[AbstractQuery]] = {
    val mode = configManager.getGeneratorMode()
    mode match {
      case "replay" => Parser.parseQueries(configManager.getReplayFilePath)
      case _ => return null
    }
  }
 
  def initGenerators: java.util.List[AbstractQueryGenerator] = {
    val generator = scala.xml.XML.loadFile("conf/generator.xml")
    val generators = new java.util.ArrayList[AbstractQueryGenerator]
    for(n <- generator \ Constants.GENERATOR) {
      val queueId = (n \ Constants.QUEUE_ID).text.toInt
      val name = (n \ Constants.QUEUE_NAME).text
      val lambda = (n \ Constants.LAMBDA).text.toDouble
      val meanColNum = (n \ Constants.MEAN_COLUMN).text.toDouble
      val stdColNum = (n \ Constants.STD_COLUMN).text.toDouble
      val grouping = (n \ Constants.GROUPING_PROBABILITY).text.toDouble
      val generator = createGenerator(queueId, name, lambda, 
          meanColNum, stdColNum, grouping)
      generators.add(generator)
      generator.setDatasets(datasets)
      generator.setListenerManager(listenerManager)
      generator.setQueryDistribution(distribution)
      for(q <- externalQueues.toArray()) {
        val queue = q.asInstanceOf[ExternalQueue]
        if(generator.getQueueId == queue.getId()) {
          generator.setExternalQueue(queue)
        }
      }
    }
    return generators
  }
  
  def createGenerator(queueId: Int, name: String, lambda: Double, 
      meanColNum: Double, stdColNum: Double, grouping: Double): 
      AbstractQueryGenerator = {
    val mode = configManager.getGeneratorMode()
    mode match {
        case "singleTable" => return new SingleTableQueryGenerator(lambda, 
            queueId, name, meanColNum, stdColNum, grouping)
        case "replay" => return new ReplayQueryGenerator(lambda, queueId, name, 
            queries(queueId))
    }
  }
  
  def createContext : Context = {
    new Context(listenerManager, generators, cachePlanner)
  }
  
  def initCachePlanner : AbstractCachePlanner = {
    val mode = configManager.getAlgorithmMode()
    val setup = configManager.getAlgorithmSetup()
    mode match {
      case "offline" => return new OfflineCachePlannerColumn(setup, listenerManager, 
          externalQueues, datasets, configManager)
      case "online" => return new OnlineCachePlannerSingleDS(setup, listenerManager, 
          externalQueues, datasets, configManager)
    }
  }

  def getDatasets() : java.util.List[Dataset] = {
    return datasets
  }

  def getQueues(): java.util.List[ExternalQueue] = {
    return externalQueues
  }

  def getConfigManager(): ConfigManager = {
    return configManager
  }
}