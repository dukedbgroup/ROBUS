package edu.duke.cacheplanner.conf

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.LoggingListener
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.data.QueryDistribution
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.Context
import edu.duke.cacheplanner.generator.SingleTableQueryGenerator
import edu.duke.cacheplanner.algorithm.{OnlineCachePlanner, OfflineCachePlanner, AbstractCachePlanner}
import scala.reflect.internal.util

object Factory {
  val configManager = initConfigManager
  val listenerManager = initListener
  val datasets = initDatasets
  val distribution = initDistribution
  val externalQueues = initExternalQueue
  val generators = initGenerators
  
  
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

  def initGenerators: java.util.List[AbstractQueryGenerator] = {
    val generator = scala.xml.XML.loadFile("conf/generator.xml")
    val generators = new java.util.ArrayList[AbstractQueryGenerator]
    for(n <- generator \ Constants.GENERATOR) {
      val queueId = (n \ Constants.QUEUE_ID).text.toInt
      val lambda = (n \ Constants.LAMBDA).text.toDouble
      val meanColNum = (n \ Constants.MEAN_COLUMN).text.toDouble
      val stdColNum = (n \ Constants.STD_COLUMN).text.toDouble
      val grouping = (n \ Constants.GROUPING_PROBABILITY).text.toDouble
      val generator = createGenerator(queueId, lambda, meanColNum, stdColNum, grouping)
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
  
  def createGenerator(queueId: Int, lambda: Double, meanColNum: Double, stdColNum: Double, grouping: Double): AbstractQueryGenerator = {
    val mode = configManager.getGeneratorMode()  
    mode match {
        case "singleTable" => return new SingleTableQueryGenerator(lambda, queueId, meanColNum, stdColNum, grouping)
    }
  }
  
  def createContext : Context = {
    new Context(listenerManager, generators, null)
  }
  
  def createCachePlanner : AbstractCachePlanner = {
    val mode = configManager.getAlgorithmMode()
    val setup = configManager.getAlgorithmSetup
    mode match {
      case "online" => return new OfflineCachePlanner(setup, listenerManager, externalQueues, datasets)
      case "offline" => return new OnlineCachePlanner(setup, listenerManager, externalQueues, datasets, configManager.getPlannerBatchTime)
    }
  }

  def getDatasets() : java.util.List[Dataset] = {
    return datasets
  }
  
}