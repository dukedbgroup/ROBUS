package edu.duke.cacheplanner.conf

import scala.collection.JavaConversions._
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.listener.LoggingListener
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.data.QueryDistribution
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import edu.duke.cacheplanner.queue.ExternalQueue
import edu.duke.cacheplanner.Context
import edu.duke.cacheplanner.generator.SingleTableQueryGenerator
import edu.duke.cacheplanner.generator.TPCHQueryGenerator
import edu.duke.cacheplanner.algorithm._
import scala.reflect.internal.util
import edu.duke.cacheplanner.query.AbstractQuery
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import edu.duke.cacheplanner.generator.ReplayQueryGenerator
import edu.duke.cacheplanner.data.QueueDistribution
import edu.duke.cacheplanner.data.DatasetDistribution
import edu.duke.cacheplanner.data.TPCHQueueDistribution

object Factory {
  val configManager = initConfigManager
  val listenerManager = initListener
  // datasets have to be initialized before queues
  val datasets = initDatasets
  // queues have to be initialized before distribution
  val externalQueues = initExternalQueue
  val distribution = initDistribution
  // queries have to be initialized before generators
  val queries = initQueries
  val generators = initGenerators
  val cachePlanner = initCachePlanner

  val tpchDatasets = initTPCHDatasets
  
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

  def initTPCHDatasets: java.util.List[Dataset] = {
    Parser.parseDataSets("conf/tpchdataset.xml")
  }

  def initDistribution: QueryDistribution = {
    val queryDistribution = new java.util.HashMap[Integer, QueueDistribution]()
    val tpchQueryDistribution = new java.util.HashMap[Integer, TPCHQueueDistribution]()
    val tpchQueueDistribution = Parser.parseTPCHDistribution("conf/tpchqueries.xml")
    for(queue <- externalQueues) {
      val rankFileName = queue.getRankFile
      if(rankFileName == "tpch") {
        tpchQueryDistribution.put(queue.getId(), tpchQueueDistribution);
      } else {
      val ranks = Parser.parseZipfRank("conf/" + rankFileName + ".xml")
      val queueDistribution = new java.util.HashMap[String, DatasetDistribution]()
      var rankSum = 0d
      (1 to ranks.size).foreach(t => rankSum += 1d/t)	// harmonic sum
      var count = 0
      for(dataset <- datasets) {
        val colDistribution = new java.util.HashMap[String, java.lang.Double]()
        val numCols = dataset.getColumns().size
        for(column <- dataset.getColumns()) {
          colDistribution.put(column.getColName(), (1d/numCols))	// FIXME: creating a uniform distribution over columns
        }
        val prob = 1d / (ranks(count).toInt * rankSum)	// zipf probability is inversely proportional to rank
        queueDistribution.put(dataset.getName(), 
            new DatasetDistribution(prob, colDistribution))
        count = count + 1
      }
      println("Distribution on queue " + queue.getId() + " = sum ")
      rankSum = 0d
      queueDistribution.foreach(t => rankSum += t._2.getDataProb)
      print(rankSum)
      queryDistribution.put(queue.getId(), new QueueDistribution(queueDistribution))
      }
    }
    val distribution = new QueryDistribution(queryDistribution)
    distribution.setTPCHQueryDistribution(tpchQueryDistribution)
    return distribution
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
        case "singleTable" =>  
		if(distribution.getQueueDistribution(queueId) == None) {
			return new TPCHQueryGenerator(lambda, queueId, name)
		} else {
			return new SingleTableQueryGenerator(lambda, queueId, name, meanColNum, stdColNum, grouping)
		}
	
        case "replay" => return new ReplayQueryGenerator(lambda, queueId, name, 
            queries(queueId))
    }
  }
  
  def createContext : Context = {
    new Context(listenerManager, generators, cachePlanner)
  }
  
  def initCachePlanner : AbstractCachePlanner = {
    val mode = configManager.getAlgorithmMode()
    mode match {
//      case "offline" => return new OfflineCachePlannerColumn(true, listenerManager, 
//          externalQueues, datasets, distribution, configManager)
      case _ => return new OnlineCachePlannerSingleDS(true, listenerManager, 
          externalQueues, datasets, tpchDatasets, distribution, configManager)
    }
  }

  def getDatasets() : java.util.List[Dataset] = {
    return datasets
  }

  def getTPCHDatasets() : java.util.List[Dataset] = {
    return tpchDatasets
  }

  def getQueues(): java.util.List[ExternalQueue] = {
    return externalQueues
  }

  def getConfigManager(): ConfigManager = {
    return configManager
  }
}
