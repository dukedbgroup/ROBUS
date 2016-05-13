package edu.duke.cacheplanner.conf

import edu.duke.cacheplanner.queue.ExternalQueue
import java.util.HashMap
import java.util.Map
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.data.ColumnType
import java.util.HashSet
import java.util.ArrayList
import java.util.List
import java.util.Queue
import java.util.LinkedList
import edu.duke.cacheplanner.data.QueryDistribution
import edu.duke.cacheplanner.data.DatasetDistribution
import java.lang.Integer
import edu.duke.cacheplanner.data.QueueDistribution
import edu.duke.cacheplanner.generator.AbstractQueryGenerator
import com.google.gson.Gson
import scala.io.Source
import edu.duke.cacheplanner.query.AbstractQuery
import edu.duke.cacheplanner.query.GroupingQuery
import edu.duke.cacheplanner.query.SingleDatasetQuery
import edu.duke.cacheplanner.query.TPCHQuery
import edu.duke.cacheplanner.data.TPCHQueueDistribution


object Parser {  
  // percent error in utility
  val ERROR = 0;

  /**
   * perturb cost estimation value within the error bound
   */
  def perturb(value: Double) : Double = {
    val direction = 2 * (scala.util.Random.nextDouble - 0.5)
    value + direction * ERROR
  }

  /**
   * parse the external.xml file for external queue
   */
  def parseExternalQueue(path: String) : java.util.List[ExternalQueue]  = {
    val queueList = new ArrayList[ExternalQueue]
    val externalQueue = scala.xml.XML.loadFile(path)
    for(n <- externalQueue \ Constants.QUEUE) {
      val id = n.attribute(Constants.ID).get.toString.toInt
      val name = n.attribute(Constants.NAME).get.toString
      val weight = (n \ Constants.WEIGHT).text.toInt
      val minShare = (n \ Constants.MIN_SHARE).text.toInt
      val batchSize = (n \ Constants.BATCH_SIZE).text.toInt
      val rankFile = (n \ Constants.ZIPF_RANK_FILE).text
      val queue = new ExternalQueue(id, weight, minShare, batchSize, name)
      queue.setRankFile(rankFile)
      queueList.add(queue)
    }
    return queueList
  }

  /**
   * parse zipfranks xml file for popularity ranks over datasets
   */
  def parseZipfRank(path: String): Array[String] = {
    val rankData = scala.xml.XML.loadFile(path)
    return rankData.text.split(',')
  }
  /**
   * parse the config.xml file and create the map 
   */
  def parseConfig(path: String) : HashMap[String,String] = {
    val map = new HashMap[String,String]()
    val xmlTree = scala.xml.XML.loadFile(path)
    for(n <- xmlTree \ Constants.PROPERTY) {
      for(k <- n \ Constants.NAME) {
        val v = n \ Constants.VALUE
        map.put(k.text, v.text)
      }
    }
    return map
  }
  
  def parseDataSets(path: String) : java.util.List[Dataset] = {
    val datasets = scala.xml.XML.loadFile(path)
    val dataset_list = new ArrayList[Dataset]
    
    for(n <- datasets \ Constants.DATASET) {
      val name = (n \ Constants.NAME).text
      val path = (n \ Constants.PATH).text
      val dataset = new Dataset(name, path)      
      val col_list = new HashSet[Column]
      val columns = n \ Constants.COlUMNS
      for(c <- columns \ Constants.COLUMN) {
        val col_name = (c \ Constants.NAME).text
        val col_size = (c \ Constants.SIZE).text.toDouble
        val col_type = (c \ Constants.TYPE).text
        col_list.add(new Column(col_size, col_name, getColumnType(col_type), name))
      }
      val cacheSize = (n \ Constants.CACHE_SIZE).text.toDouble
      dataset.setEstimatedSize(perturb(cacheSize))
      dataset.setColumns(col_list)
      dataset_list.add(dataset)
    }
    return dataset_list
  }

  def getColumnType(colType: String): ColumnType = colType match {
      case "string" => return ColumnType.STRING
      case "int" => return ColumnType.INT
      case "float" => return ColumnType.FLOAT
      case "double" => return ColumnType.DOUBLE
      case "boolean" => return ColumnType.BOOLEAN
      case "timestamp" => return ColumnType.TIMESTAMP
    }
  
  def parseQueryDistribution(path: String) : QueryDistribution = {
    val queryDistribution = new QueryDistribution
    val query = scala.xml.XML.loadFile(path)
    for(n <- query \ Constants.QUEUE_DISTRIBUTION) {
      val queueId = (n \ Constants.QUEUE_ID).text.toInt
      val dataDistribution = new HashMap[String, DatasetDistribution]
      for(d <- n \ Constants.DATA_DISTRIBUTION) {
        val colDistribution = new HashMap[String, java.lang.Double]
        val data_name = (d \ Constants.NAME).text
        val data_prob = (d \ Constants.PROBABILITY).text.toDouble
        val columns = d \ Constants.COlUMNS
        for(c <- columns \ Constants.COLUMN) {
          val col_name = (c \ Constants.NAME).text
          val col_prob = (c \ Constants.PROBABILITY).text.toDouble
          colDistribution.put(col_name, col_prob)
        }
        dataDistribution.put(data_name, new DatasetDistribution(data_prob, colDistribution))
      }
      queryDistribution.setQueueDistribution(queueId, new QueueDistribution(dataDistribution))
    }
    return queryDistribution
  }

  def parseTPCHDistribution(path: String) : TPCHQueueDistribution = {
    val queueDistribution = new HashMap[TPCHQuery, java.lang.Double]
    val query = scala.xml.XML.loadFile(path)
    for(n <- query \ Constants.TPCH_QUERY) {
        val name = (n \ Constants.TPCH_NAME).text
        val path = (n \ Constants.TPCH_PATH).text
        val cachedPath = (n \ Constants.TPCH_CACHED_PATH).text
        val benefit = (n \ Constants.TPCH_BENEFIT).text.toDouble
        val prob = (n \ Constants.TPCH_PROBABILITY).text.toDouble
        queueDistribution.put(new TPCHQuery(0, 0, name, path, cachedPath, benefit), prob)
    }
    return new TPCHQueueDistribution(queueDistribution)
  }
  
  def parseQueries(path: String) : 
  scala.collection.mutable.Map[Int, java.util.Queue[AbstractQuery]] = {
      val gson = new Gson()
      var map = new scala.collection.mutable.HashMap[Int, 
        java.util.LinkedList[AbstractQuery]]()
	  for(line <- Source.fromFile(path).getLines()) {
	      var query: AbstractQuery = null
	      if (line.contains("groupingColumn")) {
	        query = gson.fromJson(line, classOf[GroupingQuery])
	        println(query.toHiveQL(false))
	      } 
	      else if (line.contains("path")) {
                query = gson.fromJson(line, classOf[TPCHQuery])
                println(query.toHiveQL(false))
	      }
	      else {
	        query = gson.fromJson(line, classOf[SingleDatasetQuery])
	        println(query.toHiveQL(false))
	      }
	      val queue = map.getOrElse(query.getQueueID(), null)
	      if(queue == null) {
	        val newList = new java.util.LinkedList[AbstractQuery]()
	        newList.add(query)
	        map(query.getQueueID()) = newList
	      }
	      else {
	        map(query.getQueueID).add(query)
	      }
	  }
      println(map)
      map.asInstanceOf[scala.collection.mutable.Map[Int, 
        java.util.Queue[AbstractQuery]]]
  }
}
