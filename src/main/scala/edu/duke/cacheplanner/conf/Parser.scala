package edu.duke.cacheplanner.conf

import edu.duke.cacheplanner.queue.ExternalQueue
import java.util.HashMap
import java.util.Map
import edu.duke.cacheplanner.data.Dataset
import edu.duke.cacheplanner.data.Column
import java.util.HashSet
import java.util.ArrayList
import java.util.List
import edu.duke.cacheplanner.data.QueryDistribution
import edu.duke.cacheplanner.data.DatasetDistribution
import java.lang.Integer
import edu.duke.cacheplanner.data.QueueDistribution
import edu.duke.cacheplanner.generator.AbstractQueryGenerator


object Parser {  
  /**
   * parse the external.xml file for external queue
   */
  def parseExternalQueue(path: String) : java.util.List[ExternalQueue]  = {
    val queueList = new ArrayList[ExternalQueue]
    val externalQueue = scala.xml.XML.loadFile(path)
    for(n <- externalQueue \ Constants.QUEUE) {
      val id = n.attribute(Constants.ID).get.toString.toInt
      val weight = (n \ Constants.WEIGHT).text.toInt
      val minShare = (n \ Constants.MIN_SHARE).text.toInt
      val batchSize = (n \ Constants.BATCH_SIZE).text.toInt
      println(id, weight, minShare, batchSize)
      queueList.add(new ExternalQueue(id, weight, minShare, batchSize))
    }
    return queueList
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
        col_list.add(new Column(col_size, col_name))
      }
      dataset.setColumns(col_list)
      dataset_list.add(dataset)
    }
    return dataset_list
  }
  
  def parseQueryDistribution(path: String) : QueryDistribution = {
    val queryDistribution = new QueryDistribution
    val query = scala.xml.XML.loadFile(path)
    for(n <- query \ Constants.QUEUE_DISTRIBUTION) {
      val queueId = (n \ Constants.QUEUEID).text.toInt
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
}