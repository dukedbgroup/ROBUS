package edu.duke.cacheplanner.conf

import edu.duke.cacheplanner.queue.ExternalQueue
import java.util.HashMap

class Parser() extends Constants {
  
  /**
   * parse the xml file for external queue
   */
  def parseExternalQueue(path: String) {
    val allocations = scala.xml.XML.loadFile(path)
    for(n <- allocations \ QUEUE) {
      val id = n.attribute(ID).get.toString.toInt
      val weight = (n \ WEIGHT).text.toInt
      val minShare = (n \ MIN_SHARE).text.toInt
      val batchSize = (n \ BATCH_SIZE).text
      println(id, weight, minShare, batchSize)
    }
  }
  
  /**
   * parse the xml file and create the map 
   */
  def parseMap(path: String) : HashMap[String,String] = {
    val map = new HashMap[String,String]()
    val xmlTree = scala.xml.XML.loadFile(path)
    for(n <- xmlTree \ PROPERTY) {
      for(k <- n \ NAME) {
        val v = n \ VALUE
        map.put(k.text, v.text)
      }
    }
    return map
  }
}