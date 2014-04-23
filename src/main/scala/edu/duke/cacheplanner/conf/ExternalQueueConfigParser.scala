package edu.duke.cacheplanner.conf

import edu.duke.cacheplanner.queue.ExternalQueue

class ExternalQueueConfigParser extends AbstractParser {
  final val QUEUE = "queue"
  final val ID = "id"
  final val WEIGHT = "weight"
  final val MIN_SHARE = "minShare"
  final val BATCH_SIZE = "batchSize"
  
  override def fromXML() {
    val allocations = scala.xml.XML.loadFile("text.xml")
    for(n <- allocations \ QUEUE) {
      val id = n.attribute(ID).get.toString.toInt
      val weight = (n \ WEIGHT).text.toInt
      val minShare = (n \ MIN_SHARE).text.toInt
      val batchSize = (n \ BATCH_SIZE).text
      println(id, weight, minShare, batchSize)
    }
  }  
}