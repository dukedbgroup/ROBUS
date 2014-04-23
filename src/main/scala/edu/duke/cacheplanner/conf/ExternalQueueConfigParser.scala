package edu.duke.cacheplanner.conf

class ExternalQueueConfigParser extends AbstractParser {
  
  final val SCHEDULING_MODE = "schedulingMode"
  final val WEIGHT = "weight"
  final val MIN_SHARE = "minShare"
  final val NAME = "name"
  final val POOL = "pool"
  
  override def fromXML() {
    val allocations = scala.xml.XML.loadFile("conf/external.xml")
    for(n <- allocations \ POOL) {
      var name = n.attribute(NAME).get.toString
      var mode = (n \ SCHEDULING_MODE).text
      var weight = (n \ WEIGHT).text.toInt
      var minShare = (n \ MIN_SHARE).text.toInt
      println(name, mode, weight, minShare)
    }
  }
}