package edu.duke.cacheplanner

import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.listener.{SerializeListener, LoggingListener}
import java.io._
import edu.duke.cacheplanner.query.{QueryUtil, AbstractQuery}
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.conf.Parser
import edu.duke.cacheplanner.query.SingleTableQuery

object Main {
  def main(args: Array[String]) {
    
    val context = Factory.createContext
    context.addListener(new LoggingListener)
    context.addListener(new SerializeListener("test11.json"))
    context.start()
      
      //Thread.sleep(20000)
      //context.stop()
      //oos.close()
      //fos.close()
  }
}