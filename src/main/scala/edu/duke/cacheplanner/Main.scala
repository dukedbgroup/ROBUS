package edu.duke.cacheplanner

import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.listener.{SerializeListener, LoggingListener}
import java.io._

object Main {
  def main(args: Array[String]) {
    
    val context = Factory.createContext
    context.addListener(new LoggingListener)
    context.addListener(new SerializeListener("test11.json"))
    context.start()
    
    Thread.sleep(20000)	// total time of batch
    context.stop()
      //oos.close()
      //fos.close()
  }
}