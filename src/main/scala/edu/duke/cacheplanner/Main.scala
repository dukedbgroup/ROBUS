package edu.duke.cacheplanner

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.conf.Parser
import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.util.TruncatedNormal

object Main {
  def main(args: Array[String]) {
    val context = Factory.createContext
    context.start()
  }
}