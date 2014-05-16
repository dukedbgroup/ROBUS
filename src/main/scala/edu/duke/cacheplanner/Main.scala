package edu.duke.cacheplanner

import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.conf.Parser
import edu.duke.cacheplanner.conf.Factory

object Main {
  def main(args: Array[String]) {
    val context = Factory.createContext
    context.start()
  }
}