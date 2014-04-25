package edu.duke.cacheplanner

import edu.duke.cacheplanner.generator.GroupingQueryGenerator
import edu.duke.cacheplanner.listener.ListenerManager
import edu.duke.cacheplanner.algorithm.GreedyCachePlanner

object Main {
  def main(args: Array[String]) {
    val gen = new GroupingQueryGenerator(2)
    val listener = new ListenerManager
    val planner = new GreedyCachePlanner
    val context = new Context(listener,gen, planner)
    context.start()
    Thread.sleep(20000)
    context.stop()
  }
}