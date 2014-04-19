package edu.duke.cacheplanner.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import shark.{SharkContext, SharkEnv}

//This code just copied from shark context testing 
class SharkRunnable(context: SharkContext) extends Runnable{
  val sc = context

  def run() {
    sc.setLocalProperty("spark.scheduler.pool", "pool1")
    SharkQuery.runQuery(sc)
  }
}

class SharkRunnableTwo(context: SharkContext) extends Runnable{
  val sc = context

  def run() {
    sc.setLocalProperty("spark.scheduler.pool", "pool2")
    SharkQuery.runQuery2(sc)
  }
}

