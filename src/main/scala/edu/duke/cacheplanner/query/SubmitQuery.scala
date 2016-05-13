package edu.duke.cacheplanner.query

import org.apache.spark.{SparkConf, SparkContext}

abstract class SubmitQuery(name: String, memory: String, cores: String) {

  val tachyonURL = "tachyon://xeno-62:19998"

  val tachyonHOME = tachyonURL + "/cacheplanner"

  val hdfsHOME = "hdfs://xeno-62:9000/cacheplanner"

  val sc = initSparkContext(name, memory, cores)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  def initSparkContext(name: String, memory: String, cores: String): SparkContext = {
    val conf = new SparkConf().setAppName(name).setMaster(System.getenv("MASTER"))
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(Seq("target/scala-2.10/CachePlanner-assembly-0.1.jar"))
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.executor.memory", memory)
    conf.set("spark.cores.max", cores)
    conf.set("spark.storage.memoryFraction", "0.3")

    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "hdfs://xeno-62:9000/sparkEventLog")

    conf.set("spark.externalBlockStore.blockManager", "org.apache.spark.storage.TachyonBlockManager")
    conf.set("spark.externalBlockStore.url", tachyonURL)

    val sc = new SparkContext(conf)
    // tachyon configuration
    sc.hadoopConfiguration.set("fs.tachyon.impl", "tachyon.hadoop.TFS")
    sc
  }

  def submit(): Unit

}

