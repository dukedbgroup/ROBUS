package edu.duke.cacheplanner.query

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler._

abstract class SubmitQuery(name: String, sc: SparkContext) {

  val hdfsHOME = "hdfs://xeno-62:9000/cacheplanner"

//  val sc = initSparkContext(name, memory, cores)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

/*  def initSparkContext(name: String, memory: String, cores: String): SparkContext = {
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

    // speculative
    conf.set("spark.speculation", "true")
    conf.set("spark.speculation.multiplier", "2")
    conf.set("spark.speculation.interval", "1s")

    val sc = new SparkContext(conf)
    // tachyon configuration
    sc.hadoopConfiguration.set("fs.tachyon.impl", "tachyon.hadoop.TFS")

    // add listener
    sc.addSparkListener(new SparkListener() {
      var firstTaskSeen = false

      override def onTaskStart(taskStart: SparkListenerTaskStart) {
        if(!firstTaskSeen) {
          firstTaskSeen = true
          val (lock, channel) = lockFile
          unlockFile(lock, channel)
        }
      }

    });

    sc
  }
*/

  def lockFile(): (java.nio.channels.FileLock, java.nio.channels.FileChannel) = {
		try {
			
		    val file = new java.io.File("/tmp/" + name);
		    
		    // Creates a random access file stream to read from, and optionally to write to
		    val channel = new java.io.RandomAccessFile(file, "rw").getChannel();

		    // Acquire an exclusive lock on this channel's file (blocks until lock can be retrieved)
		    val lock = channel.lock();
		    (lock, channel)
		    
		} catch { case e: Exception =>
			println("I/O Error: "); e.printStackTrace();
			(null, null)
		}
  }

  def unlockFile(lock: java.nio.channels.FileLock, channel: java.nio.channels.FileChannel) {
		try {
                    // release the lock
                    lock.release();

                    // close the channel
                    channel.close();
                } catch { case e: Exception =>
                        println("I/O Error: "); e.printStackTrace();
                }

  }

  def submit(): Unit

}

