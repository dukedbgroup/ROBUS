package shark.example

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.scheduler.JobLogger
import shark.{SharkContext, SharkEnv}

object SharkMain2 {

  def run() {

    // create shark context (master configuration is set from "conf/shark-env.sh")

    // SharkEnv.initWithSharkContext("shark-example")
    // val sc = SharkEnv.sc.asInstanceOf[SharkContext]

    val conf = new SparkConf()
    conf.setMaster("spark://mint:7077")
    conf.setAppName("test2")
    conf.set("spark.cores.max", "1")
    conf.set("spark.executor.memory","256m")
    val sc = new SharkContext(conf)
    SharkEnv.sc = sc
    SharkQuery.run(sc)


    // val conf2 = new SparkConf()
    // conf2.setMaster("spark://mint:7077")
    // conf2.setAppName("test2")
    // conf2.set("spark.cores.max", "1")
    // conf2.set("spark.executor.memory","256m")
    // val sc2 = new SharkContext(conf2)
    // SharkEnv.sc = sc2

    // SharkQuery.run(sc2)


    // attach JobLogger & StatsReportListener
    // val joblogger = new JobLogger("test", "cache_test")
    // val listener = new StatsReportListener()
    // sc.addSparkListener(joblogger)
    // sc.addSparkListener(listener)

  }
}
