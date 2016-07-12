package edu.duke.cacheplanner.query

import scala.collection.JavaConversions._
import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import java.io.File

object CacheQ {

  val cachedDataframes = scala.collection.mutable.Map[String, DataFrame]()

  val cacheState = new collection.mutable.HashSet[String] with collection.mutable.SynchronizedSet[String]

  def getCachedDataframe(ds: Dataset): DataFrame = {
    try {
      cachedDataframes.getOrElse(ds.getName, null)
    } catch { case e: Exception => null }
  }

  // Materializes a dataframe, should be done at the beginning of batch
  def cacheDataframe(dsName: String) = {
    try {
      cacheState += dsName
      cachedDataframes.getOrElse(dsName, null).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY).count
    } catch { case e: Exception => null }
  }

  // evicts a dataframe, should be done at the beginning of batch
  def uncacheDataframe(dsName: String) = {
    try {
      if(cacheState contains dsName) {
        cacheState -= dsName
        cachedDataframes.getOrElse(dsName, null).unpersist()
      } else {
        null
      }
    } catch { case e: Exception => null }
  }

  def createDataframes(sc: SparkContext, datasets: java.util.List[Dataset]) = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    for (ds <- datasets) {
      val path = "hdfs://xeno-62:9000/cacheplanner/" + ds.getName

      val df = ds.getName match {
        case "lineitem" => {
          val lineitem = sc.textFile(path).map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
          lineitem.select("l_partkey", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_shipmode", "l_shipinstruct")
        }          

        case "part" => {
           sc.textFile(path).map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
        }

        case _ => { // assuming the rest are sales
          val scan = sc.textFile(path).map(_.split('|')).map(s => Sales( try {s(0).trim.toInt} catch { case e: Exception => 0 }, try {s(1).trim.toInt} catch { case e: Exception => 0 }, try {s(2).trim.toInt} catch { case e: Exception => 0 }, try {s(3).trim.toInt} catch { case e: Exception => 0 }, try {s(4).trim.toInt} catch { case e: Exception => 0 }, try {s(5).trim.toInt} catch { case e: Exception => 0 }, try {s(6).trim.toInt} catch { case e: Exception => 0 }, try {s(7).trim.toInt} catch { case e: Exception => 0 }, try {s(8).trim.toInt} catch { case e: Exception => 0 }, try {s(9).trim.toInt} catch { case e: Exception => 0 }, try {s(10).trim.toInt} catch { case e: Exception => 0 }, try {s(11).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(12).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(13).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(14).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(15).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(16).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(17).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(18).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(19).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(20).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(21).trim.toDouble} catch { case e: Exception => 0.0 })).toDF()
          scan.select("ss_sold_date_sk", "ss_sold_time_sk", "ss_wholesale_cost", "ss_list_price", "ss_sales_price")
        }
      }
 
      cachedDataframes += (ds.getName -> df)
    }

  }

}


