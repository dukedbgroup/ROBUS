package edu.duke.cacheplanner.query

import scala.collection.JavaConversions._
import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.min
import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

case class Customer(
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Int,
  l_partkey: Int,
  l_suppkey: Int,
  l_linenumber: Int,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comment: String)

case class Order(
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String)

case class Part(
  p_partkey: Int,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Int,
  ps_suppkey: Int,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Int,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Int,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

abstract class AbstractTPCHQuery(appName: String, sc: SparkContext, datasetsCached: java.util.List[Dataset]) 
	extends SubmitQuery(appName, sc) {

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  def getPaths: (String, String) = {
   var lineitem_path: String = hdfsHOME + "/lineitem"
   var part_path: String = hdfsHOME + "/part"
/*   for (ds <- datasetsCached) {
    if(ds.getName == "lineitem")
    {
      lineitem_path = tachyonHOME + "/lineitem";
    }
    if(ds.getName == "part")
    {
      part_path = tachyonHOME + "/part";
    }
   }*/
   (lineitem_path, part_path)
  }

  val (lineitem_path, part_path) = getPaths
  
  var lineitem = sc.textFile(lineitem_path).map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()

  var part = sc.textFile(part_path).map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()

  for (ds <- datasetsCached) {
    if(ds.getName == "lineitem")
    {
      lineitem = CacheQ.getCachedDataframe(ds)
    }
    if(ds.getName == "part")
    {
      part = CacheQ.getCachedDataframe(ds)
    }
  }

  def submit(): Unit

}
