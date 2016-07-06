package edu.duke.cacheplanner.query

import scala.collection.JavaConversions._
import edu.duke.cacheplanner.data.Dataset
import org.apache.spark.{SparkConf, SparkContext}

case class Sales(
              ss_sold_date_sk: Int,
              ss_sold_time_sk: Int,
              ss_item_sk: Int,
              ss_customer_sk: Int,
              ss_cdemo_sk: Int,
              ss_hdemo_sk: Int,
              ss_addr_sk: Int,
              ss_store_sk: Int,
              ss_promo_sk: Int,
              ss_ticket_number: Int,
              ss_quantity: Int,
              ss_wholesale_cost: Double,
              ss_list_price: Double,
              ss_sales_price: Double,
              ss_ext_discount_amt: Double,
              ss_ext_sales_price: Double,
              ss_ext_wholesale_cost: Double,
              ss_ext_list_price: Double,
              ss_ext_tax: Double,
              ss_coupon_amt: Double,
              ss_net_paid: Double,
              ss_net_paid_inc_tax: Double
//              ss_net_profit: Double // case classcan't have more than 22 parameters
)

class SalesQ(appName: String, query: AbstractQuery, sc: SparkContext, datasetsCached: java.util.List[Dataset]) 
	extends SubmitQuery(appName, sc) {

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val expression = {

    // extract query
    val q = query.asInstanceOf[SingleDatasetQuery]
    val sel = q.getSelections
    // val pro = q.getProjections

    // dataset path
    val ds = q.getDataset()
    val path = hdfsHOME + "/" + ds.getName

    // read input dataset
    val scan = {
      if(datasetsCached.contains(ds)) {
        CacheQ.getCachedDataframe(ds)
      } else {
        sc.textFile(path).map(_.split('|')).map(s => Sales( try {s(0).trim.toInt} catch { case e: Exception => 0 }, try {s(1).trim.toInt} catch { case e: Exception => 0 }, try {s(2).trim.toInt} catch { case e: Exception => 0 }, try {s(3).trim.toInt} catch { case e: Exception => 0 }, try {s(4).trim.toInt} catch { case e: Exception => 0 }, try {s(5).trim.toInt} catch { case e: Exception => 0 }, try {s(6).trim.toInt} catch { case e: Exception => 0 }, try {s(7).trim.toInt} catch { case e: Exception => 0 }, try {s(8).trim.toInt} catch { case e: Exception => 0 }, try {s(9).trim.toInt} catch { case e: Exception => 0 }, try {s(10).trim.toInt} catch { case e: Exception => 0 }, try {s(11).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(12).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(13).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(14).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(15).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(16).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(17).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(18).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(19).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(20).trim.toDouble} catch { case e: Exception => 0.0 }, try {s(21).trim.toDouble} catch { case e: Exception => 0.0 })).toDF()
      }
    }

    // apply filter
    if(sel != null && sel.length > 0) {
      var filterString: String = ""
      var first: Boolean = true
      for (s <- sel) {
        if(!first) {
          filterString += " && "
        }
        first = false
        filterString += "" + s.getCol.getColName + " "
        filterString += s.getOperator.toString
        filterString += " " + s.getValue
      }
      scan.filter(filterString)
    } else {
      scan
    }

  }

  def submit() {
    try { expression.collect } catch { case e: Exception => e.printStackTrace }
  }

}
