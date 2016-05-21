package edu.duke.cacheplanner.query

import scala.collection.JavaConversions._
import edu.duke.cacheplanner.data.Dataset

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

class SalesQ(appName: String, query: AbstractQuery, memory: String, cores: String, datasetsCached: java.util.List[Dataset]) 
	extends SubmitQuery(appName, memory, cores) {

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val expression = {

    // extract query
    val q = query.asInstanceOf[SingleDatasetQuery]
    val sel = q.getSelections
    // val pro = q.getProjections

    // dataset path
    val ds = q.getDataset()
    val path = {
      if( datasetsCached.contains(ds) ) {
        tachyonHOME + "/" + ds.getName
      } else {
        hdfsHOME + "/" + ds.getName
      }
    }

    // read input dataset
    // HACK: only mapping two columns
    val scan = sc.textFile(path).map(_.split('|')).map(s => Sales( try {s(0).trim.toInt} catch { case e: Exception => 0 }, try {s(1).trim.toInt} catch { case e: Exception => 0 }, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)).toDF()

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
    expression.collect
  }

}
