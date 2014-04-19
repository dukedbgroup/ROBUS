package edu.duke.CachePlanner

// Spark
import org.apache.spark.SparkContext
import SparkContext._
import shark.{SharkContext, SharkEnv}

object SharkQuery {
  
  def clearTables(sc: SharkContext) {
  
         
    val dropWebSales = "DROP TABLE IF EXISTS  web_sales"
    val dropWebSalesCached = "DROP TABLE IF EXISTS web_sales_cached"
      
    sc.runSql(dropWebSales)
    sc.runSql(dropWebSalesCached)

    val dropCatalogSales = "DROP TABLE IF EXISTS catalog_sales"
    val dropCatalogSalesCached = "DROP TABLE IF EXISTS catalog_sales_cached"
      
    sc.runSql(dropCatalogSales)
    sc.runSql(dropCatalogSalesCached)

    val dropStoreSales = "DROP TABLE IF EXISTS store_sales"
    val dropStoreSalesCached = "DROP TABLE IF EXISTS store_sales_cached"

    sc.runSql(dropStoreSales)
    sc.runSql(dropStoreSalesCached)
    
    //store sales int test
    val dropStoreSold = "DROP TABLE IF EXISTS store_sales_sold"
    sc.runSql(dropStoreSold)

    //store sales float test
    val dropStoreList = "DROP TABLE IF EXISTS store_sales_list"
    sc.runSql(dropStoreList)
    
    sc.runSql("DROP TABLE IF EXISTS store_sales_list_cached")
  }

  def createTables(sc: SharkContext, size_index: Int) {
    val size = Array("1gb", "4gb", "16gb", "64gb", "256gb", "1024gb")
    val table_size = size(size_index)
    val web_salesHDFSPath = "hdfs://yahoo047:9000/cache_test/"+ table_size +"/relational_data/web_sales"
    val catalog_salesHDFSPath = "hdfs://yahoo047:9000/cache_test/"+ table_size +"/relational_data/catalog_sales"  
    val store_salesHDFSPath = "hdfs://yahoo047:9000/cache_test/"+ table_size +"/relational_data/store_sales"
    

    //val web_salesHDFSPath = "hdfs://localhost:9000/web_sales"
  
    val createStoreSales = "create external table store_sales" +
                "(" +
                    "ss_sold_date_sk           int                       ," +
                    "ss_sold_time_sk           int                       ," +
                    "ss_item_sk                int                      ," +
                    "ss_customer_sk            int                       ," +
                    "ss_cdemo_sk               int                       ," +
                    "ss_hdemo_sk               int                       ," +
                    "ss_addr_sk                int                       ," +
                    "ss_store_sk               int                       ," +
                    "ss_promo_sk               int                       ," +
                    "ss_ticket_number          int                      ," +
                    "ss_quantity               int                       ," +
                    "ss_wholesale_cost         float                  ," +
                    "ss_list_price             float                  ," +
                    "ss_sales_price            float                  ," +
                    "ss_ext_discount_amt       float                  ," +
                    "ss_ext_sales_price        float                  ," +
                    "ss_ext_wholesale_cost     float                  ," +
                    "ss_ext_list_price         float                  ," +
                    "ss_ext_tax                float                  ," +
                    "ss_coupon_amt             float                  ," +
                    "ss_net_paid               float                  ," +
                    "ss_net_paid_inc_tax       float                  ," +
                    "ss_net_profit             float                  " +
                ")" +
                "row format delimited fields terminated by \'|\' " + "\n" +
                "location" + "\'" + store_salesHDFSPath + "\'"
    sc.runSql(createStoreSales)

 val createWebSales = "create external table web_sales" + 
             "(" + 
                 "ws_sold_date_sk           int," + 
                 "ws_sold_time_sk           int," + 
                 "ws_ship_date_sk           int," + 
                 "ws_item_sk                int," + 
                 "ws_bill_customer_sk       int," + 
                 "ws_bill_cdemo_sk          int," + 
                 "ws_bill_hdemo_sk          int," + 
                 "ws_bill_addr_sk           int," + 
                 "ws_ship_customer_sk       int," + 
                 "ws_ship_cdemo_sk          int," + 
                 "ws_ship_hdemo_sk          int," + 
                 "ws_ship_addr_sk           int," + 
                 "ws_web_page_sk            int," + 
                 "ws_web_site_sk            int," + 
                 "ws_ship_mode_sk           int," + 
                 "ws_warehouse_sk           int," + 
                 "ws_promo_sk               int," + 
                 "ws_order_number           int," + 
                 "ws_quantity               int," + 
                 "ws_wholesale_cost         float," + 
                 "ws_list_price             float," + 
                 "ws_sales_price            float," + 
                 "ws_ext_discount_amt       float," + 
                 "ws_ext_sales_price        float," + 
                 "ws_ext_wholesale_cost     float," + 
                 "ws_ext_list_price         float," + 
                 "ws_ext_tax                float," + 
                 "ws_coupon_amt             float," + 
                 "ws_ext_ship_cost          float," + 
                 "ws_net_paid               float," + 
                 "ws_net_paid_inc_tax       float," + 
                 "ws_net_paid_inc_ship      float," + 
                 "ws_net_paid_inc_ship_tax  float," + 
                 "ws_net_profit             float" +                 
             ")" +
             "row format delimited fields terminated by \'|\' " + "\n" +
             "location " + "\'" + web_salesHDFSPath + "\'"

             sc.runSql(createWebSales)
            
            
           
 val createCatalogSales = "create external table catalog_sales" +
         "(" +
             "cs_sold_date_sk           int                       ," +
             "cs_sold_time_sk           int                       ," +
             "cs_ship_date_sk           int                       ," +
             "cs_bill_customer_sk       int                       ," +
             "cs_bill_cdemo_sk          int                       ," +
             "cs_bill_hdemo_sk          int                       ," +
             "cs_bill_addr_sk           int                       ," +
             "cs_ship_customer_sk       int                       ," +
             "cs_ship_cdemo_sk          int                       ," +
             "cs_ship_hdemo_sk          int                       ," +
             "cs_ship_addr_sk           int                       ," +
             "cs_call_center_sk         int                       ," +
             "cs_catalog_page_sk        int                       ," +
             "cs_ship_mode_sk           int                       ," +
             "cs_warehouse_sk           int                       ," +
             "cs_item_sk                int                      ," +
             "cs_promo_sk               int                       ," +
             "cs_order_number           int                      ," +
             "cs_quantity               int                       ," +
             "cs_wholesale_cost         float                  ," +
             "cs_list_price             float                  ," +
             "cs_sales_price            float                  ," +
             "cs_ext_discount_amt       float                  ," +
             "cs_ext_sales_price        float                  ," +
             "cs_ext_wholesale_cost     float                  ," +
             "cs_ext_list_price         float                  ," +
             "cs_ext_tax                float                  ," +
             "cs_coupon_amt             float                  ," +
             "cs_ext_ship_cost          float                  ," +
             "cs_net_paid               float                  ," +
             "cs_net_paid_inc_tax       float                  ," +
             "cs_net_paid_inc_ship      float                  ," +
             "cs_net_paid_inc_ship_tax  float                  ," +
             "cs_net_profit             float                  " +
         ")" +
         "row format delimited fields terminated by \'|\' " + "\n" + 
         "location " + "\'" + catalog_salesHDFSPath + "\'"
         sc.runSql(createCatalogSales)
    
          
          
//      val createColumnTable = "create table store_sales_sold as select ss_sold_time_sk from store_sales"      
//      val createCachedTable = "create table store_sales_cached as select ss_sold_time_sk from store_sales"
        
      // val createColumnTable = "create table store_sales_list as select ss_ext_list_price from store_sales" 
      // val createCachedTable = "create table store_sales_cached as select ss_ext_list_price from store_sales"
      
        
      //sc.runSql(createColumnTable)
      
      //sc.runSql(createCachedTable)

  }

  def setConfigs(sc : SharkContext, compress : Boolean) {
    if(compress) {
      sc.runSql("set shark.column.compress = true")
    }
    else {
      sc.runSql("set shark.column.compress = false")
    }
  }

  def runQuery(sc: SharkContext) {
    println(sc.runSql("select count(*) from web_sales"))
  }

  def runQuery2(sc: SharkContext) {
    println(sc.runSql("select count(*) from store_sales"))
  }
  def init(sc: SharkContext) {
    clearTables(sc)
    setConfigs(sc, false)
    createTables(sc, 4)
  }
}
