package edu.duke.cacheplanner

import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.listener.{SerializeListener, LoggingListener}
import java.io._
import edu.duke.cacheplanner.query.{QueryUtil, AbstractQuery}
import edu.duke.cacheplanner.algorithm.SingleColumnBatchAnalyzer
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.conf.Parser
import edu.duke.cacheplanner.query.SingleTableQuery

object Main {
  def main(args: Array[String]) {
    //serialize
    //val pw = new PrintWriter("test_json.txt")
    
    val context = Factory.createContext
    //val datasets = Factory.getDatasets()
    //println(QueryUtil.getTableCreateSQL(datasets.get(0)))
    //println(QueryUtil.getTableCreateSQL(datasets.get(1)))
    //println(QueryUtil.getCacheTableCreateSQL(datasets.get(0).getName(), datasets.get(0).getColumnList()));
    //println(QueryUtil.getCacheTableInsertSQL(datasets.get(0).getName(), datasets.get(0).getColumnList()));
    context.addListener(new LoggingListener)
    context.addListener(new SerializeListener())
    context.start()
      
      //Thread.sleep(20000)
      //context.stop()
      //oos.close()
      //fos.close()
  }
}