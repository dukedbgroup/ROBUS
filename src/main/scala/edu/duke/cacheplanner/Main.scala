package edu.duke.cacheplanner

import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.listener.{SerializeListener, LoggingListener}
import java.io._
import edu.duke.cacheplanner.query.{QueryUtil, AbstractQuery}
import edu.duke.cacheplanner.algorithm.SingleColumnBatchAnalyzer
import edu.duke.cacheplanner.data.Column
import edu.duke.cacheplanner.conf.Parser

object Main {
  def main(args: Array[String]) {
    //serialize
    val fos = new FileOutputStream("test.ser", false)
    val oos = new ObjectOutputStream(fos)

    val context = Factory.createContext
    val datasets = Factory.getDatasets()
    //println(QueryUtil.getTableCreateSQL(datasets.get(0)))
    //println(QueryUtil.getTableCreateSQL(datasets.get(1)))
    //println(QueryUtil.getCacheTableCreateSQL(datasets.get(0).getName(), datasets.get(0).getColumnList()));
    //println(QueryUtil.getCacheTableInsertSQL(datasets.get(0).getName(), datasets.get(0).getColumnList()));
      context.addListener(new LoggingListener)
      context.addListener(new SerializeListener(oos))
      context.start()
      
      //Thread.sleep(20000)
      //context.stop()
      //oos.close()
      //fos.close()
      //load()
  }

  def load() {
    val fis = new FileInputStream("test.ser")
    val ois = new ObjectInputStream(fis)
    var check = true
    while (check) {
      try{
        val q = ois.readObject().asInstanceOf[AbstractQuery]
        println(q.toHiveQL(false))
      } catch {
        case eof : EOFException => check = false
      }
    }
    ois.close()
    fis.close()

  }
}