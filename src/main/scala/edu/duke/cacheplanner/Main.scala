package edu.duke.cacheplanner

import edu.duke.cacheplanner.conf.Factory
import edu.duke.cacheplanner.listener.{SerializeListener, LoggingListener}
import java.io._
import edu.duke.cacheplanner.query.AbstractQuery

object Main {
  def main(args: Array[String]) {
    //serialize
    val fos = new FileOutputStream("test.ser", false)
    val oos = new ObjectOutputStream(fos)

    val context = Factory.createContext
    context.addListener(new LoggingListener)
    context.addListener(new SerializeListener(oos))
    context.start()
    Thread.sleep(20000)
    context.stop()
    oos.close()
    fos.close()
    load()
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