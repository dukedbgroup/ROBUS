package edu.duke.cacheplanner.listener

import com.google.gson.Gson
import java.io.PrintWriter
import java.io.File

/**
 * an example of concrete listener that handles events
 */
class SerializeListener(path: String) extends Listener {
  val writer = new PrintWriter(new File(path))
  val gson = new Gson()
  override def onQuerySerialize(event: QuerySerialize) {
    val string = gson.toJson(event.query)
    writer.println(string)
    writer.flush()
    println(event.query.toHiveQL(false) + " is written")
  }
}