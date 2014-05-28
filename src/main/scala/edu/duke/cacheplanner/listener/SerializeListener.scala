package edu.duke.cacheplanner.listener

import java.io.ObjectOutputStream
import edu.duke.cacheplanner.query.{GroupingQuery, SingleTableQuery}

/**
 * an example of concrete listener that handles events
 */
class SerializeListener(oos: ObjectOutputStream) extends Listener {
  override def onQuerySerialize(event: QuerySerialize) {
    oos.writeObject(event.query)
    println(event.query.toHiveQL(false) + " is written")
  }

}