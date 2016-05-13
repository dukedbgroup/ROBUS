package edu.duke.cacheplanner.query

import edu.duke.cacheplanner.data.Dataset

class SalesQ(appName: String, memory: String, cores: String, datasetsCached: java.util.List[Dataset]) 
	extends SubmitQuery(appName, memory, cores) {

  def submit() {
  }

}
