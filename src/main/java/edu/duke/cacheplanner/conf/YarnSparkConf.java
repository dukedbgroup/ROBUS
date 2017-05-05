package edu.duke.cacheplanner.conf;

public class YarnSparkConf extends BasicSparkConf {

  public YarnSparkConf(String name) {
    super(name);
    conf.setMaster("yarn-client");
    conf.set("spark.executor.instances", "10");
    conf.set("spark.executor.memory", "1g");
    conf.set("spark.cores.max", "40");
    conf.set("spark.driver.memory", "4g");
  }

}
