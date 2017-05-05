package edu.duke.cacheplanner.conf;

import scala.collection.JavaConversions;
import org.apache.spark.SparkConf;

public class BasicSparkConf {

  static String assemblyJar = "target/scala-2.11/CachePlanner-assembly-0.1.jar";
  static String eventLogDir = "hdfs://xeno-62:9000/sparkEventLog";

  SparkConf conf = new SparkConf();

  public BasicSparkConf(String name) {
    conf.setAppName(name);
    java.util.List<String> jars = new java.util.ArrayList<String>();
    jars.add(assemblyJar);
    conf.setJars(JavaConversions.asScalaBuffer(jars).toSeq());
    conf.set("spark.eventLog.enabled", "true");
    conf.set("spark.eventLog.dir", eventLogDir);
  }

  public SparkConf getConf() {
    return conf;
  }

}
