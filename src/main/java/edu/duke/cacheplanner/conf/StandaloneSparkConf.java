package edu.duke.cacheplanner.conf;

public class StandaloneSparkConf extends BasicSparkConf {

  String memoryWorker = "10g";
  String totalMaxCores = "80";

  public StandaloneSparkConf(String name) {
    super(name);
    conf.setMaster(System.getenv("MASTER"));
    conf.set("spark.scheduler.mode", "FAIR");
    // HACK: assuming that internal file has same pool names as corresponding queue id
    // Also assuming weights and min shares match. 
    // Ideally there should be a single config file
    conf.set("spark.scheduler.allocation.file", "conf/internal.xml");

    conf.set("spark.executor.memory", memoryWorker);
    conf.set("spark.cores.max", totalMaxCores);
    // this fraction makes cache space about 10GB, but we are going to use only 5GB for algorithms
    conf.set("spark.memory.useLegacyMode", "true");
//TODO: Find a way to read config and set the following parameter
//    if(config.getCachePartitioningStrategy.equals("greedy")) {
//      conf.set("spark.storage.memoryFraction", "0.05"); // except for this case where we allow each query to cache and rely on LRU
//    } else {
      conf.set("spark.storage.memoryFraction", "0.1");
//    }
    // speculative
    conf.set("spark.speculation", "true");
    conf.set("spark.speculation.multiplier", "2");
    conf.set("spark.speculation.interval", "1s");
  }

}
