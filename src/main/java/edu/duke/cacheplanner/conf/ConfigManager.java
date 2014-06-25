package edu.duke.cacheplanner.conf;

import java.util.Map;

public class ConfigManager {
  private Map<String, String> config;

  public ConfigManager(Map<String, String> map) {
    config = map;
  }

  public String getGeneratorMode() {
    return config.get("cacheplanner.generator.mode");
  }

  public boolean getAlgorithmSetup() {
    //true = multi app setup, false = single app setup
    if(config.get("cacheplanner.algorithm.setup").equals("online")) {
      return true;
    }
    else {
      return false;
    }
  }

  public String getAlgorithmMode() {
    return config.get("cacheplanner.algorithm.mode");
  }
}
