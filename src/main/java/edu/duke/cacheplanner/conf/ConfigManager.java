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
  
  public String getAlgorithmSetup() {
  	return config.get("cacheplanner.algorithm.setup");
  }

}
