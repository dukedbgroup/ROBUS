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
		if(config.get("cacheplanner.algorithm.setup").equals("multi")) {
			return true;
		}
		else {
			return false;
		}
	}

	public String getReplayFilePath() {
		return config.get("cacheplanner.generator.replayfile");
	}

	public Long getPlannerBatchTime() {
		return Long.parseLong(config.get("cacheplanner.algorithm.batchtime"));
	}

	public String getAlgorithmMode() {
		return config.get("cacheplanner.algorithm.mode");
	}

	public Integer getCacheSize() {
		return Integer.parseInt(config.get("cacheplanner.cachesize"));	  
	}

	public Long getWorkloadTime() {
		return Long.parseLong(config.get("cacheplanner.workloadtime"));
	}
}
