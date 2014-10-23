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

	@Deprecated public boolean getAlgorithmSetup() {
		//true = multi app setup, false = single app setup
		if(config.get("cacheplanner.algorithm.setup").equals("multi")) {
			return true;
		}
		return false;
	}

	public String getAlgorithmName() {
		return config.get("cacheplanner.algorithm.name");
	}

	public String getCacheState() {
		return config.get("cacheplanner.algorithm.cachestate");
	}

	public String getCachePartitioningStrategy() {
		return config.get("cacheplanner.algorithm.cachepartitioning");
	}

	public boolean getUseCache() {
		if(config.get("cacheplanner.algorithm.usecache").equals("true")) {
			return true;
		}
		return false;
	}

	public String getReplayFilePath() {
		return config.get("cacheplanner.generator.replayfile");
	}

	public String getLogDirPath() {
		return config.get("cacheplanner.log.dir");
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
