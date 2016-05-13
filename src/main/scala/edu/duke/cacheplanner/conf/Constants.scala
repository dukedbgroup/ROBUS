package edu.duke.cacheplanner.conf

object Constants {
  //External Queue
  final val QUEUE = "queue"
  final val ID = "id"
  final val WEIGHT = "weight"
  final val MIN_SHARE = "minShare"
  final val BATCH_SIZE = "batchSize"
  final val ZIPF_RANK_FILE = "datasetRanks"

  //zipf ranks on datasets
  final val DATASET_RANKS = "dataset_ranks"
  
  //general config xml
  final val PROPERTY = "property"
  final val NAME = "name"
  final val VALUE = "value"
  
  //dataset xml
  final val DATASETS = "datasets"
  final val DATASET = "dataset"
  final val PATH = "path"
  final val SIZE = "size"
  final val COlUMNS = "columns"
  final val COLUMN = "col"
  final val CACHE_SIZE = "cachesize"
  
  //distribution
  final val QUERY_DISTRIBUTION = "queryDistribution"
  final val QUEUE_DISTRIBUTION = "queueDistribution"
  final val DATA_DISTRIBUTION = "datasetDistribution"
    
  //queue xml
  final val PROBABILITY = "prob"
  final val EXTERNAL_QUEUE = "externalQueue"
    
  //generator xml
  final val QUEUE_ID = "queueId"
  final val QUEUE_NAME = "name"
  final val LAMBDA = "lambda"
  final val GENERATORS = "generators"
  final val GENERATOR = "generator"
  final val MEAN_COLUMN = "meanColNum"
  final val STD_COLUMN = "stdColNum"

  //tpch queries xml
  final val TPCH_QUERY = "query"
  final val TPCH_NAME = "name"
  final val TPCH_PATH = "path"
  final val TPCH_CACHED_PATH = "cachedPath"
  final val TPCH_BENEFIT = "benefit"
  final val TPCH_PROBABILITY = "probability"

  final val TYPE = "type"
  final val GROUPING_PROBABILITY = "groupingQueryProb"
}

