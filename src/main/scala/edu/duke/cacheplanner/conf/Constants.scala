package edu.duke.cacheplanner.conf

object Constants {
  //External Queue
  final val QUEUE = "queue"
  final val ID = "id"
  final val WEIGHT = "weight"
  final val MIN_SHARE = "minShare"
  final val BATCH_SIZE = "batchSize"
  
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
  
  //distribution
  final val QUERY_DISTRIBUTION = "queryDistribution"
  final val QUEUE_DISTRIBUTION = "queueDistribution"
  final val DATA_DISTRIBUTION = "datasetDistribution"
    
  //queue xml
  final val PROBABILITY = "prob"
  final val EXTERNAL_QUEUE = "externalQueue"
    
  //generator xml
  final val QUEUEID = "queueId"
  final val LAMBDA = "lambda"
  final val GENERATORS = "generators"
  final val GENERATOR = "generator"
  final val MEAN_COLUMN = "meanColNum"
  final val STD_COLUMN = "stdColNum"

  final val TYPE = "type"
  final val GROUPING_PROBABILITY = "groupingQueryProb"
}

