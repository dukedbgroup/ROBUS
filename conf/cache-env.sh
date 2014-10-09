#!/usr/bin/env bash

######################### SPARK RELATED ##########################

export SPARK_HOME="/home/home2/mayuresh/backup_bigframe/spark-1.1.0-bin-hadoop1"

#export HADOOP_HOME="/usr/hadoop-1.2.1"

#export SCALA_HOME="/usr/scala-2.10.4"

export SPARK_MEM=2g

export MASTER="spark://okra:7077"

export HIVE_HOME="/home/home2/mayuresh/backup_bigframe/hive-0.11.0"

export HIVE_CONF_DIR="$HIVE_HOME/conf"
