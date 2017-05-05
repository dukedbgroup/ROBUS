~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.Micro \
--master yarn --deploy-mode client \
--num-executors 20 \
--driver-memory 4G --executor-memory 4G --executor-cores 8 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.yarn.executor.memoryOverhead=2G \
--conf spark.memory.offHeap.enabled=false --conf spark.memory.offHeap.size=2G \
--conf "spark.executor.extraJavaOptions=-XX:NewRatio=10 -XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Wordcount/Input" 0.5 2 

