# sort shuffle 32 cores, shuffle fraction 0.6
hadoop dfs -rmr /output

~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.WordCountDF \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory 1G --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/wordcount-huge" "hdfs://xeno-62:9000/output"

#~/spark/bin/spark-submit --class org.apache.spark.examples.WordCountDF --master yarn-client --num-executors 10 --driver-memory 4G --executor-memory 4G --executor-cores 4 --conf spark.shuffle.sort.bypassMergeThreshold=20 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog --conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" ~/spark/examples/target/scala-2.10/spark-examples-1.6.0-SNAPSHOT-hadoop2.6.0.jar "hdfs://xeno-62:9000/wordcount-huge" "hdfs://xeno-62:9000/output"
