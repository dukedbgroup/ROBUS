hadoop dfs -rmr /output
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.SortDF \
--master yarn --deploy-mode client \
--num-executors 10 \
--driver-memory 4G --executor-memory 6G --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=2G \
--conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
--jars /home/mayuresh/statsd-jvm-profiler/target/statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Sort/Input" "hdfs://xeno-62:9000/output"

<<C
hadoop dfs -rmr /output
~/spark-2.0.1/bin/spark-submit \
--class org.apache.spark.examples.Sort \
--master yarn --deploy-mode client \
--num-executors 20 \
--driver-memory 4G --executor-memory 4G --executor-cores 4 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://xeno-62:9000/sparkEventLog \
--conf spark.memory.fraction=0.96 \
--conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark" \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:NewRatio=10 -javaagent:statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar=server=152.3.145.71,port=8086,reporter=InfluxDBReporter,database=sort204G96NR10,username=profiler,password=profiler,prefix=thoth-test.SortByKey,tagMapping=namespace.application" \
--jars /home/mayuresh/statsd-jvm-profiler/target/statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar \
/home/mayuresh/spark-2.0.1/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.1.jar "hdfs://xeno-62:9000/Sort/Input" "hdfs://xeno-62:9000/output"
C
