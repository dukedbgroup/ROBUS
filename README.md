ROBUS on Tachyon
--------------

This version is dependent on Spark-1.5.1 compiled with Scala-2.11. One way to get this dependency would be to assemble Spark in the following way:
```sh
./dev/change-scala-version.sh 2.11
sbt/sbt -Pyarn -Phadoop-2.6 -Dscala-2.11 -DskipTests clean package assembly publish-local
```

Tachyon 0.7.1 needs to be running on the cluster. Follow the instructions here: http://alluxio.org/documentation/v0.7.1/Running-Tachyon-on-a-Cluster.html


In order to create eclipse project, run the following command from project home:

```sh
sbt/sbt eclipse
```

