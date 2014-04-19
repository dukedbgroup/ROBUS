Example to use the SharkContext API

Preparation to build this:

    wget http://www.scala-lang.org/files/archive/scala-2.9.3.tgz
    tar xvfz scala-2.9.3.tgz
    export SCALA_HOME=....

    wget https://github.com/amplab/shark/releases/download/v0.8.0/shark-0.8.0-bin-hadoop1.tgz
    tar xvfz shark-*-bin-*.tgz
    cd shark-*-bin-*

    export HIVE_HOME=/path/to/hive-0.9.0-shark-0.8.0-bin

    cd shark-0.8.0
    sbt/sbt publish-local


To build this project:

    sbt/sbt package


To run:

    ./run com.databricks.sharkexample.SharkExample


To create an IntelliJ IDEA workspace:

    sbt/sbt gen-idea
