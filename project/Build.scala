import sbt._
import Keys._

object DemoBuild extends Build {
  if (System.getenv("HIVE_HOME") == null) {
    System.err.println("You must set HIVE_HOME to compile this project")
    System.exit(1)
  }

  val SPARK_VERSION = "1.0.0"

  val SCALA_VERSION = "2.10.3"

  val HADOOP_VERSION = "1.2.1"

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = coreSettings)

  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")
  val excludeAsm = ExclusionRule(organization = "asm")

  def coreSettings = Defaults.defaultSettings ++ Seq(

    name := "CachePlanner",
    organization := "edu.duke",
    version := "0.1",
    scalaVersion := SCALA_VERSION,
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),

    // Download managed jars into lib_managed.
    retrieveManaged := true,
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Spray Repository" at "http://repo.spray.io/",
      "Cloudera Repository" at "http://repository.cloudera.com/artifactory/cloudera-repos/",
      "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
      "Akka Repository" at "http://repo.akka.io/releases/"
    ),

    fork := true,
    javaOptions += "-XX:MaxPermSize=512m",
    javaOptions += "-Xmx3g",

    unmanagedJars in Compile <++= baseDirectory map { base =>
      val hiveFile = file(System.getenv("HIVE_HOME")) / "lib"
      val baseDirectories = (base / "lib") +++ (hiveFile)
      val customJars = (baseDirectories ** "*.jar")
      // Hive uses an old version of guava that doesn't have what we want.
      customJars.classpath.filter(!_.toString.contains("guava"))
    },

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % SPARK_VERSION,
      "com.typesafe.akka" %% "akka-actor" % "2.2.3",
      "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION,
      "org.apache.commons" % "commons-math3" % "3.2",
      "org.apache.spark" %% "spark-hive" % SPARK_VERSION

  ),
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)
  )
}
