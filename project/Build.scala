import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object CachePlannerBuild extends Build {

  val SPARK_VERSION = "1.0.1"

  val SCALA_VERSION = "2.10.4"

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
      "Akka Repository" at "http://repo.akka.io/releases/"
    ),

    fork := true,
    javaOptions += "-XX:MaxPermSize=512m",
    javaOptions += "-Xmx3g",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % SPARK_VERSION,
      "com.typesafe.akka" %% "akka-actor" % "2.2.3",
//      "org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION,
      "org.apache.commons" % "commons-math3" % "3.2",
      "org.apache.spark" %% "spark-hive" % SPARK_VERSION,
      "com.google.code.gson" % "gson" %"2.2.4",
      "mysql" % "mysql-connector-java" % "5.1.26"
  ),
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)
  ) ++ assemblySettings ++ extraAssemblySettings
  
  def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
	mergeStrategy in assembly := {
      case m if m startsWith "org/apache/commons/logging" => MergeStrategy.last
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

}
