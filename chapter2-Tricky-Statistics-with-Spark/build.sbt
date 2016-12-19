import AssemblyKeys._

name := "SparkforDataScienceCookbook"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.2",
    "org.apache.spark" %% "spark-sql" % "1.6.2",
    "org.apache.spark" %% "spark-hive" % "1.6.2",
    "org.apache.spark" %% "spark-streaming" % "1.6.2",
    "org.apache.spark" %% "spark-mllib" % "1.6.2",
    "com.databricks" %% "spark-csv" % "1.5.0"
)

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

resolvers ++= Seq(
            "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/",
	    "Maven Central" at "https://repo1.maven.org/maven2/"
)
