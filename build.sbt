name := "aditya_sawant-hw3"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies +="org.slf4j" % "slf4j-api" % "1.7.5"
libraryDependencies +="org.slf4j" % "slf4j-simple" % "1.7.5"
libraryDependencies +="com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
libraryDependencies += "junit" % "junit" % "4.12"

libraryDependencies += "com.novocode" % "junit-interface" % "0.8" % "test->default"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}