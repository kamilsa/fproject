name := "ScalaDemo"

version := "1.0"

scalaVersion := "2.11.7"

unmanagedJars in Compile += file("lib/spark-assembly-1.6.2-hadoop2.4.0.jar")
unmanagedJars in Compile += file("lib/dbscan-0.1.jar")
unmanagedJars in Compile += file("lib/spatialpartitioner_2.11-0.1-SNAPSHOT.jar")

libraryDependencies += "com.assembla.scala-incubator" %% "graph-core" % "1.9.4"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"

//libraryDependencies += "org.cluster" %% "dbscan" % "0.1"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-assembly" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-bagel" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-docker-integration-tests" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-launcher" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-network-shuffle" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-parent" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-repl" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-flume-sink" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-flume" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-mqtt-assembly" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-mqtt" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-zeromq" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-test-tags" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-tools" % "1.6.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-unsafe" % "1.6.2" % "provided"
//libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1"
//libraryDependencies += "org.scala-lang" %% "scala-compiler" % "2.11.7"
//libraryDependencies += "jline" %% "jline" % "2.12.1"
//libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinations" % "1.0.4"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.4"