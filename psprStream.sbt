name := "PSPRwindow"

version := "1.0"

scalaVersion := "2.11.7"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % sparkVersion,
 "org.apache.spark" %% "spark-streaming" % sparkVersion,
 "org.apache.spark" %% "spark-mllib" % sparkVersion,
 "org.apache.spark" %% "spark-hive" % sparkVersion,
 "org.apache.spark" %% "spark-graphx" % sparkVersion,
 "org.apache.spark" %% "spark-streaming-flume" % sparkVersion,
 "org.apache.spark" %% "spark-mllib" % sparkVersion,
 "org.slf4j" % "slf4j-log4j12" % "1.2" % "test",

 "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.2.0",
 "org.mongodb.scala" % "mongo-scala-driver_2.11" % "1.2.1",
 "com.stratio.datasource" % "spark-mongodb_2.11" % "0.11.1",
 
// "org.apache.spark" % "spark-streaming-kafka-0-8" % sparkVersion,
// "org.apache.spark" % "spark-sql-kafka-0-10" % sparkVersion,
 "com.stratio.receiver" % "spark-rabbitmq" % "0.5.1",
 "com.github.scopt" %% "scopt" % "3.3.0",
 "com.typesafe.akka" %% "akka-actor" % "2.5.2",
 "com.typesafe.akka" %% "akka-testkit" % "2.5.2" % Test, 
 "com.typesafe" % "config" % "1.3.1",
 "com.rabbitmq" % "amqp-client" % "4.1.0"

)


