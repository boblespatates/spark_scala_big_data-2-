name := "scenarioBigData"

version := "0.1"

scalaVersion := "2.11.8"

//resolvers += "Hortonworks repo" at "http://repo.hortonworks.com/content/repositories/releases/"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % sparkVersion,
"org.apache.spark" %% "spark-sql" % sparkVersion,
"org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
"org.apache.spark" %% "spark-streaming-kafka-0-8-assembly" % "2.1.1",
"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"
)

//"com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.0.1.0-168"
