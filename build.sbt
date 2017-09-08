name := "MusicRecommend"

version := "0.1"

scalaVersion := "2.11.8"


//spark
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"


//hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.3"


//mongodb
libraryDependencies += "org.mongodb" % "casbah_2.11" % "3.1.1"


//kafka
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"




