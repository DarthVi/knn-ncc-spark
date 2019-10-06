name := "knn-ncc-spark"

version := "1.0"

scalaVersion := "2.11.12"

//extra dependencies
val sparkVersion = "2.4.3"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
