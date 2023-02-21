name := "Lab 2"
version := "1.0"
scalaVersion := "2.11.12"

scalastyleFailOnWarning := true

fork in run := true

val sparkVersion = "2.4.7"

resolvers ++= Seq(
  "osgeo" at "https://repo.osgeo.org/repository/release",
  "confluent" at "https://packages.confluent.io/maven"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.locationtech.geomesa" %% "geomesa-spark-jts" % "2.4.1",
  "com.uber" % "h3" % "3.6.4"
)
