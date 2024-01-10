ThisBuild / version := "0.1.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion: String = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

lazy val root = (project in file("."))
  .settings(
    name := "FreeCourse"
  )
