ThisBuild / organization := "piaolaidelangman"
ThisBuild / scalaVersion := "2.12.2"
ThisBuild / version      := "0.1.0"

Compile / scalaSource := baseDirectory.value / "."
Compile / unmanagedSourceDirectories += baseDirectory.value / "sparkEncryptFiles"

lazy val commonSettings = Seq(
  libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2",
  libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2",
  )

lazy val root = (project in file("."))
  .settings(
    name := "sparkDecryptFiles",
    commonSettings
  )
