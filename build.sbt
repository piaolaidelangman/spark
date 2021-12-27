name := "sparkDecrypt"

version := "0.1"

scalaVersion := "2.12.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2"
libraryDependencies += "com.macasaet.fernet" % "fernet-java8" % "1.4.2"
