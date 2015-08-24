name := "sddf"

organization := "de.unihamburg.vsis"

version := "0.1.0"

scalaVersion := "2.10.5"

// TODO remove this when web ui is disabled (possible in version 1.1.1)
parallelExecution in Test := false

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.1" % "provided"

libraryDependencies += "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.3"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"

libraryDependencies += "joda-time" % "joda-time" % "2.7"

libraryDependencies += "org.joda" % "joda-convert" % "1.7"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "com.opencsv" % "opencsv" % "3.3"

libraryDependencies += "com.quantifind" %% "wisp" % "0.0.4"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

// eclipse plugin
// link source code to the dependencies
EclipseKeys.withSource := true

// add resource scala resource folders to the eclipse source folders
//EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

// Assembly plugin
// skip tests during assembly
test in assembly := {}
