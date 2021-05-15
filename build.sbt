name := "DStreamSpark"
version := "0.1"
scalaVersion := "2.11.12"
val projectName = "DStreamSpark"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
)


libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.12.1"
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.7.1"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.8"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.play" % "play-json_2.11" % "2.4.6"
libraryDependencies += "net.liftweb" %% "lift-json" % "2.6-M4"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.5.32"

