ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "sensors-statistics"
  )

libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor" % "2.8.0",
  "com.typesafe.akka" %% "akka-slf4j" % "2.8.0",
  "com.typesafe.akka" %% "akka-stream" % "2.8.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "5.0.0",

  "com.typesafe.akka" %% "akka-testkit" % "2.8.0" % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.8.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
)
