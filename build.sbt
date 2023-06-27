val scala3Version = "3.2.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "isis-to-mongodb",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    exportJars := true,

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test,
    libraryDependencies += ("org.mongodb.scala" %% "mongo-scala-driver" % "4.7.2").cross(CrossVersion.for3Use2_13),
    libraryDependencies += "com.typesafe" % "config" % "1.4.2",
    libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.31",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.10",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.10"
  )