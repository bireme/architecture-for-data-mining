val scala3Version = "3.2.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "isis-to-mongodb",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    scalacOptions += "-deprecation",

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test,
    libraryDependencies += ("org.mongodb.scala" %% "mongo-scala-driver" % "4.7.2").cross(CrossVersion.for3Use2_13),
    libraryDependencies += "com.typesafe" % "config" % "1.4.2"
  )