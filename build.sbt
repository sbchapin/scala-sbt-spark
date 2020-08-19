ThisBuild / name := "scala-sbt-spark"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / organization := "com.hgdata"
ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings")
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

// PROJECTS:

// Build system:  Control all subprojects
lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    common,
    sparkApp,
  )

// Library: Common behavior among projects
lazy val common = (project in file("common"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "common",
    description := s"Shared behavior among ${name.value} projects",
    // Settings:
    baseSettings,
    // Dependencies:
    libraryDependencies ++= baseDeps,
    libraryDependencies ++= sparkDeps,
  )

// Application: Standard Spark
lazy val sparkApp = (project in file("spark-app"))
  .dependsOn(common)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "spark-app",
    description :=
      """Standard spark application, for usage with spark-submit.
        |
        |Example usage:
        |  spark-submit spark-app.jar --help
        |""".stripMargin,
    version := "0.0.0",
    // Settings:
    baseSettings,
    assemblySettings,
    buildInfoSettings,
    // Dependencies:
    libraryDependencies ++= baseDeps,
    libraryDependencies ++= sparkDeps,
    libraryDependencies ++= Seq(
      "info.picocli" % "picocli" % "4.5.0"
    ),
  )

// DEPENDENCIES:

/** Dependencies for all projects */
lazy val baseDeps = Seq(
  // Logging interface:
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.slf4j" % "slf4j-api" % "1.7.25",                              // slf4j as logging interface

  // Testing:
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,

  // Logging mechanism for testing:
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.12.1" % Test, // bridge: slf4j -> log4j
  "org.apache.logging.log4j" % "log4j-api" % "2.12.1" % Test,        // log4j as logging mechanism
  "org.apache.logging.log4j" % "log4j-core" % "2.12.1" % Test,       // log4j as logging mechanism
)
/** Dependencies for Spark and friends */
lazy val sparkDeps = Seq(
  // Spark:
  "org.apache.spark" %% "spark-core" % "3.0.0" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.0.0" % Provided,
)

// SETTINGS:

/** These settings allow projects that use them to...
  * - Enable sane features for scalac
  * - Use local cache resolver for maven
  * - Use Sonatype releases and snapshots
  */
lazy val baseSettings = Seq(
  scalacOptions ++= Seq(
    "-unchecked",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-deprecation",
    "-encoding", "utf8",
  ),
  resolvers ++= Seq(
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
  )
)

/** These settings allow projects that use them to...
  * - Build fat jars using `sbt assembly`
  * - Name their application jar simply projectname.jar
  * - Provide a sane and complete merge strategy for conflicting files when making the fat jar
  */
lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    // discard:
    case x if x.endsWith("/module-info.class") => MergeStrategy.discard
    // ...otherwise default to old strategy:
    case x => (assemblyMergeStrategy in assembly).value(x)
  }
)

/** These settings allow projects that use them to...
  * - Use the name, description, verison, and build time within the context of the program
  * - Not recklessly repeat strings found all over the build
  * - import com.hgdata.generated.BuildInfo
  */
lazy val buildInfoSettings = Seq(
  buildInfoKeys := Seq[BuildInfoKey](
    name,
    description,
    version,
    scalaVersion,
    sbtVersion,
  ),
  buildInfoOptions ++= Seq(
    BuildInfoOption.BuildTime,
    BuildInfoOption.ConstantValue,
  ),
  buildInfoPackage := "com.hgdata.generated",
)