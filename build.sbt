ThisBuild / name := "scala-sbt-spark"
ThisBuild / scalaVersion := "2.11.11"
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
    sparkIntentApp
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
    libraryDependencies ++= sparkDeps
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
    libraryDependencies ++= appDeps
  )

// Application: Standard Spark
lazy val sparkIntentApp = (project in file("spark-intent-app"))
  .dependsOn(common)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "spark-intent-app",
    description :=
      """Spark app to prep intent data, for usage with spark-submit.
        |
        |Example usage:
        |  spark-submit spark-intent-app.jar --help
        |""".stripMargin,
    version := "0.0.0",
    // Settings:
    baseSettings,
    assemblySettings,
    buildInfoSettings,
    Test / parallelExecution := false, // Concurrent spark tests
    // Dependencies:
    libraryDependencies ++= baseDeps,
    libraryDependencies ++= sparkDeps,
    libraryDependencies ++= hudiDeps,
    libraryDependencies ++= appDeps
  )

// DEPENDENCIES:

/** Dependencies for all projects */
lazy val baseDeps = Seq(
  // Logging interface:
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.slf4j" % "slf4j-api" % "1.7.26",                              // slf4j as logging interface

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
  "org.apache.spark" %% "spark-core" % "2.4.4" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.4" % Provided,
)
/** Dependencies for Hudi and friends */
lazy val hudiDeps = Seq(
  // Hudi
  "org.apache.hudi" %% "hudi-spark-bundle" % "0.5.3",
  "org.apache.spark" %% "spark-avro" % "2.4.4", // for hudi
  "org.apache.httpcomponents" % "httpclient" % "4.3.3", // for hudi
)
/** Dependencies for Applications (not libraries) */
lazy val appDeps = Seq(
  // Arg parsing:
  "info.picocli" % "picocli" % "4.5.0"
)

// TASKS:

lazy val sparkSubmit = inputKey[Unit]("Execute spark-submit with the assembled application.  Pass arguments as you would to spark-submit.")

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
    case "overview.html" => MergeStrategy.discard
    case x if x.endsWith("/module-info.class") => MergeStrategy.discard
    case x if x.endsWith("/spark/unused/UnusedStubClass.class") => MergeStrategy.discard
    // take last...
    case PathList("org", "apache", "http", _*) => MergeStrategy.last
    // ...otherwise default to old strategy:
    case x => (assemblyMergeStrategy in assembly).value(x)
  },

  // Custom tasks to run spark-submit on assembled jars.
  // ex: sbt sparkSubmit --help
  sparkSubmit := {
    import scala.sys.process._
    import complete.DefaultParsers._
    // Gather all args after the sparksSubmit task
    val args = spaceDelimited("<arg>").parsed
    // Run assembly
    val assemblyOutput = (Compile / assembly).value
    s"spark-submit ${assemblyOutput.getPath} ${args.mkString(" ")}" !
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
    BuildInfoOption.ConstantValue
  ),
  buildInfoPackage := "com.hgdata.generated",
)