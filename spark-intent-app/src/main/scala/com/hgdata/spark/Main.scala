package com.hgdata.spark

import com.hgdata.generated.BuildInfo
import com.hgdata.picocli.{InputCommandLineOpts, OutputCommandlineOpts}
import com.hgdata.spark.runnables.{IntentPrep, Passthrough}
import org.apache.spark.sql.SparkSession
import picocli.CommandLine

@CommandLine.Command(
  name = BuildInfo.name,
  mixinStandardHelpOptions = true,
  version = Array(BuildInfo.version + " (" + BuildInfo.builtAtString + ")"),
  description = Array(BuildInfo.description)
)
object Main {

  @CommandLine.Command(name = "intent-prep")
  object IntentPrepSubcommand extends SparkRunnable with InputCommandLineOpts with OutputCommandlineOpts {
    override def run(): Unit = withDefaultSpark { implicit spark: SparkSession =>
      val prep = new IntentPrep(
        reader = readers.rawIntent,
        writer = writers.preppedIntentDelta
      )
      prep.run()
    }
  }

  @CommandLine.Command(name = "alternate-url-deltify")
  object AlternateUrlPrepSubcommand extends SparkRunnable with InputCommandLineOpts with OutputCommandlineOpts {
    override def run(): Unit = withDefaultSpark { implicit spark: SparkSession =>
      val deltify = new Passthrough(
        reader = readers.altUrl,
        writer = writers.alternateUrlDelta
      )
      deltify.run()
    }
  }

  private[spark] lazy val commandLine: CommandLine =
    new CommandLine(this)
      .addSubcommand(IntentPrepSubcommand)
      .addSubcommand(AlternateUrlPrepSubcommand)

  def main(args: Array[String]): Unit = {
    val exitCode = commandLine.execute(args:_*)
    if (exitCode != 0) throw new RuntimeException(s"Process exited with status code ${exitCode}")
  }
}
