package com.hgdata.spark

import java.time.ZoneId
import java.util.{Calendar, TimeZone}

import org.scalatest.FunSpec
import picocli.CommandLine

class MainCLISpec extends FunSpec {
  describe("The entrypoint's CLI") {

    val main = Main
    val intentPrepSubcommand = "intent-prep"
    val altUrlDeltifySubcommand = "alternate-url-deltify"
    val metrolLookupDeltifySubcommand = "metro-lookup-deltify"
    val intentUpdateSubcommand = "intent-update"
    val inputArgs = Array("--input", "i")
    val outputArgs = Array("--output", "o")
    val outputDatabaseArgs = Array("--output-database", "od")
    val outputTableArgs = Array("--output-table", "ot")

    describe("when parsing args with picocli") {

      describe(s"with an `$intentPrepSubcommand` subcommand") {
        val baseArgs = Array(intentPrepSubcommand) ++ inputArgs ++ outputArgs

        it("should fail if passed just the subcommand") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(intentPrepSubcommand)
          }
        }

        it("should fail if passed just the subcommand and --input") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(intentPrepSubcommand) ++ inputArgs: _*)
          }
        }

        it("should fail if passed just the subcommand and --output") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(intentPrepSubcommand) ++ outputArgs: _*)
          }
        }

        it("should be able to parse required --output") {
          main.commandLine.parseArgs(baseArgs: _*)
          assert(main.IntentPrepSubcommand.inputPath == "i" && main.IntentPrepSubcommand.outputPath == "o")
        }

        it("should be able to parse --output-database") {
          main.commandLine.parseArgs(baseArgs ++ outputDatabaseArgs: _*)
          assert(main.IntentPrepSubcommand.outputHiveDatabase == "od")
        }

        it("should be able to parse --output-table") {
          main.commandLine.parseArgs(baseArgs ++ outputTableArgs: _*)
          assert(main.IntentPrepSubcommand.outputHiveTable == "ot")
        }

        it("should be able to parse --help") {
          val result = main.commandLine.parseArgs("--help")
          assert(result.isUsageHelpRequested)
        }

        it("should be able to parse --version") {
          val result = main.commandLine.parseArgs("--version")
          assert(result.isVersionHelpRequested)
        }
      }

      describe(s"with an `$altUrlDeltifySubcommand` subcommand") {

        val baseArgs = Array(altUrlDeltifySubcommand) ++ inputArgs ++ outputArgs

        it("should fail if passed just the subcommand") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(altUrlDeltifySubcommand)
          }
        }

        it("should fail if passed just the subcommand and --input") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(altUrlDeltifySubcommand) ++ inputArgs: _*)
          }
        }

        it("should fail if passed just the subcommand and --output") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(altUrlDeltifySubcommand) ++ outputArgs: _*)
          }
        }

        it("should be able to parse required --input and --output") {
          main.commandLine.parseArgs(baseArgs: _*)
          assert(main.AlternateUrlPrepSubcommand.inputPath == "i" && main.AlternateUrlPrepSubcommand.outputPath == "o")
        }

        it("should be able to parse --output-database") {
          main.commandLine.parseArgs(baseArgs ++ outputDatabaseArgs: _*)
          assert(main.AlternateUrlPrepSubcommand.outputHiveDatabase == "od")
        }

        it("should be able to parse --output-table") {
          main.commandLine.parseArgs(baseArgs ++ outputTableArgs: _*)
          assert(main.IntentPrepSubcommand.outputHiveTable == "ot")
        }
      }

      describe(s"with an `$metrolLookupDeltifySubcommand` subcommand") {

        val baseArgs = Array(metrolLookupDeltifySubcommand) ++ outputArgs

        it("should fail if passed just the subcommand") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(metrolLookupDeltifySubcommand)
          }
        }

        it("should be able to parse required --output") {
          main.commandLine.parseArgs(baseArgs: _*)
          assert(main.MetroLookupPrepSubcommand.outputPath == "o")
        }

        it("should be able to parse --output-table") {
          main.commandLine.parseArgs(baseArgs ++ outputTableArgs: _*)
          assert(main.IntentPrepSubcommand.outputHiveTable == "ot")
        }
      }

      describe(s"with an `$intentUpdateSubcommand` subcommand") {

        val inputAUArgs = Array("--input-alternate-urls-path", "au")
        val inputMLArgs = Array("--input-metro-lookup-path", "ml")
        val inputIPArgs = Array(
          "--input-prepped-intent-path", "pi",
          "--input-prepped-intent-since", "1999-12-31T23:59:59+00:00", // 1 second before Y2k
          "--input-prepped-intent-until", "2000-01-01T00:00:00+00:00" // 1 second after Y2k
        )
        val baseArgs = Array(intentUpdateSubcommand) ++ inputAUArgs ++ inputMLArgs ++ inputIPArgs ++ outputArgs

        it("should fail if passed just the subcommand") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(intentUpdateSubcommand)
          }
        }

        it("should fail if passed just the subcommand and alternate urls input arg") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(intentUpdateSubcommand) ++ inputAUArgs: _*)
          }
        }

        it("should fail if passed just the subcommand and metro lookup input arg") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(intentUpdateSubcommand) ++ inputMLArgs: _*)
          }
        }

        it("should fail if passed just the subcommand and intent prepped args") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(intentUpdateSubcommand) ++ inputIPArgs: _*)
          }
        }

        it("should fail if passed just the subcommand and output args") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(intentUpdateSubcommand) ++ outputArgs: _*)
          }
        }

        it("should be able to parse required alternate url, metro lookup, prepped intent input args, and output arg") {
          val y2kBuilder = new Calendar.Builder()
            .set(Calendar.YEAR, 2000)
            .setTimeZone(TimeZone.getTimeZone(ZoneId.of("GMT")))

          val y2kBefore = y2kBuilder.build()
          y2kBefore.add(Calendar.SECOND, -1) // 1 second before, in 1999
          val y2kAfter = y2kBuilder.build()
          y2kAfter.add(Calendar.SECOND, -1) // 1 second before, in 1999

          main.commandLine.parseArgs(baseArgs: _*)
          assert(
            main.IntentUpdateSubcommand.alternateUrlInputPath == "au" &&
            main.IntentUpdateSubcommand.metroLookupInputPath == "ml" &&
            main.IntentUpdateSubcommand.preppedIntentInputPath == "pi" &&
            main.IntentUpdateSubcommand.outputPath == "o" &&
            main.IntentUpdateSubcommand.preppedIntentInputSince == y2kBefore.toInstant &&
            main.IntentUpdateSubcommand.preppedIntentInputSince == y2kAfter.toInstant
          )
        }

        it("should be able to parse --output-database") {
          main.commandLine.parseArgs(baseArgs ++ outputDatabaseArgs: _*)
          assert(main.IntentUpdateSubcommand.outputHiveDatabase == "od")
        }

        it("should be able to parse --output-table") {
          main.commandLine.parseArgs(baseArgs ++ outputTableArgs: _*)
          assert(main.IntentPrepSubcommand.outputHiveTable == "ot")
        }
      }

    }
  }
}
