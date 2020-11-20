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
    val newIntentSubcommand = "new-intent"
    val inputArgs = Array("--input", "i")
    val outputArgs = Array("--output", "o")
    val outputDatabaseArgs = Array("--output-database", "od")

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

        it("should be able to parse required --input and --output") {
          main.commandLine.parseArgs(baseArgs: _*)
          assert(main.IntentPrepSubcommand.inputPath == "i" && main.IntentPrepSubcommand.outputPath == "o")
        }

        it("should be able to parse --output-database") {
          main.commandLine.parseArgs(baseArgs ++ outputDatabaseArgs: _*)
          assert(main.IntentPrepSubcommand.outputHiveDatabase == "od")
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
      }

      describe(s"with an `$newIntentSubcommand` subcommand") {

        val inputAUArgs = Array("--input-alternate-urls-path", "au")
        val inputIPArgs = Array("--input-prepped-intent-path", "pi", "--input-prepped-intent-since", "19991231235959") // 1 second before Y2k
        val baseArgs = Array(newIntentSubcommand) ++ inputAUArgs ++ inputIPArgs ++ outputArgs

        it("should fail if passed just the subcommand") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(newIntentSubcommand)
          }
        }

        it("should fail if passed just the subcommand and alternate urls input arg") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(newIntentSubcommand) ++ inputAUArgs: _*)
          }
        }

        it("should fail if passed just the subcommand and intent prepped args") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(newIntentSubcommand) ++ inputIPArgs: _*)
          }
        }


        it("should fail if passed just the subcommand and output args") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(Array(newIntentSubcommand) ++ outputArgs: _*)
          }
        }

        it("should be able to parse required alternate url, prepped intent input args, and output arg") {
          val y2k = new Calendar.Builder()
            .set(Calendar.YEAR, 2000)
            .setTimeZone(TimeZone.getTimeZone(ZoneId.of("GMT")))
            .build()
          y2k.add(Calendar.SECOND, -1) // 1 second before, in 1999

          main.commandLine.parseArgs(baseArgs: _*)
          assert(
            main.NewIntentSubcommand.alternateUrlInputPath == "au" &&
            main.NewIntentSubcommand.preppedIntentInputPath == "pi" &&
            main.NewIntentSubcommand.outputPath == "o" &&
            main.NewIntentSubcommand.preppedIntentInputSince == y2k.toInstant
          )
        }

        it("should be able to parse --output-database") {
          main.commandLine.parseArgs(baseArgs ++ outputDatabaseArgs: _*)
          assert(main.NewIntentSubcommand.outputHiveDatabase == "od")
        }
      }

    }
  }
}
