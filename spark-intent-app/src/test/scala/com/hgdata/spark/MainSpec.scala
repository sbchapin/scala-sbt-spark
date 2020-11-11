package com.hgdata.spark

import org.scalatest.FunSpec
import picocli.CommandLine

class MainSpec extends FunSpec {
  describe("The entrypoint") {

    val main = Main
    val intentPrepSubcommand = "intent-prep"
    val urlAliasDeltifySubcommand = "url-alias-deltify"
    val inputArgs = Array("--input", "i")
    val outputArgs = Array("--output", "o")
    val outputDatabaseArgs = Array("--output-database", "od")

    describe("when parsing args with picocli") {

      describe(s"with an `$urlAliasDeltifySubcommand` subcommand") {

        val baseOptions = Array(urlAliasDeltifySubcommand) ++ inputArgs ++ outputArgs

        it("should fail if passed just the subcommand") {
          assertThrows[CommandLine.MissingParameterException] {
            main.commandLine.parseArgs(urlAliasDeltifySubcommand)
          }
        }


      }

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
    }
  }
}
