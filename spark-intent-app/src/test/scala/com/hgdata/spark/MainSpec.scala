package com.hgdata.spark

import org.scalatest.FunSpec
import picocli.CommandLine

class MainSpec extends FunSpec {
  describe("The entrypoint") {

    val main = Main
    val baseOptions = Array("--input", "a", "--output", "b")

    describe("when parsing args with picocli") {

      it("should be able to parse required --input and --output") {
        new CommandLine(main).parseArgs(baseOptions:_*)
        assert(main.inputPath == "a" && main.outputPath == "b")
      }

      it("should be able to parse --output-database") {
        new CommandLine(main).parseArgs(baseOptions ++ Array("--output-database", "d"):_*)
        assert(main.outputHiveDatabase == "d")
      }

      it("should be able to parse --help") {
        val result = new CommandLine(main).parseArgs("--help")
        assert(result.isUsageHelpRequested)
      }

      it("should be able to parse --version") {
        val result = new CommandLine(main).parseArgs("--version")
        assert(result.isVersionHelpRequested)
      }
    }
  }
}
