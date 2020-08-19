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

      it("should be able to parse optional --spark-master") {
        new CommandLine(main).parseArgs(Array("--spark-master", "hello") ++ baseOptions :_*)
        assert(main.sparkMaster == "hello")
      }

      it("should be able to parse optional --spark-conf multiple times") {
        new CommandLine(main).parseArgs(Array("--spark-conf", "a=b", "--spark-conf", "c=d") ++ baseOptions :_*)
        assert(main.sparkConfig.get("a") == "b" && main.sparkConfig.get("c") == "d")
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
