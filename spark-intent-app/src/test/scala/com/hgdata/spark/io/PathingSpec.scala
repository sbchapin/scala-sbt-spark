package com.hgdata.spark.io

import java.nio.file.Paths

import org.scalatest.FunSpec

class PathingSpec extends FunSpec {

  describe("A Pathing object converting a relative path") {

    val currentAbsolutePath = Paths.get(".").toAbsolutePath.toString

    it("should resolve paths starting with './'") {
      assert(Pathing.relativeToAbsolute("./example-relative") == s"$currentAbsolutePath/example-relative")
    }

    it("should not resolve absolute paths") {
      assert(
        Pathing.relativeToAbsolute("/example-absolute") == "/example-absolute" &&
        Pathing.relativeToAbsolute("/example-absolute/") == "/example-absolute/" &&
        Pathing.relativeToAbsolute("file:///example-absolute/") == "file:///example-absolute/"
      )
    }

    it("should not resolve s3 paths") {
      assert(
        Pathing.relativeToAbsolute("s3://example-s3") == "s3://example-s3" &&
        Pathing.relativeToAbsolute("s3://example-s3/") == "s3://example-s3/"
      )
    }
  }

  describe("A Pathing object parsing a date partition") {

    val baseS3Path = "s3://example-bucket/example-prefix"

    describe("without separators") {

      it("should parse dotted date partitions") {
        assert(Pathing.getDatePartition(baseS3Path + "/20200101/").contains("20200101"))
      }
    }

    describe("with non-uniform separators") {

      it("should not parse") {
        assert(
          Pathing.getDatePartition(baseS3Path + "/2020-01_01/").isEmpty
            && Pathing.getDatePartition(baseS3Path + "/2020_0101/").isEmpty
            && Pathing.getDatePartition(baseS3Path + "/202001-01/").isEmpty
            && Pathing.getDatePartition(baseS3Path + "/2020.01-01/").isEmpty
        )
      }
    }

    describe("with uniform separators") {

      it("should parse dotted date partitions") {
        assert(Pathing.getDatePartition(baseS3Path + "/2020.01.01/").contains("2020.01.01"))
      }

      it("should parse underscored date partitions") {
        assert(Pathing.getDatePartition(baseS3Path + "/2020_01_01/").contains("2020_01_01"))
      }

      it("should parse dasherized date partitions") {
        assert(Pathing.getDatePartition(baseS3Path + "/2020-01-01/").contains("2020-01-01"))
      }

      it("should parse slashed date partitions") {
        assert(Pathing.getDatePartition(baseS3Path + "/2020/01/01/").contains("2020/01/01"))
      }
    }

    it("should parse paths that do not end with '/'") {
      assert(Pathing.getDatePartition(baseS3Path + "/2020-01-01").contains("2020.01.01"))
    }

    it("should parse suffix along with date partitions") {
      assert(Pathing.getDatePartition(baseS3Path + "/2020-01-01-suffix/").contains("2020-01-01-suffix"))
    }

    it("should parse prefix along with date partitions") {
      assert(Pathing.getDatePartition(baseS3Path + "/prefix-2020-01-01/").contains("prefix-2020-01-01"))
    }

    it("should parse various real-life scenarios") {
      assert(Pathing.getDatePartition("s3://hg-mrd-pipeline/delivery/2020-10-20/data_ops/alternate_urls/20201001000000").contains("2020-10-20"))
    }

  }
}
