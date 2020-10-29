package com.hgdata.spark.testutil

import java.nio.charset.StandardCharsets
import java.nio.file.Files

trait IOHelpers {


  /**
    * Create a temporary file that will be cleaned up by the JVM
    * @param contents What to put in the file (UTF-8)
    * @return a path to the file
    */
  def createTmpFile(contents: String): String = {
    val tmpFile = Files.createTempFile("test", "tmp")
    Files.deleteIfExists(tmpFile)
    Files.write(tmpFile, contents.getBytes(StandardCharsets.UTF_8))
    tmpFile.toString
  }

}
