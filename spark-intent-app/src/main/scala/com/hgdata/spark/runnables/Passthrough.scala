package com.hgdata.spark.runnables

import com.hgdata.spark.io.{Reader, Writer}

class Passthrough(reader: Reader, writer: Writer) extends Runnable {
  override def run(): Unit = {
    writer.write(reader.read)
  }
}
