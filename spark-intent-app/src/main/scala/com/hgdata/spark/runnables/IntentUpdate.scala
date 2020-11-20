package com.hgdata.spark.runnables

import com.hgdata.spark.io.{DeltaReader, HolisticReader, Writer}
import org.apache.spark.sql.functions._

class IntentUpdate(preppedIntentReader: DeltaReader, alternateUrlReader: HolisticReader, writer: Writer) extends Runnable {

  private lazy val preppedIntent = preppedIntentReader.read
  private lazy val alternateUrls = alternateUrlReader.read

  override def run(): Unit = {
    val intent = preppedIntent.join(
      broadcast(alternateUrls),
      preppedIntent("domain") === alternateUrls("alternate_url"),
      "left"
    )
    writer.write(intent)
  }
}
