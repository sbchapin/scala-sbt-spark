package com.hgdata.spark.runnables

import com.hgdata.spark.io.{DeltaReader, HolisticReader, Writer}
import org.apache.spark.sql.functions._

class IntentUpdate(preppedIntentReader: DeltaReader,
                   aliasUrlReader: HolisticReader,
                   metroLookupReader: HolisticReader,
                   writer: Writer) extends Runnable {

  private lazy val preppedIntent = preppedIntentReader.read
  private lazy val aliasUrls = aliasUrlReader.read
  private lazy val metroLookup = metroLookupReader.read

  /** Enrich the partial intent dataset with all alternate URLs. */
  override def run(): Unit = {
    val intent = preppedIntent
      .join(
        broadcast(aliasUrls),
        preppedIntent("domain") === aliasUrls("alternate_url"),
        "left"
      )
      .join(
        broadcast(metroLookup),
        preppedIntent("metro_area") === metroLookup("metro_area"),
        "left"
      )
      .select(
        preppedIntent("*"),
        // Alt URLs:
        aliasUrls("alternate_url"),
        coalesce(aliasUrls("url"), preppedIntent("domain")).as("url"),
        aliasUrls("alternate_url_type"),
        // Metro:
        metroLookup("metro_version"),
        metroLookup("country_code"),
        metroLookup("country"),
        metroLookup("state"),
        metroLookup("city_1"),
        metroLookup("city_2"),
        metroLookup("city_3")
      )
      .withColumnRenamed("domain", "intent_domain")
    writer.write(intent)
  }
}
