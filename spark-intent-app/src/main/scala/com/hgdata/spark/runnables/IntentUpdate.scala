package com.hgdata.spark.runnables

import com.hgdata.spark.io.{DeltaReader, HolisticReader, Writer}
import org.apache.spark.sql.functions._

class IntentUpdate(preppedIntentReader: DeltaReader,
                   alternateUrlReader: HolisticReader,
                   metroLookupReader: HolisticReader,
                   writer: Writer) extends Runnable {

  private lazy val preppedIntent = preppedIntentReader.read
  private lazy val alternateUrls = alternateUrlReader.read
  private lazy val metroLookup = metroLookupReader.read

  /** Enrich the partial intent dataset with all alternate URLs. */
  override def run(): Unit = {
    val intent = preppedIntent
      .join(
        broadcast(alternateUrls),
        preppedIntent("domain") === alternateUrls("alternate_url"),
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
        alternateUrls("alternate_url"),
        coalesce(alternateUrls("url"), preppedIntent("domain")).as("url"),
        alternateUrls("alternate_url_type"),
        // Metro:
        metroLookup("city_1"),
        metroLookup("city_2"),
        metroLookup("city_3"),
        metroLookup("state"),
        metroLookup("country_code"),
        metroLookup("country"),
        metroLookup("metro_version")
      )
      .withColumnRenamed("domain", "intent_domain")
    writer.write(intent)
  }
}
