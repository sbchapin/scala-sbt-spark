package com.hgdata.spark.testutil

object IntentFixtures {
  val header = """Company Name,Domain,Size,Industry,Category,Topic,Composite Score,Metro Area,Metro Composite Score,Domain Origin,Date Stamp,HVC Level1 Trend"""
  val hgRow = """"HG ""Insights"", LLC",hginsights.com,,,programming languages,scala,80,,0,united states,2020-10-17,"""
  def of(rows: String*): String = (header ++ rows).mkString("\n")
}
