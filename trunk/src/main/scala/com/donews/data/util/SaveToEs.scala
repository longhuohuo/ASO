package com.donews.data.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import org.elasticsearch.spark._

/**
  * Created by Administrator on 2017/1/19.
  */
object SaveToEs {
  val LOG = LoggerFactory.getLogger(SaveToEs.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val prefixName = conf.get("spark.app.name")
    conf.setAppName(s"$prefixName  - AsoAllProcessor")
    LOG.info("name {}", conf.get("spark.app.name"))
    val sc = new SparkContext(conf)
    val ctx = new HiveContext(sc)
    val filter = GetEsData.init(args, ctx)
    val query = ""
    val options = Map("es.nodes" -> "10.28.49.4,10.28.49.54,10.28.49.29", "es.port" -> "9200", "es.query" -> query)
    ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/hotsearch_words").registerTempTable("hotsearch_words")
    ctx.sql("")
      .rdd.saveToEs("app_store/hotsearch_list")
  }
}
