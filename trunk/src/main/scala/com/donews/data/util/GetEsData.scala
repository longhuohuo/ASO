package com.donews.data.util

import java.sql.{Connection, DriverManager}
import java.time.LocalDate

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.collection.immutable.IndexedSeq

/**
  * Created by ChengXu&ChenLei on 2017/1/4.
  */
case class InitArgs(filter: String, duration_query: String)

object GetEsData {
  val LOG = LoggerFactory.getLogger(GetEsData.getClass)

  def tryDo(time: Int)(body: => Unit): Unit = {
    var i = 0
    var running = true
    while (running) {
      try {
        i += 1
        body
        running = false
      } catch {
        case e: Throwable if i < time =>
          LOG.error(e.getMessage)
          Thread.sleep(60000)
      }
    }
  }

  def saveHBase(ds: DataFrame, table: String): Unit = {
    ds.write.mode(SaveMode.Overwrite).options(
      Map("table" -> table, "zkUrl" -> "slave01:2181;slave02:2181;slave03:2181")
    ).format("org.apache.phoenix.spark").save()
  }

  def loadHBase(table: String)(implicit ctx: SQLContext): DataFrame = {
    val df = ctx.read.options(Map("table" -> table, "zkUrl" -> "slave01:2181;slave02:2181;slave03:2181")
    ).format("org.apache.phoenix.spark").load()
    df
  }

  def updateHBase(sql: String, args: Any*): Int = {
    update("HBase", sql, args: _*)
  }

  private def update(engine: String, sql: String, args: Any*): Int = {
    useConnection(engine) { cnn =>
      val ps = cnn.prepareStatement(sql)
      try {
        for ((arg, i) <- args.zipWithIndex) {
          ps.setObject(i + 1, arg)
        }
        ps.executeUpdate()
      } finally {
        ps.close()
      }
    }

  }

  private def useConnection[T](engine: String)(func: Connection => T): T = {
    val cnn = engine match {
      case "HBase" => DriverManager.getConnection("jdbc:phoenix:" + "slave01:2181;slave02:2181;slave03:2181")
    }
    try {
      cnn.setAutoCommit(true)
      func(cnn)
    } finally {
      cnn.close()
    }
  }

  def plusMinuteToString(m: Int, date: String): String = {
    new DateTime(date, DateTimeZone.UTC).plusMinutes(m).toString.substring(0, 19)
  }

  def getDataFrameReader(hivectx: HiveContext, query: String): DataFrameReader = {
    val options = Map("es.nodes" -> "10.28.49.4,10.28.49.54,10.28.49.29", "es.port" -> "9200", "es.query" -> query)
    hivectx.read.format("org.elasticsearch.spark.sql").options(options)
  }

  def getHiveContext(className: String): HiveContext = {
    val conf = new SparkConf()
    conf.set("es.scroll.size", "500")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val prefixName = conf.get("spark.app.name")
    conf.setAppName(s"$prefixName  - " + className)
    val sc = new SparkContext(conf)
    new HiveContext(sc)
  }

  def init(interval: Array[String], ctx: HiveContext): (String, String) = {
    if (interval.length < 1) {
      System.exit(1)
    }
    var lt = ""
    val replaceTGte = interval(0)
    var filter = "updatetime >='" + replaceTGte + "'"
    if (interval.length > 1) {
      lt ="""","lt": """" + interval(1)
      val replaceTLt = interval(1)
      filter = "updatetime >='" + replaceTGte + "' and updatetime< '" + replaceTLt + "'"
    }

    //val delete_filter=" updatetime <'" + replaceTGte + "'"
    ctx.udf.register("plusMinuteToString", (a: Int, b: String) => plusMinuteToString(a, b))
    (replaceTGte.substring(0,10),filter)
  }

  def dateRange(startDay: LocalDate, endDay: LocalDate): IndexedSeq[LocalDate] = {
    Range.Long(startDay.toEpochDay, endDay.toEpochDay, 1).map { x => LocalDate.ofEpochDay(x) }
  }

  def main(interval: Array[String]): Unit = {
    //  println(new DateTime("2017-01-16 10:45".replace(" ", "T"), DateTimeZone.UTC).minusMinutes(5).toString)
    println("2017-01-16 17:44:02".compareTo("2017-01-16 17:45"))
  }
}
