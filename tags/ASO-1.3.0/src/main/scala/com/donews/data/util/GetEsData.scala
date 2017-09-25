package com.donews.data.util

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by ChengXu&ChenLei on 2017/1/4.
  */
case class InitArgs(delete_filter: String, filter: String, query: String)

object GetEsData {
  def saveHBase(ds: DataFrame, table: String): Unit = {
    ds.write.mode(SaveMode.Overwrite).options(
      Map("table" -> table, "zkUrl" -> "slave01:2181;slave02:2181;slave03:2181")
    ).format("org.apache.phoenix.spark").save()
  }

  def loadHBase(table: String)(implicit ctx: SQLContext): DataFrame = {
    val df = ctx.read.options(Map("table" -> table, "zkUrl" ->  "slave01:2181;slave02:2181;slave03:2181")
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
    new DateTime(date.replace(" ", "T"), DateTimeZone.UTC).plusMinutes(m).toString.replace("T", " ")
  }

  def init(interval: Array[String],ctx:HiveContext): InitArgs = {
    if (interval.length < 1 || !interval(0).toString.contains("T")) {
      System.exit(1)
    }
    var lt = ""
    val replaceTGte = interval(0).toString.replace("T", " ")
    var filter = "updatetime >='" + replaceTGte + "'"
    if (interval.length > 1) {
      lt ="""","lt": """" + interval(1)
      val replaceTLt = interval(1).toString.replace("T", " ")
      filter = "updatetime >='" + replaceTGte + "' and updatetime< '" + replaceTLt + "'"
    }
    val query =
      """{
          "query": {
            "bool": {
              "must": [{
                "range":{
                  "updatetime": {
                    "gte": """" + interval(0) + lt +
        """"
                  }
                }
              }]
            }
          }
        }"""
    val delete_filter=" updatetime <'" + replaceTGte + "'"
    ctx.udf.register("plusMinuteToString", (a: Int, b: String) => plusMinuteToString(a, b))
    InitArgs(delete_filter,filter, query)
  }

  def main(interval: Array[String]): Unit = {
    /*  val conf = new SparkConf()
      val sc = new SparkContext(conf)
      import org.apache.spark.sql.hive.HiveContext
      val ctx = new HiveContext(sc)
      //APPSTORE_BASE_HOTSEARCH
      val options = Map("es.nodes" -> "10.28.49.4,10.28.49.54,10.28.49.29", "es.port" -> "9200")

      ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/hotsearch_words").registerTempTable("hotsearch_words")
      val APPSTORE_BASE_HOTSEARCH = ctx.sql("select hotword,cast(ranking as int ) as ranking,country,device,cast(resultcount as int ) as resultcount , substr(cast(updatetime as string ),0,19) as  updatetime, substr(cast(updatetime as string ),0,10) as  day from hotsearch_words ")
      saveHBase(APPSTORE_BASE_HOTSEARCH, "APPSTORE_BASE_HOTSEARCH")

      //APPSTORE_STATISTIC_HOTSEARCH
      init(ctx)
      loadHBaseWithNoRegTmpTable("APPSTORE_BASE_HOTSEARCH")(ctx).registerTempTable("APPSTORE_BASE_HOTSEARCH")
      val APPSTORE_STATISTIC_HOTSEARCH = ctx.sql("select hotword, ranking,country,device,min(updatetime) as starttime ,substr(plusMinuteToString(count(updatetime)*5,min(updatetime)),0,19) as endtime,count(updatetime)*5  as duration,substr(min(updatetime),0,10) as day" +
        " from APPSTORE_BASE_HOTSEARCH group by hotword, ranking,country,device")
      saveHBase(APPSTORE_STATISTIC_HOTSEARCH, "APPSTORE_STATISTIC_HOTSEARCH")

      //APPSTORE_CURRENT_TOP10_APP_RESULTS
      //val options = Map( "es.nodes" -> "10.28.49.4,10.28.49.54,10.28.49.29", "es.port" -> "9200" )
      ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/hotsearch_top100").registerTempTable("hotsearch_top100")
      val APPSTORE_CURRENT_TOP10_APP_RESULTS = ctx.sql("select hotword,cast(ranking as int ) as ranking,country,device,substr(cast(updatetime as string ),0,19) as  updatetime ,appid,appname,art_url as APPIMG ,copyright as COMPANYNAME  from hotsearch_top100  where updatetime >='2017-01-05 15' and cast(ranking as int ) <=10  ")
      //println(APPSTORE_CURRENT_TOP10_APP_RESULTS.count())
      saveHBase(APPSTORE_CURRENT_TOP10_APP_RESULTS, "APPSTORE_CURRENT_TOP10_APP_RESULTS")

      //APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH   es.hotsearch_related  &  AppStore_Base_HotSearch
      ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/hotsearch_related").registerTempTable("hotsearch_related")
      loadHBaseWithNoRegTmpTable("APPSTORE_BASE_HOTSEARCH")(ctx).registerTempTable("APPSTORE_BASE_HOTSEARCH")

      val APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH = ctx.sql("select hr.hotword as hotword ,hr.relatedword as relatedword,cast(hr.ranking as int ) as ranking,hr.country as country,hr.device as device ,substr(cast(hr.updatetime as string ),0,19) as  updatetime ," +
        " cast(hr.searchindex as int) as searchindex from  hotsearch_related hr where hr.updatetime >='2017-01-08 16'")
      saveHBase(APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH, "APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")

      loadHBaseWithNoRegTmpTable("APPSTORE_BASE_HOTSEARCH")(ctx).registerTempTable("APPSTORE_BASE_HOTSEARCH")
      loadHBaseWithNoRegTmpTable("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")(ctx).registerTempTable("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")
      val APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH_2 = ctx.sql("select hr.hotword as hotword ,hr.relatedword as relatedword,hr.ranking  as ranking,hr.country as country,hr.device as device ,hr.updatetime  as  updatetime ," +
        " hr.searchindex as searchindex ,abh.resultcount  as  hotword_resultcount from  APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH hr inner join APPSTORE_BASE_HOTSEARCH abh on substr(cast(hr.updatetime as string ),0,14) = substr(abh.updatetime,0,14) and hr.hotword=abh.hotword")
      saveHBase(APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH_2, "APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")

      //APPSTORE_APP_TOP100
      //val filter = df.filter(df("arrival").equalTo("OTP").and(df("days").gt(3))
      val query =
      "{ \"query\": {" +
        "\"bool\": { \"must\":" +
        "[" +
        "{ \"range\":" +
        "{ \"updatetime\": { \"gte\": \"2017-01-08\",\"lt\": \"2017-01-09\"}}" +
        "}]}}}"
      /*val query =
        "{ \"query\": {\"bool\": { \"must\":[{ \"range\":{ \"updatetime\": { \"gte\": \"2017-01-09T12:20\",\"lt\": \"2017-01-09T12:30\"}}}]}}}"*/

      val app_top100_options = Map("es.nodes" -> "10.28.49.4,10.28.49.54,10.28.49.29", "es.port" -> "9200", "es.query" -> query)
      ctx.read.format("org.elasticsearch.spark.sql").options(app_top100_options).load("app_store/rank_top100").registerTempTable("rank_top100")
      val APPSTORE_APP_TOP100 = ctx.sql("select appid,appname,appimg, cast(ranking as int ) as ranking,country,device,listcategory,cast(appcategory as string) as appcategory,companyname,substr(cast(updatetime as string ),0,19) as  updatetime,substr(cast(updatetime as string ),0,10) as  day  from rank_top100")
      //APPSTORE_APP_TOP100.show(100)
      saveHBase(APPSTORE_APP_TOP100, "APPSTORE_APP_TOP100")

      //APPSTORE_CURRENT_SEARCHINDEX

      loadHBaseWithNoRegTmpTable("APPSTORE_BASE_HOTSEARCH")(ctx).registerTempTable("APPSTORE_BASE_HOTSEARCH")
      loadHBaseWithNoRegTmpTable("APPSTORE_CURRENT_TOP10_APP_RESULTS")(ctx).registerTempTable("APPSTORE_CURRENT_TOP10_APP_RESULTS")
      loadHBaseWithNoRegTmpTable("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")(ctx).registerTempTable("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")
      loadHBaseWithNoRegTmpTable("AppStore_App_Top100")(ctx).registerTempTable("AppStore_App_Top100")

      val APPSTORE_CURRENT_SEARCHINDEX = ctx.sql("select h.HOTWORD as HOTWORD,max(h.APP_RANKING) AS APP_RANKING,max(h.COUNTRY) as COUNTRY ,max(h.DEVICE) as DEVICE,max(h.SEARCHINDEX) as SEARCHINDEX,max(h.HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT," +
        " max(h.RELATEDWORDS )  as RELATEDWORDS , max(h.HOTWORD_RANKING) as HOTWORD_RANKING,h.APPID as APPID ,max(h.APPNAME) as APPNAME ,max(h.APPIMG) as  APPIMG ,max(h.COMPANYNAME) as COMPANYNAME ,max(h.UPDATETIME)  as UPDATETIME ," +
        " collect_list(concat_ws('#$',i.listcategory,i.appcategory,i.ranking)) as APPRANKINGCATEGORY from  " +
        "(select c.HOTWORD as HOTWORD,a.RANKING AS APP_RANKING,c.COUNTRY as COUNTRY ,c.DEVICE as DEVICE,c.SEARCHINDEX as SEARCHINDEX,c.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT," +
        "c.RELATEDWORDS  as RELATEDWORDS , c.HOTWORD_RANKING as HOTWORD_RANKING,a.APPID as APPID ,a.APPNAME as APPNAME ,a.APPIMG as  APPIMG ,a.COMPANYNAME as COMPANYNAME , c.UPDATETIME as UPDATETIME" +
        " from APPSTORE_CURRENT_TOP10_APP_RESULTS as a " +
        " right join " +
        " (" +
        "   select HOTWORD,max(HOTWORD_RANKING) as HOTWORD_RANKING,max(SEARCHINDEX) as SEARCHINDEX,max(HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT," +
        "   COUNTRY,DEVICE,collect_list(concat_ws('#$',RELATEDWORD,RANKING)) as  RELATEDWORDS ,max(UPDATETIME) as UPDATETIME" +
        "    from " +
        "          (select  e.HOTWORD as HOTWORD ,e.UPDATETIME as UPDATETIME,e.SEARCHINDEX as SEARCHINDEX ," +
        "            e.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT ,e.COUNTRY as COUNTRY ,e.DEVICE as DEVICE," +
        "            e.RELATEDWORD as RELATEDWORD,e.RANKING as RANKING,f.ranking as HOTWORD_RANKING" +
        "           FROM APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH e" +
        " inner join APPSTORE_BASE_HOTSEARCH f on substr(e.UPDATETIME,0,13) = substr(f.UPDATETIME,0,13) " +
        "           and e.HOTWORD=f.HOTWORD and e.COUNTRY=f.COUNTRY and e.DEVICE=f.DEVICE" +
        " ) " +
        "as b " +
        "    group by b.HOTWORD,b.COUNTRY,b.DEVICE,substr(b.UPDATETIME,0,13)" +
        " ) as  c " +
        "on  a.HOTWORD=c.HOTWORD and a.COUNTRY=c.COUNTRY and a.DEVICE=c.DEVICE" +
        ") as h " +
        "left join AppStore_App_Top100 as i on  substr(h.UPDATETIME,0,13) = substr(i.UPDATETIME,0,13)  and h.appid=i.appid and h.country=i.country and h.device=i.device " +
        " group by  h.HOTWORD,h.APPID,substr(h.UPDATETIME,0,13)")
      saveHBase(APPSTORE_CURRENT_SEARCHINDEX, "APPSTORE_CURRENT_SEARCHINDEX")*/
  }
}
