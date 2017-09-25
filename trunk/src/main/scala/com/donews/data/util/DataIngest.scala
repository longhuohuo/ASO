package com.donews.data.util

import java.time.LocalDate

import org.apache.commons.cli.{GnuParser, Options}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2017/2/24.
  */
object DataIngest {
  val LOG = LoggerFactory.getLogger(DataIngest.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("es.scroll.size", "500")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val prefixName = conf.get("spark.app.name")
    conf.setAppName(s"$prefixName  - " + "DataIngest")
    val sc = new SparkContext(conf)
    val ctx = new HiveContext(sc)
    ingestHistoryKeyWordIndex(ctx)


  }

  def ingestAppRanking(ctx: HiveContext) = {
    val startDay = LocalDate.parse("2017-02-09")
    val endDay = LocalDate.parse("2017-02-24")
    for (day <- GetEsData.dateRange(startDay, endDay)) {
      GetEsData.tryDo(2) {
        val start = day + "T23:30"
        val end = day + "T23:59"
        val duration_query_new =
          s"""
        {
             |  "sort": [
             |    {
             |      "updatetime": {
             |        "order": "asc"
             |      }
             |    }
             |  ],
             |
         |        "query": {
             |          "bool": {
             |            "must": [{
             |              "range":{
             |                "updatetime": {
             |                  "gte": "$start",
             |                  "lte": "$end"
             |                  }
             |                 }
             |              }]
             |            }
             |           }
             |
         |
         | }
          """.stripMargin
        val dataFrameReader = GetEsData.getDataFrameReader(ctx, duration_query_new)

        //APPSTORE_APP_TOP100
        val startTime6 = System.currentTimeMillis()
        dataFrameReader.load("app_store/rank_top100").registerTempTable("rank_top100")
        val APPSTORE_APP_TOP100 = ctx.sql("select appid,appname,appimg, cast(ranking as int ) as ranking,country,device,listcategory,cast(appcategory as string) as appcategory,companyname,substr(cast(updatetime as string ),0,19) as  updatetime,substr(cast(updatetime as string ),0,10) as  day  from rank_top100")
        GetEsData.saveHBase(APPSTORE_APP_TOP100, "APPSTORE_APP_TOP100")
        val endTime6 = System.currentTimeMillis()
        val inteval6 = (endTime6 - startTime6) / 1000
        LOG.info(s" The total time of dealing with APPSTORE_APP_TOP100  $day is : " + inteval6 + "s")
      }
    }

  }

  def ingestHistoryKeyWordIndex(ctx: HiveContext) = {
    val startDay = LocalDate.parse("2017-02-27")
    val endDay = LocalDate.parse("2017-02-28")
    for (day <- GetEsData.dateRange(startDay, endDay)) {
      val day_filter = s"day='$day'"
     //APPSTORE_CURRENT_SEARCHINDEX
     val startTime7 = System.currentTimeMillis()
     //search_index_list
     GetEsData.getDataFrameReader(ctx, "").load("app_store/search_index_list_v3").registerTempTable("SEARCH_INDEX_LIST")
     GetEsData.loadHBase("APPSTORE_TOP10_APP_RESULTS")(ctx).filter(day_filter).registerTempTable("APPSTORE_TOP10_APP_RESULTS")
     GetEsData.loadHBase("APPSTORE_TOP10_RELATED_HOTSEARCH")(ctx).filter(day_filter).registerTempTable("APPSTORE_TOP10_RELATED_HOTSEARCH")
     GetEsData.loadHBase("AppStore_App_Top100")(ctx).filter(day_filter).registerTempTable("AppStore_App_Top100")
        val APPSTORE_HISTORY_SEARCHINDEX = ctx.sql(
        """
          |select h.HOTWORD as HOTWORD,cast(max(h.APP_RANKING) as string )  AS APP_RANKING,h.COUNTRY as COUNTRY ,h.DEVICE as DEVICE,max(h.SEARCHINDEX) as SEARCHINDEX,max(h.HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT,
          |max(h.RELATEDWORDS )  as RELATEDWORDS ,h.APPID as APPID ,max(h.GENRENAMES) as GENRENAMES,max(h.APPNAME) as APPNAME ,max(h.APPIMG) as  APPIMG ,max(h.COMPANYNAME) as COMPANYNAME ,max(h.UPDATETIME)  as UPDATETIME ,
          |if(cast (sort_array(collect_list(concat_ws('#$',i.ranking,i.listcategory,i.appcategory))) as string) =='[]',null,sort_array(collect_list(concat_ws('#$',i.ranking,i.listcategory,i.appcategory))))  as APPRANKINGCATEGORY
          |from
          |(select * from
          |(select c.HOTWORD  as HOTWORD,a.RANKING AS APP_RANKING,c.COUNTRY as COUNTRY ,c.DEVICE as DEVICE,c.SEARCHINDEX as SEARCHINDEX,c.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT,
          |c.RELATEDWORDS  as RELATEDWORDS ,a.APPID as APPID ,a.GENRENAMES as GENRENAMES, a.APPNAME as APPNAME ,a.APPIMG as  APPIMG ,a.COMPANYNAME as COMPANYNAME , c.UPDATETIME as UPDATETIME
          |from
          | (
          |select HOTWORD,max(SEARCHINDEX) as SEARCHINDEX,max(HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT,
          |COUNTRY,DEVICE,sort_array(collect_list(concat_ws('#$',RANKING,RELATEDWORD))) as  RELATEDWORDS ,max(UPDATETIME) as UPDATETIME
          |from
          |(select  e.HOTWORD as HOTWORD ,e.UPDATETIME as UPDATETIME,f.SEARCHINDEX_ as SEARCHINDEX ,
          |f.RESULTCOUNT_ as HOTWORD_RESULTCOUNT ,e.COUNTRY as COUNTRY ,e.DEVICE as DEVICE,
          |e.RELATEDWORD as RELATEDWORD,e.RANKING as RANKING
          |FROM APPSTORE_TOP10_RELATED_HOTSEARCH e
          |inner join SEARCH_INDEX_LIST f on  e.HOTWORD=f.keyword and e.COUNTRY=f.COUNTRY and e.DEVICE=f.DEVICE
          |) as b
          |group by b.HOTWORD,b.COUNTRY,b.DEVICE
          |) as  c left join
          | APPSTORE_TOP10_APP_RESULTS as a
          |on  a.HOTWORD=c.HOTWORD and a.COUNTRY=c.COUNTRY and a.DEVICE=c.DEVICE
          |) as z  where z.appid is not null ) as h
          |left join AppStore_App_Top100 as i on   h.appid=i.appid and h.country=i.country and h.device=i.device
          |group by  h.HOTWORD,h.APPID,h.COUNTRY,h.DEVICE
        """.stripMargin)
     GetEsData.saveHBase(APPSTORE_HISTORY_SEARCHINDEX, "APPSTORE_HISTORY_SEARCHINDEX")
     val endTime7 = System.currentTimeMillis()
     val inteval7 = (endTime7 - startTime7) / 1000
     LOG.info(s" The total time of dealing with APPSTORE_HISTORY_SEARCHINDEX day='$day' is : " + inteval7 + "s")

      //APPSTORE_APP_KEYWORD_COVER
      val startTime8 = System.currentTimeMillis()
      GetEsData.loadHBase("APPSTORE_BASE_HOTSEARCH")(ctx).filter(day_filter).registerTempTable("APPSTORE_BASE_HOTSEARCH")
      GetEsData.loadHBase("APPSTORE_HISTORY_SEARCHINDEX")(ctx).filter(day_filter).filter("appid is not null").registerTempTable("APPSTORE_HISTORY_SEARCHINDEX")
      val APPSTORE_APP_KEYWORD_COVER = ctx.sql(
        """
       |select  appid,country,device,day,
       |sum(case when searchindex > 8000 then 1 else 0 end) as all_range_gte_8000 ,
       |sum(case when searchindex >= 7000 and searchindex <= 7999 then 1 else 0 end) as all_range_7000_7999 ,
       |sum(case when searchindex >= 6000 and searchindex <= 6999 then 1 else 0 end) as all_range_6000_6999 ,
       |sum(case when searchindex >= 5000 and searchindex <= 5999 then 1 else 0 end) as all_range_5000_5999 ,
       |sum(case when searchindex >= 4605 and searchindex <= 4999 then 1 else 0 end) as all_range_4605_4999 ,
       |sum(case when searchindex < 4605 then 1 else 0 end) as all_range_lt_4605 ,
       |sum(case when searchindex > 8000 and ranking <=3 then 1 else 0 end) as top3_range_gte_8000 ,
       |sum(case when searchindex >= 7000 and searchindex <= 7999 and ranking <=3  then 1 else 0 end) as top3_range_7000_7999 ,
       |sum(case when searchindex >= 6000 and searchindex <= 6999 and ranking <=3 then 1 else 0 end) as top3_range_6000_6999 ,
       |sum(case when searchindex >= 5000 and searchindex <= 5999 and ranking <=3 then 1 else 0 end) as top3_range_5000_5999 ,
       |sum(case when searchindex >= 4605 and searchindex <= 4999 and ranking <=3 then 1 else 0 end) as top3_range_4605_4999 ,
       |sum(case when searchindex < 4605 and ranking <=3 then 1 else 0 end) as top3_range_lt_4605 ,
       |sum(case when searchindex > 8000 and ranking is not null then 1 else 0 end) as top10_range_gte_8000 ,
       |sum(case when searchindex >= 7000 and searchindex <= 7999 and ranking is not null  then 1 else 0 end) as top10_range_7000_7999 ,
       |sum(case when searchindex >= 6000 and searchindex <= 6999 and ranking is not null then 1 else 0 end) as top10_range_6000_6999 ,
       |sum(case when searchindex >= 5000 and searchindex <= 5999 and ranking is not null then 1 else 0 end) as top10_range_5000_5999 ,
       |sum(case when searchindex >= 4605 and searchindex <= 4999 and ranking is not null then 1 else 0 end) as top10_range_4605_4999 ,
       |sum(case when searchindex < 4605 and ranking is not null then 1 else 0 end) as top10_range_lt_4605,
       |sum(case when ranking is not null then 1 else 0 end) as top10_total
       |from (
       |select a.appid as appid , a.country as country,a.device as device,b.ranking as ranking,a.searchindex as searchindex,substr(a.updatetime ,0,10) as day from
       |APPSTORE_HISTORY_SEARCHINDEX as a left join APPSTORE_BASE_HOTSEARCH as b on a.hotword=b.hotword and a.country=b.country and a.device=b.device
       |) as c group by appid,country,device,day
     """.stripMargin)
      GetEsData.saveHBase(
        APPSTORE_APP_KEYWORD_COVER,
        "APPSTORE_APP_KEYWORD_COVER")
   val endTime8 = System.currentTimeMillis()
   val
   inteval8 = (endTime8 - startTime8) / 1000
      LOG.info(
        " The total time of dealing with APPSTORE_APP_KEYWORD_COVER is : " + inteval8 + "s")

      //APPSTORE_APP_KEYWORD_RANKING   APPSTORE_APP_KEYWORD_RANKING
   val startTime9 = System.currentTimeMillis()
      val APPSTORE_APP_KEYWORD_RANKING = ctx.sql(
        """
       |select a.hotword as hotword ,a.appid as appid ,
       |a.country as country,a.device as device,b.ranking as hotword_ranking,a.searchindex as searchindex ,
       |a.hotword_resultcount as hotword_resultcount,substr(cast(a.updatetime as string ),0,10) as day,substr(cast(a.updatetime as string ),0,16) as updatetime from
       |APPSTORE_HISTORY_SEARCHINDEX as a left join APPSTORE_BASE_HOTSEARCH as b on a.hotword=b.hotword and a.country=b.country and a.device=b.device
     """.stripMargin)
   GetEsData.saveHBase(
     APPSTORE_APP_KEYWORD_RANKING,
     "APPSTORE_APP_KEYWORD_RANKING")
   val endTime9 = System.currentTimeMillis()
   val inteval9 = (endTime9 - startTime9) / 1000
   LOG.info(
     " The total time of dealing with APPSTORE_APP_KEYWORD_RANKING is : " + inteval9 + "s")

 }



}
}