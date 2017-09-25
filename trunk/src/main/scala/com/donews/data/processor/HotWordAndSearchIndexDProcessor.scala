package com.donews.data.processor

import com.donews.data.util.GetEsData
import org.apache.commons.cli.{GnuParser, Options}
import org.slf4j.LoggerFactory

/**
  * Created by ChengXu&ChenLei on 2017/1/10.
  * 每天跑一次,最后重新更新数据,重试次数为1
  */
object HotWordAndSearchIndexDProcessor {
  val LOG = LoggerFactory.getLogger(HotWordAndSearchIndexDProcessor.getClass)

  def main(args: Array[String]): Unit = {
    val optionsArgs = new Options()
      .addOption("i", "interval", true, s"时间区间 ")
    val parser = new GnuParser()
    val cmdLine = parser.parse(optionsArgs, args)
    val interval = cmdLine.getOptionValue("i").split("/")
    val ctx = GetEsData.getHiveContext("HotWordAndSearchIndexDProcessor--" + interval)

    GetEsData.tryDo(3) {
      try {

        val (day_filter, filter) = GetEsData.init(interval, ctx)
        val day = s"day='$day_filter'"
        LOG.info("the filter condition is : " + filter)

        //APPSTORE.TOP10_APP_RESULTS
        //        val startTime1 = System.currentTimeMillis()
        GetEsData.loadHBase("APPSTORE.HOTSEARCH_TOP100")(ctx).filter(day).drop("updatetime").distinct().registerTempTable("HOTSEARCH_TOP100")
        GetEsData.loadHBase("APPSTORE.APP_DETAIL")(ctx).filter("COUNTRY='cn' or COUNTRY='tw' or COUNTRY='hk'").registerTempTable("APP_DETAIL")
        //        val TOP10_APP_RESULTS = ctx.sql(
        //          """
        //            |select ab.hotword as hotword,ab.ranking as ranking,ab.country as country,ab.device as device,ab.updatetime as updatetime,
        //            |ab.appid as appid, an.appname as appname,an.APPIMG as APPIMG ,an.COMPANYNAME as COMPANYNAME,an.GENRENAMES as GENRENAMES,ab.day as DAY
        //            |from
        //            |(select hotword,ranking,country,device,appid,day from HOTSEARCH_TOP100 where ranking <= 10) ab
        //            |inner join APP_DETAIL an on
        //            |ab.COUNTRY=an.COUNTRY and ab.APPID=an.APPID
        //          """.stripMargin)
        //        GetEsData.saveHBase(TOP10_APP_RESULTS, "APPSTORE.TOP10_APP_RESULTS")
        //        val endTime1 = System.currentTimeMillis()
        //        val inteval1 = (endTime1 - startTime1) / 1000
        //        LOG.info(" The total time of dealing with APPSTORE.TOP10_APP_RESULTS is : " + inteval1 + "s")
        //APPSTORE_TOP10_RELATED_HOTSEARCH   es.hotsearch_related  &  AppStore_Base_HotSearch

        val startTime2 = System.currentTimeMillis()
        GetEsData.loadHBase("APPSTORE.HOTSEARCH_RELATED")(ctx).filter(day).registerTempTable("HOTSEARCH_RELATED")
        val HOTSEARCH_RELATED = ctx.sql(
          """
            |select hotword as hotword, relatedword as relatedword, ranking, country as country, device as device, updatetime,
            |searchindex, substr(updatetime,0,10) as day from HOTSEARCH_RELATED where hotword is not null and ranking <= 10
          """.stripMargin)
        GetEsData.saveHBase(HOTSEARCH_RELATED, "APPSTORE.HOTSEARCH_RELATED")
        val endTime2 = System.currentTimeMillis()
        val inteval2 = (endTime2 - startTime2) / 1000
        LOG.info(" The total time of dealing with APPSTORE.HOTSEARCH_RELATED is : " + inteval2 + "s")

        //APPSTORE.CURRENT_SEARCHINDEX
        val startTime3 = System.currentTimeMillis()
        //search_index_list
        GetEsData.loadHBase("APPSTORE.SEARCH_INDEX_LIST")(ctx).filter(day).registerTempTable("SEARCH_INDEX_LIST")
        GetEsData.loadHBase("APPSTORE.HOTSEARCH_RELATED")(ctx).filter(day).registerTempTable("HOTSEARCH_RELATED")
        GetEsData.loadHBase("APPSTORE.RANK_TOP100")(ctx).filter(day).registerTempTable("RANK_TOP100")

        ctx.sql(
          """
            |select * from
            |(select z.HOTWORD as HOTWORD, z.APP_RANKING AS APP_RANKING, z.COUNTRY as COUNTRY, z.DEVICE as DEVICE, z.SEARCHINDEX as SEARCHINDEX, z.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT,
            |z.RELATEDWORDS as RELATEDWORDS, z.APPID as APPID, l.GENRENAMES as GENRENAMES, l.APPNAME as APPNAME, l.APPIMG as APPIMG, l.COMPANYNAME as COMPANYNAME, z.UPDATETIME as UPDATETIME
            |from
            |(select c.HOTWORD as HOTWORD, a.RANKING AS APP_RANKING, c.COUNTRY as COUNTRY, c.DEVICE as DEVICE, c.SEARCHINDEX as SEARCHINDEX, c.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT,
            |c.RELATEDWORDS as RELATEDWORDS, a.APPID as APPID, c.UPDATETIME as UPDATETIME
            |from
            |(select HOTWORD, max(SEARCHINDEX) as SEARCHINDEX,max(HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT,COUNTRY, DEVICE,
            |cast(sort_array(collect_list(concat_ws('#$',RANKING,RELATEDWORD))) as string) as RELATEDWORDS, max(UPDATETIME) as UPDATETIME
            |from
            |(select e.HOTWORD as HOTWORD, e.UPDATETIME as UPDATETIME, f.SEARCHINDEX as SEARCHINDEX, f.RESULTCOUNT as HOTWORD_RESULTCOUNT, e.COUNTRY as COUNTRY, e.DEVICE as DEVICE,
            |e.RELATEDWORD as RELATEDWORD,e.RANKING as RANKING FROM HOTSEARCH_RELATED e
            |inner join SEARCH_INDEX_LIST f on e.HOTWORD=f.keyword and e.COUNTRY=f.COUNTRY and e.DEVICE=f.DEVICE) as b
            |group by b.HOTWORD,b.COUNTRY,b.DEVICE) as c
            |left join
            |HOTSEARCH_TOP100 as a
            |on a.HOTWORD=c.HOTWORD and a.COUNTRY=c.COUNTRY and a.DEVICE=c.DEVICE) as z
            |inner join
            |APP_DETAIL as l
            |on z.COUNTRY=l.COUNTRY and z.APPID=l.APPID) as m
            |where m.appid is not null
          """.stripMargin).distinct().registerTempTable("DISTINCT_APPID_RESULTS")

        val CURRENT_SEARCHINDEX = ctx.sql(
          """
            |select h.HOTWORD as HOTWORD, cast(max(h.APP_RANKING) as string) AS APP_RANKING,h.COUNTRY as COUNTRY ,h.DEVICE as DEVICE,max(h.SEARCHINDEX) as SEARCHINDEX,
            |max(h.HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT, max(h.RELATEDWORDS ) as RELATEDWORDS, h.APPID as APPID, max(h.GENRENAMES) as GENRENAMES, max(h.APPNAME) as APPNAME,
            |max(h.APPIMG) as APPIMG, max(h.COMPANYNAME) as COMPANYNAME ,max(h.UPDATETIME) as UPDATETIME,
            |if(cast(sort_array(collect_list(concat_ws('#$',i.ranking,i.listcategory,i.appcategory))) as string) =='[]',null,cast(sort_array(collect_list(concat_ws('#$',i.ranking,i.listcategory,i.appcategory))) as string)) as APPRANKINGCATEGORY
            |from DISTINCT_APPID_RESULTS as h left join
            |RANK_TOP100 as i on h.appid=i.appid and h.country=i.country and h.device=i.device
            |group by h.HOTWORD, h.APPID, h.COUNTRY, h.DEVICE
          """.stripMargin)



        /*val CURRENT_SEARCHINDEX = ctx.sql(
          """
            |select h.HOTWORD as HOTWORD, cast(max(h.APP_RANKING) as string) AS APP_RANKING,h.COUNTRY as COUNTRY ,h.DEVICE as DEVICE,max(h.SEARCHINDEX) as SEARCHINDEX,
            |max(h.HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT, max(h.RELATEDWORDS ) as RELATEDWORDS, h.APPID as APPID, max(h.GENRENAMES) as GENRENAMES, max(h.APPNAME) as APPNAME,
            |max(h.APPIMG) as APPIMG, max(h.COMPANYNAME) as COMPANYNAME ,max(h.UPDATETIME) as UPDATETIME,
            |if(cast(sort_array(collect_list(concat_ws('#$',i.ranking,i.listcategory,i.appcategory))) as string) =='[]',null,cast(sort_array(collect_list(concat_ws('#$',i.ranking,i.listcategory,i.appcategory))) as string)) as APPRANKINGCATEGORY
            |from
            |(select * from
            |(select z.HOTWORD as HOTWORD, z.APP_RANKING AS APP_RANKING, z.COUNTRY as COUNTRY, z.DEVICE as DEVICE, z.SEARCHINDEX as SEARCHINDEX, z.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT,
            |z.RELATEDWORDS as RELATEDWORDS, z.APPID as APPID, l.GENRENAMES as GENRENAMES, l.APPNAME as APPNAME, l.APPIMG as APPIMG, l.COMPANYNAME as COMPANYNAME, z.UPDATETIME as UPDATETIME
            |from
            |(select c.HOTWORD as HOTWORD, a.RANKING AS APP_RANKING, c.COUNTRY as COUNTRY, c.DEVICE as DEVICE, c.SEARCHINDEX as SEARCHINDEX, c.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT,
            |c.RELATEDWORDS as RELATEDWORDS, a.APPID as APPID, c.UPDATETIME as UPDATETIME
            |from
            |(select HOTWORD, max(SEARCHINDEX) as SEARCHINDEX,max(HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT,COUNTRY, DEVICE,
            |cast(sort_array(collect_list(concat_ws('#$',RANKING,RELATEDWORD))) as string) as RELATEDWORDS, max(UPDATETIME) as UPDATETIME
            |from
            |(select e.HOTWORD as HOTWORD, e.UPDATETIME as UPDATETIME, f.SEARCHINDEX as SEARCHINDEX, f.RESULTCOUNT as HOTWORD_RESULTCOUNT, e.COUNTRY as COUNTRY, e.DEVICE as DEVICE,
            |e.RELATEDWORD as RELATEDWORD,e.RANKING as RANKING FROM HOTSEARCH_RELATED e
            |inner join SEARCH_INDEX_LIST f on e.HOTWORD=f.keyword and e.COUNTRY=f.COUNTRY and e.DEVICE=f.DEVICE) as b
            |group by b.HOTWORD,b.COUNTRY,b.DEVICE) as c
            |left join
            |HOTSEARCH_TOP100 as a
            |on a.HOTWORD=c.HOTWORD and a.COUNTRY=c.COUNTRY and a.DEVICE=c.DEVICE) as z
            |inner join
            |APP_DETAIL as l
            |on z.COUNTRY=l.COUNTRY and z.APPID=l.APPID) as m
            |where m.appid is not null ) as h
            |left join RANK_TOP100 as i on h.appid=i.appid and h.country=i.country and h.device=i.device
            |group by h.HOTWORD, h.APPID, h.COUNTRY, h.DEVICE
          """.stripMargin)*/


        GetEsData.saveHBase(CURRENT_SEARCHINDEX, "APPSTORE.CURRENT_SEARCHINDEX")
        val endTime3 = System.currentTimeMillis()
        val inteval3 = (endTime3 - startTime3) / 1000
        LOG.info(" The total time of dealing with APPSTORE.CURRENT_SEARCHINDEX is : " + inteval3 + "s")

        //APPSTORE.APP_KEYWORD_COVER
        val startTime4 = System.currentTimeMillis()
        GetEsData.loadHBase("APPSTORE.CURRENT_SEARCHINDEX")(ctx).filter(filter).filter("appid is not null").registerTempTable("CURRENT_SEARCHINDEX")
        val APP_KEYWORD_COVER = ctx.sql(
          """
            |select APPID, COUNTRY, DEVICE, DAY,
            |sum(case when SEARCHINDEX > 8000 then 1 else 0 end) as ALL_RANGE_GTE_8000,
            |sum(case when SEARCHINDEX >= 7000 and SEARCHINDEX <= 7999 then 1 else 0 end) as ALL_RANGE_7000_7999,
            |sum(case when SEARCHINDEX >= 6000 and SEARCHINDEX <= 6999 then 1 else 0 end) as ALL_RANGE_6000_6999,
            |sum(case when SEARCHINDEX >= 5000 and SEARCHINDEX <= 5999 then 1 else 0 end) as ALL_RANGE_5000_5999,
            |sum(case when SEARCHINDEX >= 4605 and SEARCHINDEX <= 4999 then 1 else 0 end) as ALL_RANGE_4605_4999,
            |sum(case when SEARCHINDEX < 4605 then 1 else 0 end) as ALL_RANGE_LT_4605,
            |sum(case when SEARCHINDEX > 8000 and RANKING <=3 then 1 else 0 end) as TOP3_RANGE_GTE_8000,
            |sum(case when SEARCHINDEX >= 7000 and SEARCHINDEX <= 7999 and RANKING <=3  then 1 else 0 end) as TOP3_RANGE_7000_7999,
            |sum(case when SEARCHINDEX >= 6000 and SEARCHINDEX <= 6999 and RANKING <=3 then 1 else 0 end) as TOP3_RANGE_6000_6999,
            |sum(case when SEARCHINDEX >= 5000 and SEARCHINDEX <= 5999 and RANKING <=3 then 1 else 0 end) as TOP3_RANGE_5000_5999,
            |sum(case when SEARCHINDEX >= 4605 and SEARCHINDEX <= 4999 and RANKING <=3 then 1 else 0 end) as TOP3_RANGE_4605_4999,
            |sum(case when SEARCHINDEX < 4605 and RANKING <=3 then 1 else 0 end) as TOP3_RANGE_LT_4605,
            |sum(case when SEARCHINDEX > 8000 and RANKING <=10 then 1 else 0 end) as TOP10_RANGE_GTE_8000,
            |sum(case when SEARCHINDEX >= 7000 and SEARCHINDEX <= 7999 and RANKING <=10  then 1 else 0 end) as TOP10_RANGE_7000_7999,
            |sum(case when SEARCHINDEX >= 6000 and SEARCHINDEX <= 6999 and RANKING <=10 then 1 else 0 end) as TOP10_RANGE_6000_6999,
            |sum(case when SEARCHINDEX >= 5000 and SEARCHINDEX <= 5999 and RANKING <=10 then 1 else 0 end) as TOP10_RANGE_5000_5999,
            |sum(case when SEARCHINDEX >= 4605 and SEARCHINDEX <= 4999 and RANKING <=10 then 1 else 0 end) as TOP10_RANGE_4605_4999,
            |sum(case when SEARCHINDEX < 4605 and RANKING <=10 then 1 else 0 end) as TOP10_RANGE_LT_4605,
            |sum(case when RANKING <=10 then 1 else 0 end) as TOP10_TOTAL
            |from (
            |select a.APPID as APPID , a.COUNTRY as COUNTRY,a.DEVICE as DEVICE,b.RANKING as RANKING,a.SEARCHINDEX as SEARCHINDEX,substr(a.updatetime ,0,10) as day from
            |CURRENT_SEARCHINDEX as a left join HOTSEARCH_TOP100 as b on a.APPID=b.APPID and a.HOTWORD=b.HOTWORD and a.COUNTRY=b.COUNTRY and a.DEVICE=b.DEVICE
            |) as c group by APPID, COUNTRY, DEVICE, DAY
          """.stripMargin)
        GetEsData.saveHBase(APP_KEYWORD_COVER, "APPSTORE.APP_KEYWORD_COVER")
        val endTime4 = System.currentTimeMillis()
        val inteval4 = (endTime4 - startTime4) / 1000
        LOG.info(" The total time of dealing with APPSTORE.APP_KEYWORD_COVER is : " + inteval4 + "s")

        //APPSTORE.APP_KEYWORD_RANKING
        val startTime5 = System.currentTimeMillis()
        val APP_KEYWORD_RANKING = ctx.sql(
          """
            |select a.HOTWORD as HOTWORD, a.APPID as APPID, a.COUNTRY as COUNTRY, a.DEVICE as DEVICE, b.RANKING as APP_RANKING, a.SEARCHINDEX as SEARCHINDEX,
            |a.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT,substr(cast(a.UPDATETIME as string),0,10) as day, substr(cast(a.UPDATETIME as string),0,16) as UPDATETIME from
            |CURRENT_SEARCHINDEX as a left join HOTSEARCH_TOP100 as b on a.APPID=b.APPID and  a.HOTWORD=b.HOTWORD and a.COUNTRY=b.COUNTRY and a.DEVICE=b.DEVICE
          """.stripMargin)
        GetEsData.saveHBase(APP_KEYWORD_RANKING, "APPSTORE.APP_KEYWORD_RANKING")
        val endTime5 = System.currentTimeMillis()
        val inteval5 = (endTime5 - startTime5) / 1000
        LOG.info(" The total time of dealing with APPSTORE.APP_KEYWORD_RANKING is : " + inteval5 + "s")

        //APPSTORE.APP_KEYWORD_ON_BOARD_COVER
        val startTime6 = System.currentTimeMillis()
        GetEsData.loadHBase("APPSTORE.HOTSEARCH_WORDS")(ctx).filter(day).registerTempTable("HOTSEARCH_WORDS")
        val APP_KEYWORD_ON_BOARD_COVER = ctx.sql(
          """
            |select appid, country, device, day, sum(case when ranking is not null then 1 else 0 end) as top10_total
            |from (
            |select a.appid as appid , a.country as country,a.device as device,b.ranking as ranking,a.searchindex as searchindex,substr(a.updatetime ,0,10) as day
            |from CURRENT_SEARCHINDEX as a
            |left join HOTSEARCH_WORDS as b on a.hotword=b.hotword and a.country=b.country and a.device=b.device
            |) as c
            |group by appid, country, device, day
          """.stripMargin)
        GetEsData.saveHBase(APP_KEYWORD_ON_BOARD_COVER, "APPSTORE.APP_KEYWORD_ON_BOARD_COVER")
        val endTime6 = System.currentTimeMillis()
        val inteval6 = (endTime6 - startTime6) / 1000
        LOG.info(" The total time of dealing with APPSTORE.APP_KEYWORD_ON_BOARD_COVER is : " + inteval6 + "s")
      }
      catch {
        case e: Exception =>
          throw new Exception(e)
      }

    }
  }
}