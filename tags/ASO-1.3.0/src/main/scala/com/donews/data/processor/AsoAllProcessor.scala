package com.donews.data.processor
import com.donews.data.util.{GetEsData, InitArgs}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2017/1/12.
  */
object AsoAllProcessor {
  def main(args: Array[String]): Unit = {
    val LOG = LoggerFactory.getLogger(AsoAllProcessor.getClass)
    val conf = new SparkConf
    val prefixName = conf.get("spark.app.name")
    conf.setAppName(s"$prefixName  - AsoAllProcessor")
    LOG.info("name {}", conf.get("spark.app.name"))
    val sc = new SparkContext(conf)
    val ctx =  new HiveContext(sc)
    ctx.setConf("spark.sql.parquet.mergeSchema", "true")
    val InitArgs(delete_filter,filter,query)=GetEsData.init(args,ctx)
    LOG.info("the filter condition is : " + filter)
    LOG.info("the query condition is : " + query)
    val startTime1 = System.currentTimeMillis()

    //APPSTORE_BASE_HOTSEARCH
    val options = Map("es.nodes" -> "10.28.49.4,10.28.49.54,10.28.49.29", "es.port" -> "9200", "es.query" -> query)
    ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/hotsearch_words").registerTempTable("hotsearch_words")
    val APPSTORE_BASE_HOTSEARCH = ctx.sql("select hotword,cast(ranking as int ) as ranking,country,device,cast(resultcount as int ) as resultcount , substr(cast(updatetime as string ),0,19) as  updatetime, substr(cast(updatetime as string ),0,10) as  day from hotsearch_words ")
    GetEsData.saveHBase(APPSTORE_BASE_HOTSEARCH, "APPSTORE_BASE_HOTSEARCH")
    val endTime1 = System.currentTimeMillis()
    val inteval1 = (endTime1 - startTime1) / 1000
    LOG.info(" The total time of dealing with APPSTORE_BASE_HOTSEARCH is : " + inteval1 + "s")

    //APPSTORE_STATISTIC_HOTSEARCH
    val startTime2 = System.currentTimeMillis()
    GetEsData.loadHBase("APPSTORE_BASE_HOTSEARCH")(ctx).filter(filter).registerTempTable("APPSTORE_BASE_HOTSEARCH")
    val APPSTORE_STATISTIC_HOTSEARCH = ctx.sql("select hotword, ranking,country,device,min(updatetime) as starttime ,substr(plusMinuteToString(count(updatetime)*5,min(updatetime)),0,19) as endtime,count(updatetime)*5  as duration,substr(min(updatetime),0,10) as day" +
      " from APPSTORE_BASE_HOTSEARCH group by hotword, ranking,country,device")
    GetEsData.saveHBase(APPSTORE_STATISTIC_HOTSEARCH, "APPSTORE_STATISTIC_HOTSEARCH")
    val endTime2 = System.currentTimeMillis()
    val inteval2 = (endTime2 - startTime2) / 1000
    LOG.info(" The total time of dealing with APPSTORE_STATISTIC_HOTSEARCH is : " + inteval2 + "s")

    //APPSTORE_HOTSEARCH_AGRR
    val startTime3 = System.currentTimeMillis()
    GetEsData.loadHBase("APPSTORE_BASE_HOTSEARCH")(ctx).filter(filter).registerTempTable("APPSTORE_BASE_HOTSEARCH")
    val APPSTORE_HOTSEARCH_AGRR = ctx.sql("select COUNTRY,DEVICE,collect_list(concat_ws('#$',HOTWORD,RANKING)) as HOTWORDS,max(substr(UPDATETIME,0,16)) as UPDATETIME,DAY from APPSTORE_BASE_HOTSEARCH group by COUNTRY,DEVICE,substr(UPDATETIME,0,16),DAY")
    GetEsData.saveHBase(APPSTORE_HOTSEARCH_AGRR, "APPSTORE_HOTSEARCH_AGRR")
    val endTime3 = System.currentTimeMillis()
    val inteval3 = (endTime3 - startTime3) / 1000
    LOG.info(" The total time of dealing with APPSTORE_HOTSEARCH_AGRR is : " + inteval3 + "s")

    //APPSTORE_CURRENT_TOP10_APP_RESULTS
    val startTime4 = System.currentTimeMillis()
    ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/hotsearch_top100").registerTempTable("hotsearch_top100")
    val APPSTORE_CURRENT_TOP10_APP_RESULTS = ctx.sql("select hotword,cast(ranking as int ) as ranking,country,device,substr(cast(updatetime as string ),0,19) as  updatetime ,appid,appname,art_url as APPIMG ,copyright as COMPANYNAME,GENRENAMES    from hotsearch_top100  where  cast(ranking as int ) <=10  ")
    GetEsData.saveHBase(APPSTORE_CURRENT_TOP10_APP_RESULTS, "APPSTORE_CURRENT_TOP10_APP_RESULTS")
    GetEsData.updateHBase("delete from APPSTORE_CURRENT_TOP10_APP_RESULTS where  "+delete_filter)
    val endTime4 = System.currentTimeMillis()
    val inteval4 = (endTime4 - startTime4) / 1000
    LOG.info(" The total time of dealing with APPSTORE_CURRENT_TOP10_APP_RESULTS is : " + inteval4 + "s")

    //APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH   es.hotsearch_related  &  AppStore_Base_HotSearch
    val startTime5 = System.currentTimeMillis()
    ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/hotsearch_related").registerTempTable("hotsearch_related")
    GetEsData.loadHBase("APPSTORE_BASE_HOTSEARCH")(ctx).filter(filter).registerTempTable("APPSTORE_BASE_HOTSEARCH")
    val APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH = ctx.sql("select hr.hotword as hotword ,hr.relatedword as relatedword,cast(hr.ranking as int ) as ranking,hr.country as country,hr.device as device ,substr(cast(hr.updatetime as string ),0,19) as  updatetime ," +
      " cast(hr.searchindex as int) as searchindex from  hotsearch_related hr")
    GetEsData.saveHBase(APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH, "APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")
    GetEsData.loadHBase("APPSTORE_BASE_HOTSEARCH")(ctx).filter(filter).registerTempTable("APPSTORE_BASE_HOTSEARCH")
    GetEsData.loadHBase("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")(ctx).registerTempTable("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")
    val APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH_2 = ctx.sql("select hr.hotword as hotword ,hr.relatedword as relatedword,hr.ranking  as ranking,hr.country as country,hr.device as device ,hr.updatetime  as  updatetime ," +
      " hr.searchindex as searchindex ,abh.resultcount  as  hotword_resultcount from  APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH hr inner join APPSTORE_BASE_HOTSEARCH abh on  hr.hotword=abh.hotword")
    GetEsData.saveHBase(APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH_2, "APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")
    GetEsData.updateHBase("delete from APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH where  "+delete_filter)
    val endTime5 = System.currentTimeMillis()
    val inteval5 = (endTime5 - startTime5) / 1000
    LOG.info(" The total time of dealing with APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH is : " + inteval5 + "s")

    //APPSTORE_APP_TOP100
    val startTime6 = System.currentTimeMillis()
    ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/rank_top100").registerTempTable("rank_top100")
    val APPSTORE_APP_TOP100 = ctx.sql("select appid,appname,appimg, cast(ranking as int ) as ranking,country,device,listcategory,cast(appcategory as string) as appcategory,companyname,substr(cast(updatetime as string ),0,19) as  updatetime,substr(cast(updatetime as string ),0,10) as  day  from rank_top100")
    GetEsData.saveHBase(APPSTORE_APP_TOP100, "APPSTORE_APP_TOP100")
    val endTime6 = System.currentTimeMillis()
    val inteval6 = (endTime6 - startTime6) / 1000
    LOG.info(" The total time of dealing with APPSTORE_APP_TOP100   is : " + inteval6 + "s")

    //APPSTORE_CURRENT_SEARCHINDEX
    val startTime7 = System.currentTimeMillis()
    GetEsData.loadHBase("APPSTORE_BASE_HOTSEARCH")(ctx).filter(filter).registerTempTable("APPSTORE_BASE_HOTSEARCH")
    GetEsData.loadHBase("APPSTORE_CURRENT_TOP10_APP_RESULTS")(ctx).registerTempTable("APPSTORE_CURRENT_TOP10_APP_RESULTS")
    GetEsData.loadHBase("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")(ctx).registerTempTable("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")
    GetEsData.loadHBase("AppStore_App_Top100")(ctx).filter(filter).registerTempTable("AppStore_App_Top100")
    val APPSTORE_CURRENT_SEARCHINDEX = ctx.sql("select h.HOTWORD as HOTWORD,max(h.APP_RANKING) AS APP_RANKING,h.COUNTRY as COUNTRY ,h.DEVICE as DEVICE,max(h.SEARCHINDEX) as SEARCHINDEX,max(h.HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT," +
      " max(h.RELATEDWORDS )  as RELATEDWORDS , max(h.HOTWORD_RANKING) as HOTWORD_RANKING,h.APPID as APPID ,max(h.GENRENAMES) as GENRENAMES,max(h.APPNAME) as APPNAME ,max(h.APPIMG) as  APPIMG ,max(h.COMPANYNAME) as COMPANYNAME ,max(h.UPDATETIME)  as UPDATETIME ," +
      " if(cast (collect_list(concat_ws('#$',i.listcategory,i.appcategory,i.ranking)) as string) =='[]',null,collect_list(concat_ws('#$',i.listcategory,i.appcategory,i.ranking)))  as APPRANKINGCATEGORY from  " +
      "(select c.HOTWORD as HOTWORD,a.RANKING AS APP_RANKING,c.COUNTRY as COUNTRY ,c.DEVICE as DEVICE,c.SEARCHINDEX as SEARCHINDEX,c.HOTWORD_RESULTCOUNT as HOTWORD_RESULTCOUNT," +
      "c.RELATEDWORDS  as RELATEDWORDS , c.HOTWORD_RANKING as HOTWORD_RANKING,a.APPID as APPID ,a.GENRENAMES as GENRENAMES, a.APPNAME as APPNAME ,a.APPIMG as  APPIMG ,a.COMPANYNAME as COMPANYNAME , c.UPDATETIME as UPDATETIME" +
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
      " inner join APPSTORE_BASE_HOTSEARCH f on  e.HOTWORD=f.HOTWORD and e.COUNTRY=f.COUNTRY and e.DEVICE=f.DEVICE" +
      " ) as b " +
      "    group by b.HOTWORD,b.COUNTRY,b.DEVICE" +
      " ) as  c " +
      "on  a.HOTWORD=c.HOTWORD and a.COUNTRY=c.COUNTRY and a.DEVICE=c.DEVICE" +
      ") as h " +
      "left join AppStore_App_Top100 as i on   h.appid=i.appid and h.country=i.country and h.device=i.device " +
      " group by  h.HOTWORD,h.APPID,h.COUNTRY,h.DEVICE")

    GetEsData.saveHBase(APPSTORE_CURRENT_SEARCHINDEX, "APPSTORE_CURRENT_SEARCHINDEX")
    val endTime7 = System.currentTimeMillis()
    val inteval7 = (endTime7 - startTime7) / 1000
    LOG.info(" The total time of dealing with APPSTORE_CURRENT_SEARCHINDEX is : " + inteval7 + "s")
  }
}
