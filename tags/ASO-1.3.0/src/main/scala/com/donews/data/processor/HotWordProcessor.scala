package com.donews.data.processor

import com.donews.data.batch.Processor
import com.donews.data.util.{AsoConfig, GetEsData}
import org.slf4j.LoggerFactory

/**
  * Created by ChengXu&ChenLei on 2017/1/10.
  */
object HotWordProcessor extends Processor {
  override def process(): Unit = {
    val ctx = this.ctx
    val LOG = LoggerFactory.getLogger(HotWordProcessor.getClass)
    LOG.info("the filter condition is : " + filter)
    LOG.info("the query condition is : " + query)
    val startTime1 = System.currentTimeMillis()

    //APPSTORE_BASE_HOTSEARCH
    val options = Map("es.nodes" -> AsoConfig.ES_URL, "es.port" -> AsoConfig.ES_PORT, "es.query" -> query)
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
    GetEsData.updateHBase("delete from APPSTORE_CURRENT_TOP10_APP_RESULTS")
    ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/hotsearch_top100").registerTempTable("hotsearch_top100")
    val APPSTORE_CURRENT_TOP10_APP_RESULTS = ctx.sql("select hotword,cast(ranking as int ) as ranking,country,device,substr(cast(updatetime as string ),0,19) as  updatetime ,appid,appname,art_url as APPIMG ,copyright as COMPANYNAME,GENRENAMES  from hotsearch_top100  where  cast(ranking as int ) <=10  ")
    GetEsData.saveHBase(APPSTORE_CURRENT_TOP10_APP_RESULTS, "APPSTORE_CURRENT_TOP10_APP_RESULTS")
    val endTime4 = System.currentTimeMillis()
    val inteval4 = (endTime4 - startTime4) / 1000
    LOG.info(" The total time of dealing with APPSTORE_CURRENT_TOP10_APP_RESULTS is : " + inteval4 + "s")

    //APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH   es.hotsearch_related  &  AppStore_Base_HotSearch
    val startTime5 = System.currentTimeMillis()
    GetEsData.updateHBase("delete from APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")
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
    val endTime5 = System.currentTimeMillis()
    val inteval5 = (endTime5 - startTime5) / 1000
    LOG.info(" The total time of dealing with APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH is : " + inteval5 + "s")

  }

}
