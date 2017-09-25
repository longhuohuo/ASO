package com.donews.data.processor

import java.time.LocalDate

import com.donews.data.batch.Processor
import com.donews.data.util.{AsoConfig, GetEsData}
import org.slf4j.LoggerFactory

/**
  * Created by ChengXu&ChenLei on 2017/1/10.
  */
object AppRankProcessor extends Processor {
  override def process(): Unit = {
    val ctx = this.ctx
    val LOG = LoggerFactory.getLogger(AppRankProcessor.getClass)
    LOG.info("the filter condition is : " + filter)
    LOG.info("the query condition is : " + query)
    LOG.info(filter)
    //APPSTORE_APP_TOP100
    val startTime = System.currentTimeMillis()
    GetEsData.updateHBase("delete from APPSTORE_APP_TOP100 where day='"+LocalDate.now()+"'")
    val options = Map("es.nodes" -> AsoConfig.ES_URL, "es.port" -> AsoConfig.ES_PORT, "es.query" -> query)
    ctx.read.format("org.elasticsearch.spark.sql").options(options).load("app_store/rank_top100").registerTempTable("rank_top100")
    val APPSTORE_APP_TOP100 = ctx.sql("select appid,appname,appimg, cast(ranking as int ) as ranking,country,device,listcategory,cast(appcategory as string) as appcategory,companyname,substr(cast(updatetime as string ),0,19) as  updatetime,substr(cast(updatetime as string ),0,10) as  day  from rank_top100")
    GetEsData.saveHBase(APPSTORE_APP_TOP100, "APPSTORE_APP_TOP100")
    val endTime = System.currentTimeMillis()
    val inteval = (endTime - startTime) / 1000
    LOG.info(" The total time of dealing with APPSTORE_APP_TOP100   is : " + inteval + "s")
    ctx.clearCache()
  }
}
