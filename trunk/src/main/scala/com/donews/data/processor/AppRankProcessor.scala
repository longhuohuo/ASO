package com.donews.data.processor

import com.donews.data.util.GetEsData
import org.apache.commons.cli.{GnuParser, Options}
import org.slf4j.LoggerFactory

/**
  * Created by ChengXu&ChenLei on 2017/1/10.
  * 每30分钟跑的，重试次数为0
  * 这个暂时不用跑,直接由前端进行join即可,前端跑不动再由这边处理！
  */
object AppRankProcessor {
  val LOG = LoggerFactory.getLogger(AppRankProcessor.getClass)

  def main(args: Array[String]): Unit = {
    val optionsArgs = new Options()
      .addOption("i", "interval", true, s"时间区间 ")
    val parser = new GnuParser()
    val cmdLine = parser.parse(optionsArgs, args)
    val interval = cmdLine.getOptionValue("i").split("/")
    val ctx = GetEsData.getHiveContext("AppRankProcessor--" + interval)
    GetEsData.tryDo(2) {
      try {
        val (day, filter) = GetEsData.init(interval, ctx)
        val startTime = System.currentTimeMillis()
        GetEsData.loadHBase("APPSTORE.RANK_TOP100")(ctx).filter(s"day = '$day' and " + filter).registerTempTable("rank_top100")
        GetEsData.loadHBase("APPSTORE.APP_DETAIL")(ctx).filter("COUNTRY='cn' or COUNTRY='tw' or COUNTRY='hk'").registerTempTable("app_detail")
        // 实现页面：APP排行，每30分钟跑整体刷新一次，按天分
        val APP_TOP100 = ctx.sql(
          """
            |select rank_top100.DAY as DAY, rank_top100.COUNTRY as COUNTRY, rank_top100.DEVICE as DEVICE, rank_top100.LISTCATEGORY as LISTCATEGORY, rank_top100.APPCATEGORY as APPCATEGORY,
            |rank_top100.RANKING as RANKING, rank_top100.APPID as APPID, app_detail.APPNAME as APPNAME, app_detail.APPIMG as APPIMG, app_detail.COMPANYNAME as COMPANYNAME,
            |rank_top100.UPDATETIME as UPDATETIME from rank_top100 inner join app_detail on
            |rank_top100.COUNTRY=app_detail.COUNTRY and rank_top100.APPID=app_detail.APPID
          """.stripMargin)

        GetEsData.saveHBase(APP_TOP100, "APPSTORE.APP_TOP100")
        val endTime = System.currentTimeMillis()
        val inteval = (endTime - startTime) / 1000
        LOG.info(" The total time of dealing with APPSTORE_APP_TOP100   is : " + inteval + "s")
      }
      catch {
        case e: Exception =>
          throw new Exception(e)
      }

    }
  }
}
