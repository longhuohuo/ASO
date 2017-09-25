package com.donews.data.processor

import com.donews.data.batch.Processor
import com.donews.data.util.GetEsData
import org.slf4j.LoggerFactory

/**
  * Created by ChengXu&ChenLei on 2017/1/10.
  */
object AppSearchIndexProcessor extends Processor {
  override def process(): Unit = {
    val ctx = this.ctx
    val LOG = LoggerFactory.getLogger(AppSearchIndexProcessor.getClass)
    LOG.info("the filter condition is : " + filter)
    val startTime = System.currentTimeMillis()

    //APPSTORE_CURRENT_SEARCHINDEX
    GetEsData.updateHBase("delete from APPSTORE_CURRENT_SEARCHINDEX")
    GetEsData.loadHBase("APPSTORE_BASE_HOTSEARCH")(ctx).filter(filter).registerTempTable("APPSTORE_BASE_HOTSEARCH")
    GetEsData.loadHBase("APPSTORE_CURRENT_TOP10_APP_RESULTS")(ctx).filter(filter).registerTempTable("APPSTORE_CURRENT_TOP10_APP_RESULTS")
    GetEsData.loadHBase("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")(ctx).filter(filter).registerTempTable("APPSTORE_CURRENT_TOP10_RELATED_HOTSEARCH")
    GetEsData.loadHBase("AppStore_App_Top100")(ctx).filter(filter).registerTempTable("AppStore_App_Top100")
    val APPSTORE_CURRENT_SEARCHINDEX = ctx.sql("select h.HOTWORD as HOTWORD,max(h.APP_RANKING) AS APP_RANKING,h.COUNTRY as COUNTRY ,h.DEVICE as DEVICE,max(h.SEARCHINDEX) as SEARCHINDEX,max(h.HOTWORD_RESULTCOUNT) as HOTWORD_RESULTCOUNT," +
      " max(h.RELATEDWORDS )  as RELATEDWORDS , max(h.HOTWORD_RANKING) as HOTWORD_RANKING,h.APPID as APPID ,max(h.GENRENAMES) as GENRENAMES,max(h.APPNAME) as APPNAME ,max(h.APPIMG) as  APPIMG ,max(h.COMPANYNAME) as COMPANYNAME ,max(h.UPDATETIME)  as UPDATETIME ," +
      " collect_list(concat_ws('#$',i.listcategory,i.appcategory,i.ranking)) as APPRANKINGCATEGORY from  " +
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
    val endTime = System.currentTimeMillis()
    val inteval = (endTime - startTime) / 1000
    LOG.info(" The total time of dealing with APPSTORE_CURRENT_SEARCHINDEX is : " + inteval + "s")
  }
}
