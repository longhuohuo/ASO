import com.donews.data.util.{GetEsData, InitArgs}
import org.apache.commons.cli.{GnuParser, Options}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2017/2/22.
  */
object Test {
  val LOG = LoggerFactory.getLogger(Test.getClass)

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[2]").setAppName("ts")
    val sc = new SparkContext(conf)
    val ctx=new HiveContext(sc)
    /*  val yesterday=interval(0)
      val day_filter=s"day='$yesterday'"
      val InitArgs(filter, duration_query) = GetEsData.init(interval, ctx)
      LOG.info("the filter condition is : " + filter)
      LOG.info("the query condition is : " + duration_query)*/
     // val dataFrameReader = GetEsData.getDataFrameReader(ctx, duration_query)

      GetEsData.loadHBase("APPSTORE_HOTSEARCH_AGRR")(ctx).filter("day >='2017-02-23' and day <='2017-02-24'").registerTempTable("APPSTORE_HOTSEARCH_AGRR")
      ctx.sql("select * from (select *,row_number() over (partition by COUNTRY,DEVICE order by updatetime desc ) rank from APPSTORE_HOTSEARCH_AGRR)  a where a.rank=1 ").show(100)


  }
}
