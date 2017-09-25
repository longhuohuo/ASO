package com.donews.data.batch

import java.util
import java.util.regex.Pattern

import com.donews.data.util.AsoConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by lihui on 2016/8/9.
  */


abstract class Task {
  private val upperCharP = Pattern.compile("[A-Z]")
  //从类名转换为 action 名称,规则：去掉尾部的Task[$]?,Processor[$]?,并且将驼峰模式转换为下划线模式
  def name: String = {
    val simpleName = getClass.getSimpleName.replaceAll("(Task[$]?|Processor[$]?|[$])$", "")
    val sb = new StringBuffer()
    val m = upperCharP.matcher(simpleName)
    while (m.find()) {
      val lowcaseMatched = m.group().toLowerCase
      val replacement = if (sb.length() == 0) lowcaseMatched else "_" + lowcaseMatched
      m.appendReplacement(sb, replacement)
    }
    m.appendTail(sb)
    sb.toString
  }
  var filter: String = _
  var query: String = _

  protected val LOG = LoggerFactory.getLogger(this.getClass)

  def process(): Unit

  def run(appkey: String,interval :String): Unit = {
    process()
  }

  override def toString: String = name
}

trait SQLContextAware {
  implicit var ctx: SQLContext = _
}

abstract class Processor extends Task with SQLContextAware {
  override def run(appkey: String,interval :String): Unit = {
    val yarnPropsMap = AsoConfig.YARN_PROPS_MAP
    val arrayList = yarnPropsMap(appkey)(name).split(":|,")
    val list = new util.ArrayList[String]()
    list.add("spark-submit")
    for (x <- arrayList) {
      list.add(x)
    }
    list.add(AsoConfig.JAR_PATH)
    list.add(name)
    list.add("-i")
    list.add(s"$interval")
    println(list)

    val p = new ProcessBuilder(list).inheritIO().start()

    if (0 != p.waitFor()) {
      throw new RuntimeException(s"任务${name}失败")
    }

  }

}

object ProcessorDriver {
  private val LOG = LoggerFactory.getLogger(ProcessorDriver.getClass)

  def run(interval:String,processName: String): Unit = {
    //val processor = TaskRegistry(processName)
    val conf = new SparkConf
    val prefixName = conf.get("spark.app.name")
    conf.setAppName(s"$prefixName  - $processName")
    LOG.info("name {}", conf.get("spark.app.name"))
    val sc = new SparkContext(conf)
    val ctx =  new HiveContext(sc)
    val blockSize = 1024 * 1024 * 128 // 128MB
    sc.hadoopConfiguration.setInt("dfs.blocksize", blockSize)
    sc.hadoopConfiguration.setInt("parquet.block.size", blockSize)
    ctx.setConf("spark.sql.parquet.mergeSchema", "true")
   /* val InitArgs(filter,query)=GetEsData.init(interval.split("/"),ctx)
    processor.filter=filter
    processor.query=query
    processor.ctx = ctx
    processor.process()
    processor.ctx.sparkContext.stop()*/
  }
  def main(args: Array[String]) {
    val cmdArg = CmdArg.parse(args)
    run(cmdArg.interval, cmdArg.processor)
  }
}
