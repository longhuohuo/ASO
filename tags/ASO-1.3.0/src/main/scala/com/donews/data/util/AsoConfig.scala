package com.donews.data.util

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable

object AsoConfig {

  private val conf: Config = ConfigFactory.load()

  final val BUSINESS_TYPE = "aso"
  val MYSQL_LOGIC_URL = conf.getString("mysql.logic.url")
  val MYSQL_LOGIC_PROPERTIES = propsFromConfig(conf.getConfig("mysql.logic.properties"))
  var MYSQL_BI_URL = ""
  var MYSQL_BI_PROPERTIES = new Properties()


  val publicTableList = DBHelper.getAllPublicTable

  var HBASE_URL = ""
  var ES_URL = ""
  var ES_PORT = ""
  if (publicTableList.nonEmpty) {
    for (publicTable <- publicTableList) {
      publicTable.name match {
        case "hbase_url" => HBASE_URL = publicTable.details;
        case "es_url" => ES_URL = publicTable.details;
        case "es_port" => ES_PORT = publicTable.details;
        case "mysql_bi_url" => MYSQL_BI_URL = publicTable.details;
        case "mysql_bi_properties" => MYSQL_BI_PROPERTIES = propsFromString(publicTable.details);
        case _ => null
      }
    }
  }

  val defaultTableList = DBHelper.getAllDefaultTableByName(BUSINESS_TYPE)
  var JAR_PATH = ""
  if (defaultTableList.nonEmpty) {
    for (defaultTable <- defaultTableList) {
      defaultTable.name match {
        case "jar_path" => JAR_PATH = defaultTable.details;
        case _ => null
      }
    }
  }

  val DEFAULT_TABLE_MAP = new mutable.HashMap[String, String]()
  for (x <- defaultTableList) {
    DEFAULT_TABLE_MAP.put(x.name, x.details)
  }

  var YARN = DBHelper.getAllYarnPropTableByName(BUSINESS_TYPE)
  val YARN_PROPS_MAP = new mutable.HashMap[String, mutable.HashMap[String, String]]()
  for (x <- YARN) {
    val map = new mutable.HashMap[String, String]()
    map.put(x.processorName, x.props)
    if (YARN_PROPS_MAP.contains(x.appkey)) {
      YARN_PROPS_MAP(x.appkey).put(x.processorName, x.props)
    } else
      YARN_PROPS_MAP.put(x.appkey, map)
  }

  val YARN_EXT_MAP = new mutable.HashMap[String, mutable.HashMap[String, String]]()
  for (x <- YARN) {
    val map = new mutable.HashMap[String, String]()
    map.put(x.processorName, x.ext)
    if (YARN_EXT_MAP.contains(x.appkey)) {
      YARN_EXT_MAP(x.appkey).put(x.processorName, x.ext)
    } else
      YARN_EXT_MAP.put(x.appkey, map)
  }

  private def propsFromConfig(config: Config): Properties = {
    import scala.collection.JavaConversions._

    val props = new Properties()

    val map: Map[String, Object] = config.entrySet().map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)

    props.putAll(map)
    props
  }

  private def propsFromString(str: String): Properties = {

    import scala.collection.JavaConversions._
    val jsonObject = JSON.parseObject(str)
    val props = new Properties()
    val map: Map[String, Object] = jsonObject.entrySet().map(entry => entry.getKey -> entry.getValue)(collection.breakOut)
    props.putAll(map)
    props
  }

  def main(args: Array[String]) {


  }
}
