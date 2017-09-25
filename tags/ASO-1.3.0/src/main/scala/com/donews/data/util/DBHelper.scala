package com.donews.data.util

import java.sql.ResultSet
import java.util.{List => JList}

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper
import org.skife.jdbi.v2.sqlobject.{Bind, SqlQuery}
import org.skife.jdbi.v2.tweak.ResultSetMapper
import org.skife.jdbi.v2.{DBI, StatementContext}

import scala.collection.JavaConversions._



case class PublicTable(name: String, details: String)

class PublicTableMapper extends ResultSetMapper[PublicTable] {
  override def map(i: Int, resultSet: ResultSet, statementContext: StatementContext): PublicTable =
    PublicTable(
      name = resultSet.getString("name"),
      details = resultSet.getString("details")
    )
}

@RegisterMapper(Array(classOf[PublicTableMapper]))
trait PublicTableDao{
  @SqlQuery("select * from service_config_public_table where name=:name")
  def getAllByName(@Bind("name") name:String):JList[PublicTable]
  @SqlQuery("select * from service_config_public_table")
  def getAll:JList[PublicTable]
}

case class YarnPropertiesTable(businessType:String,appkey: String, processorName: String,props:String,ext:String)

class YarnPropertiesTableMapper extends ResultSetMapper[YarnPropertiesTable] {
  override def map(i: Int, resultSet: ResultSet, statementContext: StatementContext): YarnPropertiesTable =
    YarnPropertiesTable(
      resultSet.getString("business_type"),
      resultSet.getString("appkey"),
      resultSet.getString("processor_name"),
      resultSet.getString("props"),
      resultSet.getString("ext")
    )
}

@RegisterMapper(Array(classOf[YarnPropertiesTableMapper]))
trait YarnPropertiesTableDao{
  @SqlQuery("select * from service_config_yarn_properties_table where business_type=:businessType")
  def getAllByBusinessType(@Bind("businessType") businessType:String):JList[YarnPropertiesTable]
  @SqlQuery("select * from service_config_yarn_properties_table")
  def getAll:JList[YarnPropertiesTable]
}

case class DefaultTable(name: String, details: String)

class DefaultTableMapper extends ResultSetMapper[DefaultTable] {
  override def map(i: Int, resultSet: ResultSet, statementContext: StatementContext): DefaultTable =
    DefaultTable(
      name = resultSet.getString("name"),
      details = resultSet.getString("details")
    )
}
@RegisterMapper(Array(classOf[DefaultTableMapper]))
trait DefaultTableDao{
  @SqlQuery("select * from service_config_default_table where business_type=:businessType")
  def getAllByName(@Bind("businessType") businessType:String):JList[DefaultTable]
  @SqlQuery("select * from service_config_default_table")
  def getAll:JList[DefaultTable]
}


object DBHelper {

  val ds=new ComboPooledDataSource()
  ds.setProperties(AsoConfig.MYSQL_LOGIC_PROPERTIES)

  lazy val dbi: DBI = new DBI(ds)

  lazy val publicTableDao = dbi.onDemand(classOf[PublicTableDao])
  lazy val yarnPropertiesTableDao = dbi.onDemand(classOf[YarnPropertiesTableDao])
  lazy val defaultTableDao = dbi.onDemand(classOf[DefaultTableDao])

  def getAllPublicTable: List[PublicTable] = publicTableDao.getAll.toList
  def getAllYarnPropTable: List[YarnPropertiesTable] = yarnPropertiesTableDao.getAll.toList
  def getAllDefaultTable: List[DefaultTable] = defaultTableDao.getAll.toList

  def getAllPublicTableByName(name:String): List[PublicTable] = publicTableDao.getAllByName(name).toList
  def getAllYarnPropTableByName(name:String): List[YarnPropertiesTable] = yarnPropertiesTableDao.getAllByBusinessType(name).toList
  def getAllDefaultTableByName(name:String): List[DefaultTable] = defaultTableDao.getAllByName(name).toList

  def bench(func: () => Any): Unit = {
    val start = System.currentTimeMillis()
    var n = 1
    var r = func()
    while (System.currentTimeMillis() - start < 10 * 1000L) {
      r = func()
      n += 1
    }
    val time = System.currentTimeMillis() - start
    println(s"$r \nops=${n * 1000.0 / time}")
  }

  def main(args: Array[String]) {

    getAllDefaultTableByName("appstatistic").foreach(println(_))
  }
}
