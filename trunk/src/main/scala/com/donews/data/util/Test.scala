package com.donews.data.util

/**
  * Created by Administrator on 2017/1/20.
  */

import java.time.LocalDate

import scala.annotation.tailrec
import scala.util.matching.Regex

object Test {
  def sumD(n: BigInt): BigInt = {
    @tailrec def innerSum(sum: BigInt, n: BigInt): BigInt = {
      if (n <= 1) sum
      else innerSum(sum + 4 * n.pow(2) - 6 * n + 6, n - 2)
    }
    innerSum(1, n)
  }

  def main(args: Array[String]): Unit = {
  val day="dfds"
    val start=day+"T23"
    val end=day+"T23:59"
    val duration_query_new =
      s"""
        {
         |  "sort": [
         |    {
         |      "updatetime": {
         |        "order": "asc"
         |      }
         |    }
         |  ],
         |
         |        "query": {
         |          "bool": {
         |            "must": [{
         |              "range":{
         |                "updatetime": {
         |                  "gte": "$start",
         |                  "lte": "$end"
         |                  }
         |                 }
         |              }]
         |            }
         |           }
         |
         |
         | }
          """.stripMargin
    println(duration_query_new)
  }

}
//BigInt(n-2).pow(2)