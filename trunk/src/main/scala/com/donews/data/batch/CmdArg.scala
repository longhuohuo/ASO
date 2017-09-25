package com.donews.data.batch

import org.apache.commons.cli.{GnuParser, Options}


case class CmdArg(interval: String, processor: String)

object CmdArg {
  def parse(args: Array[String]): CmdArg = {
    //Option[T]是容器
    val options = new Options()
      .addOption("i", "interval", true, s"时间区间 ")
    val parser = new GnuParser()
    val cmdLine = parser.parse(options, args)
    val interval = cmdLine.getOptionValue("i", null)
    val processor = cmdLine.getArgs.headOption.get

    CmdArg(interval, processor)
  }

  def main(args: Array[String]): Unit = {
    parse(args)
  }
}
