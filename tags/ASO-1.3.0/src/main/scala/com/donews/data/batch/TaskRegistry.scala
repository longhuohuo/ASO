package com.donews.data.batch

import com.donews.data.processor.{AppRankProcessor, AppSearchIndexProcessor, HotWordProcessor}

import scala.collection.immutable.ListMap

object TaskRegistry {


  private var _processors: Map[String, Processor] = ListMap()

  def taskNames(): Set[String] = _processors.keySet

  def register(processors: Processor*): Unit = {
    _processors ++= processors.map(processor => processor.name -> processor).toMap
  }

  def apply(name: String): Processor = {
    _processors(name)
  }

  TaskRegistry.register(
    AppRankProcessor,
    AppSearchIndexProcessor,
    HotWordProcessor
  )

  def main(args: Array[String]): Unit = {

  }
}
