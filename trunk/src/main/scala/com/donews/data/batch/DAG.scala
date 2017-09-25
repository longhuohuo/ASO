package com.donews.data.batch

import java.time.Duration
import com.donews.data.processor.{AppRankProcessor, HotWordAndSearchIndexMProcessor}
import org.slf4j.LoggerFactory
import scala.collection.immutable.{ListMap, ListSet}


case class Node[T](task: T, parent: T*) {
  override def toString: String = {
    s"$task(${parent.mkString(",")})"
  }
}

case class DAG[T](nodes: Node[T]*)

case class DAGExecutor[T](dag: DAG[T]) {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  private val _nodes: Map[T, Seq[T]] = dag.nodes.map(node => (node.task, node.parent.filter(_ != null))).toMap

  private var _pending: Set[T] = ListSet()
  private var _fails = ListMap[T, String]()
  private var _success = Seq[T]()


  def getPending: Set[T] = {
    _pending.filter { name =>
      val parents = _nodes(name)
      !parents.exists(name => {
        val a = _success
        val b = !a.contains(name)
        b
      })
    }
    /*   _pending.find { name =>
         val parents = _nodes(name)
         !parents.exists(name => {
           val a = _success
           val b = !a.contains(name)
           b
         })
       }*/

  }

  def fail(name: T, message: String): Unit = {
    _pending -= name
    _fails += name -> message
    for (child <- _pending.filter(child => _nodes(child).contains(name))) {
      fail(child, s"依赖的任务无法执行: $name")
    }
  }

  def success(name: T): Unit = {
    _pending -= name
    _success = _success :+ name
  }

  def execute(func: Set[T] => Unit): Unit = {
    _pending = _nodes.keySet
    _fails = ListMap()
    _success = Seq()
    var running = true
    while (running) {
      val taskOpt = getPending
      if (taskOpt.nonEmpty) {
        /*    val task=taskOpt.get
            val startMills = System.currentTimeMillis()
            LOG.info("start task {}", task)
            try {
              func(task)
              val time = Duration.ofMillis(System.currentTimeMillis() - startMills)
              LOG.info(s"end task $task time=$time")
              success(task)
            } catch {
              case e: Throwable => fail(task, e.getMessage)
                LOG.error(e.getMessage, e)
                LOG.info(s"fail task $task")
            }*/
        func(taskOpt)
      }

      else {
        running = false
      }
    }

    for (name <- _success) {
      LOG.info(s"success task: $name")
    }
    for (name <- _fails) {
      LOG.info(s"fail task: ${name._1} - ${name._2}")
    }
  }
}

object DAG {

  val allDAG = new DAG[Processor](
  /*  Node(HotWordAndSearchIndexMProcessor),
    //Node(AppSearchIndexProcessor, HotWordAndSearchIndexMProcessor, AppRankProcessor),
    Node(AppRankProcessor)*/
  )

  def sequence(tasks: Processor*): DAG[Processor] = {
    new DAG(tasks.zip(null +: tasks)
      .map { case (task, parent) => Node(task, parent) }: _*)
  }

  def parallels(tasks: Processor*): DAG[Processor] = {
    new DAG(tasks.map(Node(_)): _*)
  }

  def main(args: Array[String]): Unit = {
    DAGExecutor(allDAG).execute { task => }
  }
}





