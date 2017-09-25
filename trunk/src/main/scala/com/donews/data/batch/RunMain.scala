package com.donews.data.batch

import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue

import org.slf4j.LoggerFactory
import akka.actor.{Actor, ActorSystem, Props}


object RunMain {
  val LOG = LoggerFactory.getLogger(RunMain.getClass)
  var isLocal: Boolean = false

  def tryDo(body: => Unit): Unit = {
    var i = 0
    var running = true
    while (running) {
      try {
        i += 1
        body
        running = false
      } catch {
        case e: Throwable if i < 3 =>
          LOG.error(e.getMessage)
          Thread.sleep(60000)
      }
    }
  }

  case class TaskRunning(task: Processor, interval: String, executor: DAGExecutor[Processor])

  class TaskRunningActor extends Actor {
    def receive = {
      case TaskRunning(task, interval, executor) =>
        try {
          val startMills = System.currentTimeMillis()
          LOG.info("start task {}", task)

          task.run("aso", interval)

          val time = Duration.ofMillis(System.currentTimeMillis() - startMills)
          executor.success(task)
          LOG.info(s"end task $task time=$time")
        }
        catch {
          case e: Throwable =>
            LOG.error(e.getMessage, e)
            executor.fail(task, e.getMessage)
            LOG.info(s"fail task $task")
        }
    }
  }

  case class ParameterizedDAG(dag: DAG[Processor])

  val allQueue = new ArrayBlockingQueue[ParameterizedDAG](3)

  def main(args: Array[String]): Unit = {

    val CmdArg(interval, processor) = CmdArg.parse(args)
    this.isLocal = isLocal
    //CmdArg中tasksString的选择
    processor match {
      case "all" =>
        allQueue.put(ParameterizedDAG(DAG.allDAG))
      case _ =>
        val taskNames = processor.split(",").toSeq
        val unkownActionSet = taskNames.toSet -- TaskRegistry.taskNames()
        if (unkownActionSet.nonEmpty) {
          LOG.error("不被支持的命令: {}", unkownActionSet)
          return
        }
        LOG.info(s"""process ${taskNames.mkString(",")}""")
        val tasks = taskNames.map(TaskRegistry(_))
        allQueue.put(ParameterizedDAG(DAG.sequence(tasks: _*)))
    }
    val pdag = allQueue.take()
    val executor = DAGExecutor(pdag.dag)
    val system = ActorSystem("actor-demo-scala")
    val taskRunningActor = system.actorOf(Props[TaskRunningActor])
    executor.execute { tasks =>

      tasks.par.foreach { task =>
        tryDo {
          // taskRunningActor ! TaskRunning(task, interval, executor)
          try {
            val startMills = System.currentTimeMillis()
            LOG.info("start task {}", task)

            task.run("aso", interval)

            val time = Duration.ofMillis(System.currentTimeMillis() - startMills)
            executor.success(task)
            LOG.info(s"end task $task time=$time")
          }
          catch {
            case e: Throwable =>
              LOG.error(e.getMessage, e)
              executor.fail(task, e.getMessage)
              LOG.info(s"fail task $task")
          }
        }
      }
    }

  }
}
