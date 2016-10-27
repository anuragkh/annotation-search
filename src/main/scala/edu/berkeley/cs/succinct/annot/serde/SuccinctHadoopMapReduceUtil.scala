package edu.berkeley.cs.succinct.annot.serde

import java.lang.{Boolean => JBoolean, Integer => JInteger}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID}

object SuccinctHadoopMapReduceUtil {

  private def firstAvailableClass(first: String, second: String): Class[_] = {
    try {
      Class.forName(first)
    } catch {
      case e: ClassNotFoundException =>
        Class.forName(second)
    }
  }

  def newTaskAttemptID(jtIdentifier: String, jobId: Int, isMap: Boolean, taskId: Int,
                       attemptId: Int) = {
    val klass = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptID")
    try {
      // First, attempt to use the old-style constructor that takes a boolean isMap
      // (not available in YARN)
      val ctor = klass.getDeclaredConstructor(classOf[String], classOf[Int], classOf[Boolean],
        classOf[Int], classOf[Int])
      ctor.newInstance(jtIdentifier, new JInteger(jobId), new JBoolean(isMap), new JInteger(taskId),
        new JInteger(attemptId)).asInstanceOf[TaskAttemptID]
    } catch {
      case exc: NoSuchMethodException =>
        // If that failed, look for the new constructor that takes a TaskType (not available in 1.x)
        val taskTypeClass = Class.forName("org.apache.hadoop.mapreduce.TaskType")
          .asInstanceOf[Class[Enum[_]]]
        val taskType = taskTypeClass.getMethod("valueOf", classOf[String]).invoke(
          taskTypeClass, if (isMap) "MAP" else "REDUCE")
        val ctor = klass.getDeclaredConstructor(classOf[String], classOf[Int], taskTypeClass,
          classOf[Int], classOf[Int])
        ctor.newInstance(jtIdentifier, new JInteger(jobId), taskType, new JInteger(taskId),
          new JInteger(attemptId)).asInstanceOf[TaskAttemptID]
    }
  }

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = {
    val klass = firstAvailableClass(
      "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl", // hadoop2, hadoop2-yarn
      "org.apache.hadoop.mapreduce.TaskAttemptContext") // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[TaskAttemptID])
    ctor.newInstance(conf, attemptId).asInstanceOf[TaskAttemptContext]
  }
}
