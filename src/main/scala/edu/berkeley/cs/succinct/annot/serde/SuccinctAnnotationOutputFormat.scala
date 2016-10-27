package edu.berkeley.cs.succinct.annot.serde

import java.io.File

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class SuccinctAnnotationOutputFormat
  extends FileOutputFormat[NullWritable, (Int, Iterator[(String, String, String)])] {
  override def getRecordWriter(job: TaskAttemptContext):
  RecordWriter[NullWritable, (Int, Iterator[(String, String, String)])] = {
    val conf = job.getConfiguration
    val ignoreParseErrors = conf.get("succinct.construct.ignoreParseErrors", "true").toBoolean
    val inMemory = conf.get("succinct.construct.inmemory", "true").toBoolean
    val dirs = conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")).split(",")
    val path = FileOutputFormat.getOutputPath(job)
    new SuccinctAnnotationRecordWriter(path, ignoreParseErrors, conf, (inMemory, new File(dirs(0))))
  }
}
