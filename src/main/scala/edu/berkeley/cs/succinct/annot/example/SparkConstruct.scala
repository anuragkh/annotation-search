package edu.berkeley.cs.succinct.annot.example

import com.elsevier.cat.StringArrayWritable
import edu.berkeley.cs.succinct.annot.AnnotatedSuccinctRDD
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object SparkConstruct {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(0) + ".succinct"
    val sc = new SparkContext(new SparkConf().setAppName("SparkConstruct"))
    val rdd = sc.hadoopFile[Text, StringArrayWritable, SequenceFileInputFormat[Text, StringArrayWritable]](inputPath)
      .map(v => (v._1.toString, v._2.toStrings()(0), v._2.toStrings()(1))).persist(StorageLevel.DISK_ONLY)

    println("Number of documents: " + rdd.count())

    val succinctAnnot = AnnotatedSuccinctRDD(rdd)
    println("Constructed Succinct data structures, saving to disk...")

    succinctAnnot.save(outputPath)
    println("Save to disk complete. Storage statistics: ")
    succinctAnnot.printStorageStats()
  }
}
