package edu.berkeley.cs.succinct

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

package object annot {

  implicit class SuccinctContext(sc: SparkContext) {
    def annotatedSuccinctFile(filePath: String, numPartitions: Int = 0, annotClassFilter: String = ".*", annotTypeFilter: String = ".*"): AnnotatedSuccinctRDD = {
      AnnotatedSuccinctRDD(sc, filePath, numPartitions, annotClassFilter, annotTypeFilter)
    }
  }

  implicit class SuccinctSession(spark: SparkSession) {
    def annotatedSuccinctFile(filePath: String, numPartitions: Int = 0, annotClassFilter: String = ".*", annotTypeFilter: String = ".*"): AnnotatedSuccinctRDD = {
      AnnotatedSuccinctRDD(spark.sparkContext, filePath, numPartitions, annotClassFilter, annotTypeFilter)
    }
  }

  implicit class AnnotSuccinctRDD(rdd: RDD[(String, String, String)]) {
    def succinctAnnotated: AnnotatedSuccinctRDD = {
      AnnotatedSuccinctRDD(rdd)
    }

    def saveAsAnnotatedSuccinctFile(path: String, numPartitions: Int, conf: Configuration = new Configuration()): Unit = {
      AnnotatedSuccinctRDD.construct(rdd, path, numPartitions, conf)
    }
  }

}
