package edu.berkeley.cs.succinct.annot.impl

import edu.berkeley.cs.succinct.annot.AnnotatedSuccinctRDD
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.succinct.annot.AnnotatedSuccinctPartition

class AnnotatedSuccinctRDDImpl private[succinct](val partitionsRDD: RDD[AnnotatedSuccinctPartition])
  extends AnnotatedSuccinctRDD(partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  val recordCount: Long = partitionsRDD.map(_.count).aggregate(0L)(_ + _, _ + _)

  /** Set the name for the RDD; By default set to "AnnotatedSuccinctRDD" */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("AnnotatedSuccinctRDD")

  /**
    * Cannot change persistence level.
    */
  override def persist(newLevel: StorageLevel): this.type = {
    this
  }

  /** Cannot un-persist. */
  override def unpersist(blocking: Boolean = true): this.type = {
    this
  }

  /** Unaffected since the RDD is always persisted by default. */
  override def cache(): this.type = {
    this
  }

  override def count(): Long = {
    recordCount
  }
}
