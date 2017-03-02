package edu.berkeley.cs.succinct.annot

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import edu.berkeley.cs.succinct.annot.impl.AnnotatedSuccinctRDDImpl
import edu.berkeley.cs.succinct.annot.serde.{AnnotatedDocumentSerializer, SuccinctAnnotationOutputFormat, SuccinctHadoopMapReduceUtil}
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.succinct.annot.AnnotatedSuccinctPartition

abstract class AnnotatedSuccinctRDD(@transient private val sc: SparkContext,
                                    @transient private val deps: Seq[Dependency[_]])
  extends RDD[(String, String)](sc, deps) {

  /**
    * Returns the RDD of partitions.
    *
    * @return The RDD of partitions.
    */
  private[succinct] def partitionsRDD: RDD[AnnotatedSuccinctPartition]

  /**
    * Returns first parent of the RDD.
    *
    * @return The first parent of the RDD.
    */
  protected[succinct] def getFirstParent: RDD[AnnotatedSuccinctPartition] = {
    firstParent[AnnotatedSuccinctPartition]
  }

  /**
    * Returns the array of partitions.
    *
    * @return The array of partitions.
    */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /** Overrides the compute function to return iterator over Succinct records. */
  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val succinctIterator = firstParent[AnnotatedSuccinctPartition].iterator(split, context)
    if (succinctIterator.hasNext) {
      succinctIterator.next().iterator
    } else {
      Iterator[(String, String)]()
    }
  }

  /**
    * Get document text given the documentID.
    *
    * @param documentId The documentID for the document.
    * @return The document text.
    */
  def getDocument(documentId: String): String = {
    val values = partitionsRDD.map(buf => buf.getDocument(documentId)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"DocumentId ${documentId.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  /**
    * Get document text at a specified offset given the documentID.
    *
    * @param documentId The documentID for the document.
    * @param offset     The offset into the document text.
    * @param length     The number of characters to fetch.
    * @return The document text.
    */
  def extractDocument(documentId: String, offset: Int, length: Int): String = {
    val values = partitionsRDD.map(buf => buf.extractDocument(documentId, offset, length)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"DocumentId ${documentId.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  /**
    * Find all (documentID, startOffset, endOffset) triplets corresponding to an arbitrary query
    * composed of Contains, ContainedIn, Before, After, FilterAnnotations, Search and RegexSearch
    * queries.
    *
    * @param operator An arbitrary expression tree composed of Contains, ContainedIn, Before, After,
    *                 FilterAnnotations, Search and RegexSearch.
    * @return An [[Iterator]] over matching (documentID, startOffset, endOffset) triplets
    *         encapsulated as Result objects.
    */
  def query(operator: Operator): RDD[Result] = {
    partitionsRDD.flatMap(_.query(operator))
  }

  def count(operator: Operator): Long = {
    partitionsRDD.map(_.count(operator)).collect().sum
  }

  /**
    * Saves the [[AnnotatedSuccinctRDD]] at the specified path.
    *
    * @param location The path where the [[AnnotatedSuccinctRDD]] should be stored.
    */
  def save(location: String, conf: Configuration = new Configuration()): Unit = {
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, conf)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }

    val it = conf.iterator()
    val properties = new Properties()
    while (it.hasNext) {
      val entry = it.next()
      properties.setProperty(entry.getKey, entry.getValue)
    }

    partitionsRDD.zipWithIndex().foreach(entry => {
      val i = entry._2
      val partition = entry._1
      val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)

      val localConf = new Configuration()
      val propIt = properties.entrySet().iterator()
      while (propIt.hasNext) {
        val entry = propIt.next()
        val propName = entry.getKey.asInstanceOf[String]
        val propValue = entry.getValue.asInstanceOf[String]
        localConf.set(propName, propValue)
      }
      partition.save(partitionLocation, localConf)
    })

    val successPath = new Path(location.stripSuffix("/") + "/_SUCCESS")
    fs.create(successPath).close()
  }

  // DEBUG: Storage
  def printStorageStats(): Unit = {
    val stats = partitionsRDD.zipWithIndex()
      .map(entry => (entry._2, entry._1.storageBreakdown())).collect()

    stats.foreach(println)
  }

}

object AnnotatedSuccinctRDD {

  private def getConstructionConf(sc: SparkContext): (Boolean, File) = {
    val serializeInMemory = sc.getConf.get("succinct.annotations.serializeInMemory", "true").toBoolean
    val dirs = sc.getConf.get("spark.local.dir", System.getProperty("java.io.tmpdir")).split(",")
    println("serializeInMemory = " + serializeInMemory + "Spark local dir = " + dirs(0)
      + " persistInMemory = false")
    (serializeInMemory, new File(dirs(0)))
  }

  /**
    * Creates an [[AnnotatedSuccinctRDD]] from an RDD of triplets (documentID, documentText, annotations).
    *
    * @param inputRDD RDD of (documentID, documentText, annotations) triplets.
    * @return The [[AnnotatedSuccinctRDD]].
    */
  def apply(inputRDD: RDD[(String, String, String)], numPartitions: Int = 0): AnnotatedSuccinctRDD = {
    val sc = inputRDD.sparkContext
    val constructConf = getConstructionConf(sc)
    val ignoreParseErrors = sc.getConf.get("succinct.annotations.ignoreParseErrors", "true").toBoolean
    if (numPartitions == 0) {
      val partitionsRDD = inputRDD.sortBy(_._1)
        .mapPartitionsWithIndex((idx, it) => {
          createAnnotatedSuccinctPartition(it, ignoreParseErrors, constructConf)
        }).cache()
      new AnnotatedSuccinctRDDImpl(partitionsRDD)
    } else {
      val partitionsRDD = inputRDD.sortBy(_._1).repartition(numPartitions)
        .mapPartitionsWithIndex((idx, it) => {
          createAnnotatedSuccinctPartition(it, ignoreParseErrors, constructConf)
        }).cache()
      new AnnotatedSuccinctRDDImpl(partitionsRDD)
    }
  }

  /**
    * Constructs and writes the [[AnnotatedSuccinctRDD]] to given output path.
    *
    * @param inputRDD RDD of (documentID, documentText, annotations) triplets.
    * @param location Output path for the data.
    */
  def construct(inputRDD: RDD[(String, String, String)], location: String,
                numPartitions: Int = 0, conf: Configuration = new Configuration()) {

    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, conf)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }

    val serializableConf = new SerializableWritable(conf)
    val now = new Date()

    if (numPartitions == 0) {
      inputRDD.sortBy(_._1).mapPartitionsWithIndex((i, it) => Iterator((i, it))).foreach(part => {
        val i = part._1
        val conf = serializableConf.value
        val job = Job.getInstance(conf)
        job.setOutputKeyClass(classOf[NullWritable])
        job.setOutputFormatClass(classOf[SuccinctAnnotationOutputFormat])
        FileOutputFormat.setOutputPath(job, new Path(location))

        val formatter = new SimpleDateFormat("yyyyMMddHHmmss")
        val jobtrackerID = formatter.format(now)
        val attemptNumber = 1

        val attemptID = SuccinctHadoopMapReduceUtil.newTaskAttemptID(jobtrackerID, i, isMap = false, i, attemptNumber)
        val hadoopContext = SuccinctHadoopMapReduceUtil.newTaskAttemptContext(job.getConfiguration, attemptID)

        val format = new SuccinctAnnotationOutputFormat
        val commiter = format.getOutputCommitter(hadoopContext)
        commiter.setupTask(hadoopContext)
        val writer = format.getRecordWriter(hadoopContext)
        writer.write(NullWritable.get(), part)
        writer.close(hadoopContext)
        commiter.commitTask(hadoopContext)
      })
    } else {
      inputRDD.sortBy(_._1).repartition(numPartitions)
        .mapPartitionsWithIndex((i, it) => Iterator((i, it))).foreach(part => {
        val i = part._1
        val conf = serializableConf.value
        val job = Job.getInstance(conf)
        job.setOutputKeyClass(classOf[NullWritable])
        job.setOutputFormatClass(classOf[SuccinctAnnotationOutputFormat])
        FileOutputFormat.setOutputPath(job, new Path(location))

        val formatter = new SimpleDateFormat("yyyyMMddHHmmss")
        val jobtrackerID = formatter.format(now)
        val attemptNumber = 1

        val attemptID = SuccinctHadoopMapReduceUtil.newTaskAttemptID(jobtrackerID, i, isMap = false, i, attemptNumber)
        val hadoopContext = SuccinctHadoopMapReduceUtil.newTaskAttemptContext(job.getConfiguration, attemptID)

        val format = new SuccinctAnnotationOutputFormat
        val commiter = format.getOutputCommitter(hadoopContext)
        commiter.setupTask(hadoopContext)
        val writer = format.getRecordWriter(hadoopContext)
        writer.write(NullWritable.get(), part)
        writer.close(hadoopContext)
        commiter.commitTask(hadoopContext)
      })
    }

    val successPath = new Path(location.stripSuffix("/") + "/_SUCCESS")
    fs.create(successPath).close()
  }

  /**
    * Reads a AnnotatedSuccinctRDD from disk.
    *
    * @param sc       The spark context
    * @param location The path to read the [[AnnotatedSuccinctRDD]] from.
    * @return The [[AnnotatedSuccinctRDD]].
    */
  def apply(sc: SparkContext, location: String, numPartitions: Int = 0): AnnotatedSuccinctRDD = {
    apply(sc, location, numPartitions, ".*", ".*")
  }

  /**
    * Reads a [[AnnotatedSuccinctRDD]] from disk, based on filters on Annotation Class and Type.
    *
    * @param sc               The spark context
    * @param location         The path to read the SuccinctKVRDD from.
    * @param annotClassFilter Regex metadataFilter specifying which annotation classes to load.
    * @param annotTypeFilter  Regex metadataFilter specifying which annotation types to load.
    * @return The [[AnnotatedSuccinctRDD]].
    */
  def apply(sc: SparkContext, location: String, numPartitions: Int = 0, annotClassFilter: String, annotTypeFilter: String): AnnotatedSuccinctRDD = {
    val locationPath = new Path(location)
    val fs = FileSystem.get(locationPath.toUri, sc.hadoopConfiguration)
    val status = fs.listStatus(locationPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-") && path.getName.endsWith(".sdocs")
      }
    })
    val n = if (numPartitions == 0) status.length else Math.min(numPartitions, status.length)
    val dirs = sc.getConf.get("spark.local.dir", System.getProperty("java.io.tmpdir")).split(",")
    val partitionsRDD = sc.parallelize(0 until n, n)
      .mapPartitionsWithIndex[AnnotatedSuccinctPartition]((i, _) => {
      val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
      Iterator(AnnotatedSuccinctPartition(partitionLocation, annotClassFilter, annotTypeFilter, dirs(0)))
    }).cache()
    new AnnotatedSuccinctRDDImpl(partitionsRDD)
  }

  /**
    * Creates an [[AnnotatedSuccinctPartition]] from a collection of (documentID, documentText,
    * annotations) triplets.
    *
    * @param dataIter          An iterator over (documentID, documentText, annotations) triplets.
    * @param ignoreParseErrors Ignores errors in parsing annotations if set to true; throws an
    *                          exception on error otherwise.
    * @param constructConf     Temporary directory used for writing files during index construction.
    * @return An iterator over [[AnnotatedSuccinctPartition]]
    */
  def createAnnotatedSuccinctPartition(dataIter: Iterator[(String, String, String)],
                                       ignoreParseErrors: Boolean, constructConf: (Boolean, File)):
  Iterator[AnnotatedSuccinctPartition] = {
    val serializer = new AnnotatedDocumentSerializer(ignoreParseErrors, constructConf)
    serializer.serialize(dataIter)

    val docIds = serializer.getDocIds
    val docTextBuffer = serializer.getTextBuffer
    val succinctDocTextBuffer = new SuccinctIndexedFileBuffer(docTextBuffer._2, docTextBuffer._1)
    val succinctAnnotBufferMap = serializer.getAnnotationMap.map(kv => {
      val key = kv._1
      val annotClass = key.split('^')(1)
      val annotType = key.split('^')(2)
      val buffers = kv._2.read
      val numAnnots = kv._2.getNumAnnotations()
      (key, new SuccinctAnnotationBuffer(annotClass, annotType, buffers._1, buffers._2, buffers._3, numAnnots))
    })
    Iterator(new AnnotatedSuccinctPartition(docIds, succinctDocTextBuffer, succinctAnnotBufferMap))
  }


}
