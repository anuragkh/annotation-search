package edu.berkeley.cs.succinct.annot

import java.io.{File, ObjectOutputStream}
import java.util.Properties

import edu.berkeley.cs.succinct.annot.impl.AnnotatedSuccinctRDDImpl
import edu.berkeley.cs.succinct.annot.serde.AnnotatedDocumentSerializer
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.succinct.annot.AnnotatedSuccinctPartition
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

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
    val inMemory = sc.getConf.get("succinct.construct.inmemory", "true").toBoolean
    val dirs = sc.getConf.get("spark.local.dir", System.getProperty("java.io.tmpdir")).split(",")
    println("Temp Dir: " + dirs(0))
    (inMemory, new File(dirs(0)))
  }

  /**
    * Creates an [[AnnotatedSuccinctRDD]] from an RDD of triplets (documentID, documentText, annotations).
    *
    * @param inputRDD          RDD of (documentID, documentText, annotations) triplets.
    * @param ignoreParseErrors Ignores errors in parsing annotations if set to true; throws an exception on error otherwise.
    * @return The [[AnnotatedSuccinctRDD]].
    */
  def apply(inputRDD: RDD[(String, String, String)], ignoreParseErrors: Boolean = true): AnnotatedSuccinctRDD = {
    val constructConf = getConstructionConf(inputRDD.sparkContext)
    val partitionsRDD = inputRDD.sortBy(_._1)
      .mapPartitionsWithIndex((idx, it) => {
        createAnnotatedSuccinctPartition(it, ignoreParseErrors, constructConf)
      }).cache()
    new AnnotatedSuccinctRDDImpl(partitionsRDD)
  }

  /**
    * Constructs and writes the [[AnnotatedSuccinctRDD]] to given output path.
    *
    * @param inputRDD          RDD of (documentID, documentText, annotations) triplets.
    * @param location          Output path for the data.
    * @param ignoreParseErrors Ignores errors in parsing annotations if set to true;
    *                          throws an exception on error otherwise.
    */
  def construct(inputRDD: RDD[(String, String, String)], location: String,
                conf: Configuration = new Configuration(), ignoreParseErrors: Boolean = true) {

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

    val constructConf = getConstructionConf(inputRDD.sparkContext)

    inputRDD.sortBy(_._1).mapPartitionsWithIndex((i, it) => Iterator((i, it))).foreach(part => {
      val i = part._1
      val it = part._2

      /* Serialize partition */
      val serStartTime = System.currentTimeMillis()
      val serializer = new AnnotatedDocumentSerializer(ignoreParseErrors, constructConf)
      serializer.serialize(it)
      val serEndTime = System.currentTimeMillis()
      println("Partition " + i + ": Serialization time: " + (serEndTime - serStartTime) + "ms")

      /* Obtain configuration parameters. */
      val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
      val localConf = new Configuration()
      val propIt = properties.entrySet().iterator()
      while (propIt.hasNext) {
        val entry = propIt.next()
        val propName = entry.getKey.asInstanceOf[String]
        val propValue = entry.getValue.asInstanceOf[String]
        localConf.set(propName, propValue)
      }
      val fsLocal = FileSystem.get(new Path(partitionLocation).toUri, localConf)

      /* Write docIds to persistent store */
      val per1StartTime = System.currentTimeMillis()
      val docIds = serializer.getDocIds
      val pathDocIds = new Path(partitionLocation + ".sdocids")
      fsLocal.delete(pathDocIds, true)
      val osDocIds = new ObjectOutputStream(fsLocal.create(pathDocIds))
      osDocIds.writeObject(docIds)
      osDocIds.close()
      val per1EndTime = System.currentTimeMillis()
      println("Partition " + i + ": Doc. ids persist time: " + (per1EndTime - per1StartTime) + "ms")

      /* Write Succinct docTextBuffer to persistent store */
      val per2StartTime = System.currentTimeMillis()
      val docTextBuffer = serializer.getTextBuffer
      val pathDoc = new Path(partitionLocation + ".sdocs")
      fsLocal.delete(pathDoc, true)
      val osDoc = fsLocal.create(pathDoc)
      val docBuf = new SuccinctIndexedFileBuffer(docTextBuffer._2, docTextBuffer._1)
      docBuf.writeToStream(osDoc)
      osDoc.close()
      val per2EndTime = System.currentTimeMillis()
      println("Partition " + i + ": Doc. txt (" + docTextBuffer._2.length / (1024 * 1024)
        + "MB) index time: " + (per2EndTime - per2StartTime) + "ms")

      /* Write Succinct annotationBuffers to persistent store */
      val per3StartTime = System.currentTimeMillis()
      val pathAnnotToc = new Path(partitionLocation + ".sannots.toc")
      fsLocal.delete(pathAnnotToc, true)
      val pathAnnot = new Path(partitionLocation + ".sannots")
      fsLocal.delete(pathAnnot, true)
      val osAnnotToc = fsLocal.create(pathAnnotToc)
      val osAnnot = fsLocal.create(pathAnnot)
      var totAnnotBytes = 0
      serializer.getAnnotationMap.foreach(kv => {
        val key = kv._1
        val startPos = osAnnot.getPos

        // Write Succinct annotationBuffer to persistent store.
        val buffers = kv._2.read
        val annotBuf = new SuccinctAnnotationBuffer("", "", buffers._1, buffers._2, buffers._3)
        annotBuf.writeToStream(osAnnot)
        totAnnotBytes += buffers._3.length
        val endPos = osAnnot.getPos
        val size = endPos - startPos

        // Add entry to TOC
        osAnnotToc.writeBytes(s"$key\t$startPos\t$size\n")
      })
      osAnnotToc.close()
      osAnnot.close()
      val per3EndTime = System.currentTimeMillis()
      println("Partition " + i + ": Annotation (" + totAnnotBytes / (1024 * 1024)
        + "MB) index time: " + (per3EndTime - per3StartTime) + "ms")
    })
  }

  /**
    * Reads a AnnotatedSuccinctRDD from disk.
    *
    * @param sc       The spark context
    * @param location The path to read the [[AnnotatedSuccinctRDD]] from.
    * @return The [[AnnotatedSuccinctRDD]].
    */
  def apply(sc: SparkContext, location: String): AnnotatedSuccinctRDD = {
    apply(sc, location, ".*", ".*")
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
  def apply(sc: SparkContext, location: String, annotClassFilter: String, annotTypeFilter: String): AnnotatedSuccinctRDD = {
    val locationPath = new Path(location)
    val fs = FileSystem.get(locationPath.toUri, sc.hadoopConfiguration)
    val status = fs.listStatus(locationPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-") && path.getName.endsWith(".sdocs")
      }
    })
    val numPartitions = status.length
    val partitionsRDD = sc.parallelize(0 until numPartitions, numPartitions)
      .mapPartitionsWithIndex[AnnotatedSuccinctPartition]((i, partition) => {
      val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
      Iterator(AnnotatedSuccinctPartition(partitionLocation, annotClassFilter, annotTypeFilter))
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
      (key, new SuccinctAnnotationBuffer(annotClass, annotType, buffers._1, buffers._2, buffers._3))
    })
    Iterator(new AnnotatedSuccinctPartition(docIds, succinctDocTextBuffer, succinctAnnotBufferMap))
  }


}
