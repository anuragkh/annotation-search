package edu.berkeley.cs.succinct.annot.serde

import java.io.{File, ObjectOutputStream}

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot.SuccinctAnnotationBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class SuccinctAnnotationRecordWriter(path: Path, ignoreParseErrors: Boolean, conf: Configuration,
                                     constructConf: (Boolean, File))
  extends RecordWriter[NullWritable, (Int, Iterator[(String, String, String)])] {

  override def write(key: NullWritable, part: (Int, Iterator[(String, String, String)])): Unit = {
    val i = part._1
    val it = part._2

    /* Serialize partition */
    val serStartTime = System.currentTimeMillis()
    val serializer = new AnnotatedDocumentSerializer(ignoreParseErrors, constructConf)
    serializer.serialize(it)
    val serEndTime = System.currentTimeMillis()
    println("Partition " + i + ": Serialization time: " + (serEndTime - serStartTime) + "ms")

    /* Obtain configuration parameters. */
    val partitionLocation = path.toString.stripSuffix("/") + "/part-" + "%05d".format(i)

    val fsLocal = FileSystem.get(new Path(partitionLocation).toUri, conf)

    /* Write docIds to persistent store */
    val per1StartTime = System.currentTimeMillis()
    val docIds = serializer.getDocIds
    val pathDocIds = new Path(partitionLocation + ".sdocids")
    val osDocIds = new ObjectOutputStream(fsLocal.create(pathDocIds))
    osDocIds.writeObject(docIds)
    osDocIds.close()
    val per1EndTime = System.currentTimeMillis()
    println("Partition " + i + ": Doc. ids persist time: " + (per1EndTime - per1StartTime) + "ms")

    /* Write Succinct docTextBuffer to persistent store */
    val per2StartTime = System.currentTimeMillis()
    val docTextBuffer = serializer.getTextBuffer
    val pathDoc = new Path(partitionLocation + ".sdocs")
    val osDoc = fsLocal.create(pathDoc)
    SuccinctIndexedFileBuffer.construct(docTextBuffer._2, docTextBuffer._1, osDoc)
    osDoc.close()
    val per2EndTime = System.currentTimeMillis()
    println("Partition " + i + ": Doc. txt (" + docTextBuffer._2.length / (1024 * 1024)
      + "MB) index time: " + (per2EndTime - per2StartTime) + "ms")

    /* Write Succinct annotationBuffers to persistent store */
    val per3StartTime = System.currentTimeMillis()
    val pathAnnotToc = new Path(partitionLocation + ".sannots.toc")
    val pathAnnot = new Path(partitionLocation + ".sannots")
    val osAnnotToc = fsLocal.create(pathAnnotToc)
    val osAnnot = fsLocal.create(pathAnnot)
    var totAnnotBytes = 0
    serializer.getAnnotationMap.foreach(kv => {
      val key = kv._1
      val startPos = osAnnot.getPos

      // Write Succinct annotationBuffer to persistent store.
      val buffers = kv._2.read
      SuccinctAnnotationBuffer.construct(buffers._3, buffers._2, buffers._1, osAnnot)
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
  }

  override def close(context: TaskAttemptContext): Unit = {

  }
}
