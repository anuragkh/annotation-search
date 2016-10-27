package edu.berkeley.cs.succinct.annot.serde

import java.io.{DataOutputStream, FileOutputStream, File, ObjectOutputStream}

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot.SuccinctAnnotationBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import scala.collection.immutable.TreeMap

class SuccinctAnnotationRecordWriter(path: Path, ignoreParseErrors: Boolean, conf: Configuration,
                                     constructConf: (Boolean, File))
  extends RecordWriter[NullWritable, (Int, Iterator[(String, String, String)])] {


  val fs = FileSystem.get(path.toUri, conf)
  var localFiles = new TreeMap[Int, (File, File, File, File)]()

  override def write(key: NullWritable, part: (Int, Iterator[(String, String, String)])): Unit = {
    val i = part._1
    val it = part._2

    /* Serialize partition */
    val serStartTime = System.currentTimeMillis()
    val serializer = new AnnotatedDocumentSerializer(ignoreParseErrors, constructConf)
    serializer.serialize(it)
    val serEndTime = System.currentTimeMillis()
    println("Partition " + i + ": Serialization time: " + (serEndTime - serStartTime) + "ms")

    val tmpDir = constructConf._2

    /* Write docIds to persistent store */
    val per1StartTime = System.currentTimeMillis()
    val docIds = serializer.getDocIds
    val pathDocIds = File.createTempFile("part-" + "%05d".format(i) + ".sdocids", ".tmp", tmpDir)
    pathDocIds.deleteOnExit()
    val osDocIds = new ObjectOutputStream(new FileOutputStream(pathDocIds))
    osDocIds.writeObject(docIds)
    osDocIds.close()
    val per1EndTime = System.currentTimeMillis()
    println("Partition " + i + ": Doc. ids persist time: " + (per1EndTime - per1StartTime) + "ms")

    /* Write Succinct docTextBuffer to persistent store */
    val per2StartTime = System.currentTimeMillis()
    val docTextBuffer = serializer.getTextBuffer
    val pathDoc = File.createTempFile("part-" + "%05d".format(i) + ".sdocs", ".tmp", tmpDir)
    pathDoc.deleteOnExit()
    val osDoc = new DataOutputStream(new FileOutputStream(pathDoc))
    SuccinctIndexedFileBuffer.construct(docTextBuffer._2, docTextBuffer._1, osDoc)
    osDoc.close()
    val per2EndTime = System.currentTimeMillis()
    println("Partition " + i + ": Doc. txt (" + docTextBuffer._2.length / (1024 * 1024)
      + "MB) index time: " + (per2EndTime - per2StartTime) + "ms")

    /* Write Succinct annotationBuffers to persistent store */
    val per3StartTime = System.currentTimeMillis()
    val pathAnnotToc = File.createTempFile("part-" + "%05d".format(i) + ".sannots.toc", ".tmp", tmpDir)
    pathAnnotToc.deleteOnExit()
    val pathAnnot = File.createTempFile("part-" + "%05d".format(i) + ".sannots", ".tmp", tmpDir)
    pathAnnot.deleteOnExit()
    val osAnnotToc = new DataOutputStream(new FileOutputStream(pathAnnotToc))
    val osAnnot = new DataOutputStream(new FileOutputStream(pathAnnot))
    var totAnnotBytes = 0
    serializer.getAnnotationMap.foreach(kv => {
      val key = kv._1
      val startPos = osAnnot.size()

      // Write Succinct annotationBuffer to persistent store.
      val buffers = kv._2.read
      SuccinctAnnotationBuffer.construct(buffers._3, buffers._2, buffers._1, osAnnot)
      totAnnotBytes += buffers._3.length
      val endPos = osAnnot.size()
      val size = endPos - startPos

      // Add entry to TOC
      osAnnotToc.writeBytes(s"$key\t$startPos\t$size\n")
    })
    osAnnotToc.close()
    osAnnot.close()
    val per3EndTime = System.currentTimeMillis()
    println("Partition " + i + ": Annotation (" + totAnnotBytes / (1024 * 1024)
      + "MB) index time: " + (per3EndTime - per3StartTime) + "ms")

    localFiles += (i -> (pathDocIds, pathDoc, pathAnnotToc, pathAnnot))
  }

  override def close(context: TaskAttemptContext): Unit = {
    localFiles.foreach(entry => {
      val i = entry._1
      val files = entry._2
      val pathDocIds = new Path(path, "part-" + "%05d".format(i) + ".sdocids")
      val pathDoc = new Path(path, "part-" + "%05d".format(i) + ".sdocs")
      val pathAnnotToc = new Path(path, "part-" + "%05d".format(i) + ".sannots.toc")
      val pathAnnot = new Path(path, "part-" + "%05d".format(i) + ".sannots")

      if (fs.exists(pathDocIds)) {
        fs.delete(pathDocIds, true)
      }
      fs.copyFromLocalFile(true, new Path(files._1.getAbsolutePath), pathDocIds)

      if (fs.exists(pathDoc)) {
        fs.delete(pathDoc, true)
      }
      fs.copyFromLocalFile(true, new Path(files._2.getAbsolutePath), pathDoc)

      if (fs.exists(pathAnnotToc)) {
        fs.delete(pathAnnotToc, true)
      }
      fs.copyFromLocalFile(true, new Path(files._3.getAbsolutePath), pathAnnotToc)

      if (fs.exists(pathAnnot)) {
        fs.delete(pathAnnot, true)
      }
      fs.copyFromLocalFile(true, new Path(files._4.getAbsolutePath), pathAnnot)
    })
  }
}
